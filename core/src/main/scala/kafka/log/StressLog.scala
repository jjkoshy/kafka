package kafka.log

import java.io.File
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import kafka.api.ApiVersion
import kafka.server.{BrokerState, BrokerTopicStats, KafkaConfig, KafkaServer, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, RecordVersion}
import org.apache.kafka.common.utils.{KafkaThread, Utils}

import scala.util.Random

object StressLog extends Logging {

  private object LogWriter {

    private def createRecords(maxSize: Int) = {
      val rand = Random.nextGaussian()
      val size = (rand * maxSize).abs.floor.intValue().min(maxSize / 2)
      val bytes = new Array[Byte](size)
      Random.nextBytes(bytes)
      val compressionType = CompressionType.forId(Random.nextInt(4))
      val magic = Random.nextInt(3).toByte
      (magic, RecordUtils.recordsWithValues(magic, compressionType, bytes))
    }

  }

  private class LogWriter(log: Log) extends Runnable with Logging {
    val bytesWritten = new AtomicLong(0)
    val totalBytesWritten = new AtomicLong(0)

    override def run(): Unit = {
      println("starting writer " + log.name)
      while (!StressLog.shuttingDown.get()) {
        val (magic, records) = LogWriter.createRecords(log.config.maxMessageSize)
        val result = log.appendAsLeader(
          records,
          leaderEpoch = 0,
          interBrokerProtocolVersion = ApiVersion.minSupportedFor(RecordVersion.lookup(magic))
        )
        totalBytesWritten.getAndAdd(result.validBytes)
        bytesWritten.getAndAdd(result.validBytes)
      }
      println("stopping writer " + log.name + " (wrote " + totalBytesWritten.get() + " bytes total)")
    }
  }

  private class LogReader(log: Log) extends Runnable with Logging {
    val bytesRead = new AtomicLong(0)

    override def run(): Unit = {
      println("starting reader " + log.name)
      while (!StressLog.shuttingDown.get()) {
        val offset = log.logEndOffset - 100
        if (offset >= 0) {
          val result = log.read(
            offset,
            maxLength = 1048576,
            maxOffset = None,
            minOneMessage = true,
            includeAbortedTxns = true
          )
          if (!result.firstEntryIncomplete) {
            val batchIt = result.records.batchIterator()
            while (batchIt.hasNext) {
              val next = batchIt.next()
              if (next.isValid)
                bytesRead.addAndGet(next.sizeInBytes())
            }
          }
        }
      }
    }
  }

  val shuttingDown = new AtomicBoolean(false)

  def main(args: Array[String]): Unit = {
    val opts = new StressLogOptions(args)

    val dir = new File(opts.logDir)
    Utils.delete(dir)

    val scheduler: KafkaScheduler = new KafkaScheduler(threads = 2)
    scheduler.startup()

    val defaultProps = KafkaServer.copyKafkaConfigToLog(opts.kafkaConfig)
    LogConfig.validateValues(defaultProps)
    val defaultLogConfig = LogConfig(defaultProps)

    val logManager = new LogManager(
      logDirs = Seq(dir),
      initialOfflineDirs = Seq.empty,
      topicConfigs = Map.empty,
      initialDefaultConfig = defaultLogConfig,
      cleanerConfig = CleanerConfig(enableCleaner = false),
      recoveryThreadsPerDataDir = opts.kafkaConfig.numRecoveryThreadsPerDataDir,
      flushCheckMs = opts.kafkaConfig.logFlushSchedulerIntervalMs,
      flushRecoveryOffsetCheckpointMs = opts.kafkaConfig.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = opts.kafkaConfig.logFlushStartOffsetCheckpointIntervalMs,
      retentionCheckMs = opts.kafkaConfig.logCleanupIntervalMs,
      maxPidExpirationMs = opts.kafkaConfig.transactionalIdExpirationMs,
      scheduler,
      brokerState = BrokerState(),
      brokerTopicStats = new BrokerTopicStats(),
      logDirFailureChannel = new LogDirFailureChannel(1),
      time = org.apache.kafka.common.utils.Time.SYSTEM
    )

    println("starting up")

    logManager.startup()

    val logWriters = (0 until opts.numWriters).map(i =>
      (i, new LogWriter(logManager.getOrCreateLog(new TopicPartition("stress", i), defaultLogConfig))))

    val logReaders = (0 until opts.numWriters).flatMap(i => {
      (0 until opts.readFanout).map { subid =>
        (i + "-" + subid, new LogReader(logManager.getOrCreateLog(new TopicPartition("stress", i), defaultLogConfig)))
      }
    })

    val writerThreads = logWriters.map {
      case (id, writer) =>
        KafkaThread.nonDaemon("writer-" + id, writer)
    }

    val readerThreads = logReaders.map {
      case (id, reader) =>
        KafkaThread.nonDaemon("reader-" + id, reader)
    }

    writerThreads.foreach(t => {
      // not setting uncaught exception handler
      t.start()
    })

    readerThreads.foreach(t => {
      // not setting uncaught exception handler
      t.start()
    })

    scheduler.schedule("progress",
      () => {
        logWriters.foreach {
          case (id, writer) =>
            println("writer " + id + " - wrote " + writer.bytesWritten.getAndSet(0L) + " bytes ; " + writer.totalBytesWritten.get() + " bytes total")
        }
        logReaders.foreach {
          case (id, reader) =>
            println("reader " + id + " - read " + reader.bytesRead.getAndSet(0L) + " bytes")
        }
      },
      delay = 5000L,
      period = 5000L,
      TimeUnit.MILLISECONDS)

    def cleanShutdown(): Unit = {
      println("shutting down")
      writerThreads.foreach(_.join())
      readerThreads.foreach(_.join())
      CoreUtils.swallow(scheduler.shutdown(), this)
      CoreUtils.swallow(logManager.shutdown(), this)
    }

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        cleanShutdown()
      }
    })

    // wait for duration
    CoreUtils.swallow(Thread.sleep(opts.duration.toMillis), this)
    shuttingDown.set(true)

    // shutdown hook runs on exit
  }

  class StressLogOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    private val numWritersOpt = parser.accepts("num-writers", "number of parallel writers")
      .withOptionalArg()
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    private val readFanoutOpt = parser.accepts("read-fanout", "read fanout - instantiate this many readers")
      .withOptionalArg()
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(0)
    private val durationSecsOpt = parser.accepts("duration-secs", "duration of the stress test in seconds (if uninterrupted by user)")
      .withOptionalArg()
      .ofType(classOf[java.lang.Long])
      .defaultsTo(Duration.ofSeconds(10).getSeconds)
    private val kafkaConfigOpt = parser.accepts("kafka-config", "configuration for the log manager (should use Kafka config properties, not log config)")
      .withOptionalArg()
      .ofType(classOf[java.lang.String])
    private val logDirOpt = parser.accepts("log-dir", "directory where log files will be written")
        .withRequiredArg()
        .ofType(classOf[String])

    options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, logDirOpt)

    val numWriters: Integer = options.valueOf(numWritersOpt).intValue()
    val readFanout: Integer = options.valueOf(readFanoutOpt).intValue()
    val duration: Duration = Duration.ofSeconds(options.valueOf(durationSecsOpt).longValue())
    val logDir: String = options.valueOf(logDirOpt)
    val kafkaConfig: KafkaConfig = if (options.hasArgument(kafkaConfigOpt)) {
      val props = Utils.loadProps(options.valueOf(kafkaConfigOpt))
      props.put("log.message.format.version", "0.10.0") // force message format 0.10.0
      new KafkaConfig(props)
    } else {
      val props = new Properties()
      props.put("zookeeper.connect", "localhost:0")
      props.put("log.message.format.version", "0.10.0") // force message format 0.10.0
      new KafkaConfig(props)
    }
  }
}
