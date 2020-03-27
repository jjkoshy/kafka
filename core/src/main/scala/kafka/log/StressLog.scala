package kafka.log

import java.io.File
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import kafka.api.ApiVersion
import kafka.server.{BrokerState, BrokerTopicStats, LogDirFailureChannel}
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

  val shuttingDown = new AtomicBoolean(false)

  def main(args: Array[String]): Unit = {
    val opts = new StressLogOptions(args)

    val dir = new File(opts.logDir)
    Utils.delete(dir)

    val scheduler: KafkaScheduler = new KafkaScheduler(threads = 2)
    scheduler.startup()

    val logManager = new LogManager(
      logDirs = Seq(dir),
      initialOfflineDirs = Seq.empty,
      topicConfigs = Map.empty,
      initialDefaultConfig = opts.logConfig,
      cleanerConfig = CleanerConfig(enableCleaner = false),
      recoveryThreadsPerDataDir = 1,
      flushCheckMs = 1000L,
      flushRecoveryOffsetCheckpointMs = 10000L,
      flushStartOffsetCheckpointMs = 10000L,
      retentionCheckMs = 1000L,
      maxPidExpirationMs = 60 * 60 * 1000,
      scheduler,
      brokerState = BrokerState(),
      brokerTopicStats = new BrokerTopicStats(),
      logDirFailureChannel = new LogDirFailureChannel(1),
      time = org.apache.kafka.common.utils.Time.SYSTEM
    )

    println("starting up")
    println("log config:")

    println(opts.logConfig.values())

    logManager.startup()

    val logWriters = (0 until opts.numWriters).map(i =>
      (i, new LogWriter(logManager.getOrCreateLog(new TopicPartition("stress", i), opts.logConfig))))

    val threads = logWriters.map {
      case (id, writer) =>
        KafkaThread.nonDaemon("writer-" + id, writer)
    }

    threads.foreach(t => {
      // not setting uncaught exception handler
      t.start()
    })

    scheduler.schedule("progress",
      () => logWriters.foreach {
        case (id, writer) =>
          println("writer " + id + " - wrote " + writer.bytesWritten.getAndSet(0L) + " bytes ; " + writer.totalBytesWritten.get() + " bytes total")
      },
      delay = 5000L,
      period = 5000L,
      TimeUnit.MILLISECONDS)

    def cleanShutdown(): Unit = {
      println("shutting down")
      threads.foreach(_.join())
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
    private val durationSecsOpt = parser.accepts("duration-secs", "duration of the stress test in seconds (if uninterrupted by user)")
      .withOptionalArg()
      .ofType(classOf[java.lang.Long])
      .defaultsTo(Duration.ofSeconds(10).getSeconds)
    private val logConfigOpt = parser.accepts("log-config", "configuration for the log manager")
      .withOptionalArg()
      .ofType(classOf[java.lang.String])
    private val logDirOpt = parser.accepts("log-dir", "directory where log files will be written")
        .withRequiredArg()
        .ofType(classOf[String])

    options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, logDirOpt)

    val numWriters: Integer = options.valueOf(numWritersOpt).intValue()
    val duration: Duration = Duration.ofSeconds(options.valueOf(durationSecsOpt).longValue())
    val logDir: String = options.valueOf(logDirOpt)
    val logConfig: LogConfig = if (options.hasArgument(logConfigOpt)) {
      new LogConfig(Utils.loadProps(options.valueOf(logConfigOpt)))
    } else {
      LogConfig()
    }
  }

}
