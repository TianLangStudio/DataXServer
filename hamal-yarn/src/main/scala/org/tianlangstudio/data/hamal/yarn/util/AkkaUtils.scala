package org.tianlangstudio.data.hamal.yarn.util

import akka.actor.{ActorSystem, ExtendedActorSystem}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.tianlangstuido.data.hamal.core.HamalConf

/**
 * Various utility classes for working with Akka.
 */
private[hamal] object AkkaUtils{

  /**
   * Creates an ActorSystem ready for remoting, with various Spark features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   *
   * Note: the `name` parameter is important, as even if a client sends a message to right
   * host + port, if the system name is incorrect, Akka will drop the message.
   *
   * If indestructible is set to true, the Actor System will continue running in the event
   * of a fatal exception. This is used by [[org.apache.spark.executor.Executor]].
   */
  def createActorSystem(
      name: String,
      host: String,
      port: Int,
      conf: HamalConf): (ActorSystem, Int) = {
    val startService: Int => (ActorSystem,Int) = { actualPort =>
      doCreateActorSystem(name, host, actualPort, conf)
    }
    Utils.startServiceOnPort(port, startService, conf, name)
  }

  private def doCreateActorSystem(
      name: String,
      host: String,
      port: Int,
      conf: HamalConf): (ActorSystem,Int) = {
    println(s" do create actor system on port $port")
    val akkaThreads = conf.getInt("datax.akka.threads", 4)
    val akkaBatchSize = conf.getInt("datax.akka.batchSize", 15)
    val akkaTimeoutS = "120"
    val akkaFrameSize = maxFrameSizeBytes(conf)
    val akkaLogLifecycleEvents = conf.getBoolean("spark.akka.logLifecycleEvents", false)
    val lifecycleEvents = if (akkaLogLifecycleEvents) "on" else "off"
    if (!akkaLogLifecycleEvents) {
      // As a workaround for Akka issue #3787, we coerce the "EndpointWriter" log to be silent.
      // See: https://www.assembla.com/spaces/akka/tickets/3787#/
      Option(Logger.getLogger("akka.remote.EndpointWriter")).map(l => l.setLevel(Level.FATAL))
    }

    val logAkkaConfig = if (conf.getBoolean("spark.akka.logAkkaConfig", false)) "on" else "off"

    val akkaHeartBeatPausesS = "6000"
    val akkaHeartBeatIntervalS = "1000"
    //akka.stdout-loglevel = "DEBUG"
    val akkaConf =
      //.withFallback(ConfigFactory.parseString(
      ConfigFactory.parseString(
      s"""
      |akka.loggers = ["akka.event.slf4j.Slf4jLogger"]
      |akka.jvm-exit-on-fatal-error = off
      |akka.remote.transport-failure-detector.heartbeat-interval = $akkaHeartBeatIntervalS s
      |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPausesS s
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.connection-timeout = $akkaTimeoutS s
      |akka.remote.netty.tcp.maximum-frame-size = ${akkaFrameSize}B
      |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
      |akka.actor.default-dispatcher.throughput = $akkaBatchSize
      |akka.log-config-on-start = $logAkkaConfig
      |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
      |akka.log-dead-letters = $lifecycleEvents
      |akka.log-dead-letters-during-shutdown = $lifecycleEvents
      """.stripMargin).withFallback(conf.getConf)
    //)
      println("tcp.port:" + akkaConf.getString("akka.remote.netty.tcp.port"))
      val actorSystem = ActorSystem(name, akkaConf)
      val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider

      val boundPort = provider.getDefaultAddress.port.get
      println(s"create actor system on port:$boundPort")
     (actorSystem,boundPort)
  }

  private val AKKA_MAX_FRAME_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /** Returns the configured max frame size for Akka messages in bytes. */
  def maxFrameSizeBytes(conf: HamalConf): Int = {
    val frameSizeInMB = conf.getInt("datax.akka.frameSize", 128)
    if (frameSizeInMB > AKKA_MAX_FRAME_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"spark.akka.frameSize should not be greater than $AKKA_MAX_FRAME_SIZE_IN_MB MB")
    }
    frameSizeInMB * 1024 * 1024
  }


  def protocol(actorSystem: ActorSystem): String = {
    val akkaConf = actorSystem.settings.config
    val sslProp = "akka.remote.netty.tcp.enable-ssl"
    protocol(akkaConf.hasPath(sslProp) && akkaConf.getBoolean(sslProp))
  }

  def protocol(ssl: Boolean = false): String = {
    if (ssl) {
      "akka.ssl.tcp"
    } else {
      "akka.tcp"
    }
  }

  def address(
      protocol: String,
      systemName: String,
      host: String,
      port: Int,
      actorName: String): String = {

        address(protocol,
          systemName,
          s"$host:$port",
          actorName
        )
  }
  def address(
               protocol: String,
               systemName: String,
               hostPort: String,
               actorName: String): String = {
    s"$protocol://$systemName@$hostPort/user/$actorName"
  }
}
