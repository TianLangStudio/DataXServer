package com.tianlangstudio.data.datax.local

import java.util.UUID

import akka.actor.{ActorLogging, Props, Actor}
import akka.actor.Actor.Receive
import com.tianlangstudio.data.datax.main.ThriftServerMain
import com.tianlangstudio.data.datax.thrift.AkkaThriftServerHandler
import com.tianlangstudio.data.datax.yarn.ApplyExecutor
import com.tianlangstudio.data.datax.{Constants, DataxConf}
import com.tianlangstudio.data.datax.util.AkkaUtils
import com.tianlangstudio.data.datax.JobScheduler

/**
 * Created by zhuhq on 2016/5/3.
 */
object ApplicationMaster extends App{
  val logging = org.slf4j.LoggerFactory.getLogger(classOf[ApplicationMaster])
  val dataxConf = new DataxConf()
  logging.info("create master actor system begin");
  val schedulerHost = dataxConf.getString(Constants.DATAX_MASTER_HOST,"127.0.0.1")
  val (schedulerSystem,schedulerPort) = AkkaUtils.createActorSystem(Constants.AKKA_JOB_SCHEDULER_SYSTEM,schedulerHost,0,dataxConf)
  logging.info(s"create master actor system end on port $schedulerPort");
  val amActor = schedulerSystem.actorOf(Props(classOf[ApplicationMaster],dataxConf),Constants.AKKA_AM_ACTOR)
  val jobSchedulerActor = schedulerSystem.actorOf(Props(classOf[JobScheduler],dataxConf,amActor),Constants.AKKA_JOB_SCHEDULER_ACTOR)
  jobSchedulerActor ! "start jobSchedulerActor"
  logging.info(s"start thrift server begin")
  val thriftPort = dataxConf.getInt(Constants.THRIFT_SERVER_PORT,9777)
  val thriftHost = dataxConf.getString(Constants.THRIFT_SERVER_HOST,"127.0.0.1")
  val thriftConcurrence = dataxConf.getInt(Constants.THRIFT_SERVER_CONCURRENCE,8)
  val thriftServerHandler = new AkkaThriftServerHandler(jobSchedulerActor)

  logging.info(s"start thrift server on  $thriftHost:$thriftPort")
  ThriftServerMain.start(thriftConcurrence,thriftHost,thriftPort,thriftServerHandler)

}
class ApplicationMaster(dataxConf: DataxConf) extends Actor with ActorLogging{
  val containerCmd = dataxConf.getString(Constants.DATAX_EXECUTOR_CMD, "D:\\datax\\startExecutor.bat")
  override def receive: Receive = {
    case msg:String =>
      log.info(s"${self.path} receive msg: $msg")
    case ApplyExecutor(num) =>
      applyExecutor(num)
  }
  private def applyExecutor(num:Int): Unit = {

    log.info(s"apply executor num $num");
    for(i <- 0 until num) {
      sys.process.stringToProcess(
          containerCmd + " " +
          ApplicationMaster.schedulerHost + ":" + ApplicationMaster.schedulerPort + " " +
          UUID.randomUUID().toString).run()
    }

  }
}
