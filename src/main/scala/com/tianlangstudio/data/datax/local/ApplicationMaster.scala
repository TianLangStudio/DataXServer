package com.tianlangstudio.data.datax.local

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Props}
import akka.actor.Actor.Receive
import com.tianlangstudio.data.datax.main.ThriftServerMain
import com.tianlangstudio.data.datax.thrift.AkkaThriftJobHandler
import com.tianlangstudio.data.datax.yarn.ApplyExecutor
import com.tianlangstudio.data.datax.{Constants, DataxConf}
import com.tianlangstudio.data.datax.util.AkkaUtils
import com.tianlangstudio.data.datax.JobScheduler
import org.slf4j.LoggerFactory

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
  val thriftServerHandler = new AkkaThriftJobHandler(jobSchedulerActor)

  logging.info(s"start thrift server on  $thriftHost:$thriftPort")
  ThriftServerMain.start(thriftConcurrence,thriftHost,thriftPort,thriftServerHandler)

}
class ApplicationMaster(dataxConf: DataxConf) extends Actor with ActorLogging{
  private val logger = LoggerFactory.getLogger(getClass)
  val runEnv = dataxConf.getString(Constants.RUN_ENV, Constants.RUN_ENV_PRODUCTION).toLowerCase()
  logger.info("run env:{}", runEnv)
  val containerCmd = if(Constants.RUN_ENV_DEVELOPMENT.equals(runEnv)) {
    s"""
       |java ${System.getProperty("java.class.path")}
       | -Ddatax.home=${dataxConf.getString(Constants.DATAX_HOME)} -Xms512M -Xmx1024M
       |  -XX:PermSize=128M -XX:MaxPermSize=512M com.tianlangstudio.data.datax.Executor
     """.stripMargin
  }else {
    dataxConf.getString(Constants.DATAX_EXECUTOR_CMD, "./startLocalExecutor.sh")
  }


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
      log.info(s"apply executor ${i+1}/$num")
    }

  }
}
