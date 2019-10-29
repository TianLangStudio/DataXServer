package org.tianlangstudio.data.hamal.yarn.local

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Props}
import org.slf4j.LoggerFactory
import org.tianlangstudio.data.hamal.core.{Constants, HamalConf}
import org.tianlangstudio.data.hamal.server.thrift.ThriftServerApp
import org.tianlangstudio.data.hamal.yarn.{ApplyExecutor, TaskScheduler}
import org.tianlangstudio.data.hamal.yarn.thrift.AkkaThriftTaskHandler
import org.tianlangstudio.data.hamal.yarn.util.AkkaUtils
import org.tianlangstudio.data.hamal.core.HamalConf

/**
 * Created by zhuhq on 2016/5/3.
  * 在本机申请运行资源，多进程方式批量运行任务
 */
object LocalApplicationMaster extends App{
  val logging = org.slf4j.LoggerFactory.getLogger(classOf[LocalApplicationMaster])
  val dataxConf = new HamalConf()
  logging.info("create master actor system begin");
  val schedulerHost = dataxConf.getString(Constants.DATAX_MASTER_HOST,"127.0.0.1")
  val (schedulerSystem,schedulerPort) = AkkaUtils.createActorSystem(Constants.AKKA_JOB_SCHEDULER_SYSTEM,schedulerHost,0,dataxConf)
  logging.info(s"create master actor system end on port $schedulerPort");
  val amActor = schedulerSystem.actorOf(Props(classOf[LocalApplicationMaster],dataxConf),Constants.AKKA_AM_ACTOR)
  val taskSchedulerActor = schedulerSystem.actorOf(Props(classOf[TaskScheduler],dataxConf,amActor),Constants.AKKA_JOB_SCHEDULER_ACTOR)
  taskSchedulerActor ! "start taskSchedulerActor"
  logging.info(s"start thrift server begin")
  val thriftPort = dataxConf.getInt(Constants.THRIFT_SERVER_PORT,9777)
  val thriftHost = dataxConf.getString(Constants.THRIFT_SERVER_HOST,"127.0.0.1")
  val thriftConcurrence = dataxConf.getInt(Constants.THRIFT_SERVER_CONCURRENCE,8)
  val thriftServerHandler = new AkkaThriftTaskHandler(taskSchedulerActor)

  logging.info(s"start thrift server on  $thriftHost:$thriftPort")
  ThriftServerApp.start(thriftHost,thriftPort,thriftServerHandler)

}
class LocalApplicationMaster(dataxConf: HamalConf) extends Actor with ActorLogging{
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
          LocalApplicationMaster.schedulerHost + ":" + LocalApplicationMaster.schedulerPort + " " +
          UUID.randomUUID().toString).run()
      log.info(s"apply executor ${i+1}/$num")
    }

  }
}
