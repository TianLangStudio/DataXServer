package org.tianlangstudio.data.hamal.yarn

import java.io.{File, PrintWriter}
import java.nio.charset.Charset
import java.util
import java.util.{Collections, UUID}

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.AddressTerminatedTopic
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{AMRMClient, NMClient}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.slf4j.LoggerFactory
import org.tianlangstudio.data.hamal.core.{Constants, HamalConf}
import org.tianlangstudio.data.hamal.core.handler.ITaskHandler
import org.tianlangstudio.data.hamal.server.http.HttpServerApp
import org.tianlangstudio.data.hamal.server.thrift.ThriftServerApp
import org.tianlangstudio.data.hamal.yarn.server.handler.AkkaTaskHandler
import org.tianlangstudio.data.hamal.yarn.thrift.AkkaThriftTaskHandler
import org.tianlangstudio.data.hamal.yarn.util.{AkkaUtils, Utils}
import org.tianlangstudio.data.hamal.core.HamalConf

//import org.apache.log4j.PropertyConfigurator

//import scala.collection.JavaConversions._

object ApplicationMaster extends App {
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[ApplicationMaster])
  private val hamalConf:HamalConf = new HamalConf()


  /**
   * 加载日志配置文件
   */
  //PropertyConfigurator.configure("masterConf/log4j.properties")
  //传递参数false 代表不是使用onyarn模式运行　只使用本机executor  即多进程模式运行任务
  private val isOnYarn = if(args.length > 0) {
    !"false".equalsIgnoreCase(args(0))
  }else {
    true
  }

  val yarnConfiguration: Configuration = new YarnConfiguration()
  /* if(rmSchAddressOpt.isDefined) {
  yarnConfiguration.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, rmSchAddressOpt.get)
}*/

  logger.info("rm scheduler address:{}", yarnConfiguration.get(YarnConfiguration.RM_SCHEDULER_ADDRESS))
  logger.info(s"yarn configuration:$yarnConfiguration")

  //yarnConfiguration.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,1000 * 60 * 60 * 2)//设置单个 application超时时间 未起作用


  val executorCores =
  hamalConf.getInt(Constants.DATAX_EXECUTOR_CORES, 1)
  var executorResource: Resource =
  Records.newRecord(classOf[Resource])
  var executorPriority: Priority =
  Records.newRecord(classOf[Priority])


  executorResource = Records.newRecord(classOf[Resource])
  executorResource.setMemory(1536)
  executorResource.setVirtualCores(executorCores)

  executorPriority = Records.newRecord(classOf[Priority])
  executorPriority.setPriority(0)
  executorPriority = Records.newRecord(classOf[Priority])

  val nmClient:NMClient = NMClient.createNMClient()
  logger.info("create amrmClientAsync");
  val amrmClientAsync:AMRMClientAsync[AMRMClient.ContainerRequest] =
    AMRMClientAsync.createAMRMClientAsync(2000,
      new RMCallbackHandler(nmClient,getContainerCmd _,
        hamalConf,yarnConfiguration))
  logger.info("create amActor");


  if(isOnYarn) {
    amrmClientAsync.init(yarnConfiguration)
    amrmClientAsync.start()

    logger.info("register application master begin")
    amrmClientAsync.registerApplicationMaster("",0,"")
    logger.info("register application master end")
    nmClient.init(yarnConfiguration)
    nmClient.start();
    sys.addShutdownHook{
      amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.UNDEFINED,"","")
    }
  }

  logger.info("create master actor system begin");
  val schedulerHost = hamalConf.getString(Constants.DATAX_MASTER_HOST,"127.0.0.1")
  logger.info("scheduler host:{}", schedulerHost);
  val (schedulerSystem,port) = AkkaUtils.createActorSystem(Constants.AKKA_JOB_SCHEDULER_SYSTEM,schedulerHost,0,hamalConf)
  logger.info("create master actor system end");
  sys.addShutdownHook{
    schedulerSystem.terminate()
  }
  private val taskSchedulerHostPost:String = s"${hamalConf.getString(Constants.DATAX_MASTER_HOST)}:$port"
  private val archiveName = Constants.DATAX_EXECUTOR_ARCHIVE_FILE_NAME
  private val dataxHome = hamalConf.getString(Constants.DATAX_HOME, ApplicationConstants.Environment.PWD.$())

  //private val runEnv = hamalConf.getString(Constants.RUN_ENV, Constants.RUN_ENV_PRODUCTION).toLowerCase()
  val currentClassPath = System.getProperty("java.class.path")
  //获取ClassPath
  private val executorCP = hamalConf.getString(
      Constants.DATAX_EXECUTOR_CP,
      s"./*:$archiveName/*:$archiveName/lib/*:$archiveName/common/*:" +
        s"$archiveName/conf/*:$archiveName/datax/*:$archiveName/datax/lib/*:" +
        s"$archiveName/datax/common/*:$archiveName/datax/conf/*:" +
        s"$currentClassPath"
  )

  val cmdDefault =  s"java -classpath  $executorCP " +
    s" -Ddatax.home=$dataxHome -Xms512M -Xmx1024M " +
    s" -XX:PermSize=128M -XX:MaxPermSize=512M org.tianlangstudio.data.hamal.yarn.Executor"

  val localCmdDefault = s"$cmdDefault "
  val executorLocalCmd  = hamalConf.getString(Constants.DATAX_EXECUTOR_LOCAL_CMD, localCmdDefault)
  logger.info(s"local executor start cmd:$executorLocalCmd")

  private val executorCmd:String = hamalConf.getString(
                Constants.DATAX_EXECUTOR_CMD,
                cmdDefault
              ) +
              s" $taskSchedulerHostPost ${Constants.PLACEHOLDER_EXECUTOR_ID} ${Constants.EXECUTOR_RUN_ON_TYPE_YARN}" +
              s" 1> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout " +
              s" 2> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr"
  logger.info(s"yarn executor start cmd:$executorCmd");
  val amActor = schedulerSystem.actorOf(Props(classOf[ApplicationMaster],
    hamalConf,
    yarnConfiguration,
    executorResource,
    executorPriority,
    amrmClientAsync,
    nmClient),
    Constants.AKKA_AM_ACTOR
  )
  try {
    val hostPortWriter = new PrintWriter(new File("masterHostPort"),"UTF-8")
    hostPortWriter.print(taskSchedulerHostPost)
    hostPortWriter.close()
  }catch {
    case ex:Throwable =>
      logger.error("writer host port error",ex)
  }
  logger.info("executor cmd:" + executorCmd)
  def getContainerCmd(container: Container) = {
    val executorId = Utils.containerIdNodeId2ExecutorId(container.getId,container.getNodeId)
    executorCmd.replace(Constants.PLACEHOLDER_EXECUTOR_ID,executorId)
  }

  val taskSchedulerActor = schedulerSystem.actorOf(Props(classOf[TaskScheduler],hamalConf,amActor),Constants.AKKA_JOB_SCHEDULER_ACTOR)
  taskSchedulerActor ! "taskSchedulerActor started"

  logger.info(s"address:${taskSchedulerActor.path.address.hostPort}   ${taskSchedulerActor.path}  ${taskSchedulerActor.path.address}")

  def startThriftServer(taskSchedulerActor: ActorRef, hamalConf: HamalConf): Unit = {
    logger.info("start thrift server begin")
    val thriftPort = hamalConf.getInt(Constants.THRIFT_SERVER_PORT,9777)
    val thriftHost = hamalConf.getString(Constants.THRIFT_SERVER_HOST,"127.0.0.1")
    val thriftServerHandler = new AkkaThriftTaskHandler(taskSchedulerActor)
    logger.info(s"start thrift server on  $thriftHost:$thriftPort")
    ThriftServerApp.start(thriftHost,thriftPort,thriftServerHandler)
  }
  def startHttpServer(taskSchedulerActor: ITaskHandler, hamalConf: HamalConf): Unit = {
    logger.info("start http server begin")
    HttpServerApp.start(taskSchedulerActor, hamalConf)(schedulerSystem)
  }
  try{
     startHttpServer(new AkkaTaskHandler(taskSchedulerActor), hamalConf)
     startThriftServer(taskSchedulerActor, hamalConf)
  }catch {
    case ex:Exception =>
      logger.error("start server error",ex)
      //amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.UNDEFINED,"","")
      schedulerSystem.terminate()
  }

}
/**
 * Created by zhuhq on 2016/4/27.
 */
class ApplicationMaster(
                         hamalConf: HamalConf,
                         yarnConfiguration: Configuration,
                         executorResource:Resource,
                         executorPriority:Priority,
                         aMRMClient: AMRMClientAsync[AMRMClient.ContainerRequest],
                         nMClient: NMClient
                         ) extends Actor with ActorLogging{


  val containerLocalCmd = ApplicationMaster.executorLocalCmd
  def applyExecutor(count:Int = 1) = {
    if(aMRMClient != null  && nMClient != null) {
      applyExecutorYarn(count)
    }else {
      log.warning("is not on yarn mode")
    }

  }
  def applyExecutorLocal(count:Int = 1) = {
    val executorCount = math.max(count,1);
    log.info(s"apply executor local $count begin")
    for(_ <- 1 to executorCount) {
      val cmd =  containerLocalCmd + " " +
        ApplicationMaster.taskSchedulerHostPost + " " +
        UUID.randomUUID().toString + " " +
        Constants.EXECUTOR_RUN_ON_TYPE_LOCAL
      log.info(s"apply executor local cmd $cmd")
      sys.process.stringToProcess(cmd).run()
    }
    log.info(s"apply executor local $count end")
  }
  def applyExecutorYarn(count:Int = 1) = {
    val executorCount = math.max(count,1);
    log.info(s"apply executor yarn $count begin")
    for(_ <- 1 to executorCount) {
      val containerAsk = new ContainerRequest(executorResource,null,null,executorPriority);
      aMRMClient.addContainerRequest(containerAsk);
    }
    log.info(s"apply executor yarn $count end")

  }
  override def receive = {
    case ApplyExecutor(count) =>
      applyExecutor(count)
    case ApplyExecutorLocal(count) =>
      applyExecutorLocal(count)
    case ApplyExecutorYarn(count) =>
      applyExecutorYarn(count)
    case ReturnExecutor(executorId) =>
      Utils.executorId2ContainerIdNodeId(executorId) match {
        case Some((containerId,nodeId)) =>
          nMClient.stopContainer(containerId,nodeId)
          log.info(s"stop container of executor $executorId")
        case _ =>
          log.info(s"container of executor $executorId is not yarn container")
      }
    case _ => {

    }
  }

}
case class ApplyExecutor(count:Int = 1)
case class ApplyExecutorYarn(count:Int = 1)
case class ApplyExecutorLocal(count:Int = 1)
case class ReturnExecutor(executorId:String)
case class RegisterAM()
