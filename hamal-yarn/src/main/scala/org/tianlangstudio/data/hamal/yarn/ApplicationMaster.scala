package org.tianlangstudio.data.hamal.yarn

import java.io.{File, PrintWriter}
import java.nio.charset.Charset
import java.util
import java.util.{Collections, UUID}

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, Props}
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
import org.tianlangstudio.data.hamal.server.thrift.ThriftServerApp
import org.tianlangstudio.data.hamal.yarn.thrift.AkkaThriftTaskHandler
import org.tianlangstudio.data.hamal.yarn.util.{AkkaUtils, Utils}
import org.tianlangstuido.data.hamal.core.{Constants, HamalConf}

//import org.apache.log4j.PropertyConfigurator

//import scala.collection.JavaConversions._

object ApplicationMaster extends App {

  /**
   * 加载日志配置文件
   */
  //PropertyConfigurator.configure("masterConf/log4j.properties")
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[ApplicationMaster])
  private val rmSchAddressOpt = if(args.length > 0) {
    Some(args(0))
  }else {
    None
  }

  private val yarnConfiguration:Configuration = new YarnConfiguration()
  if(rmSchAddressOpt.isDefined) {
    yarnConfiguration.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, rmSchAddressOpt.get)
  }

  logger.info("rm scheduler address:{}", yarnConfiguration.get(YarnConfiguration.RM_SCHEDULER_ADDRESS))
  logger.info(s"yarn configuration:$yarnConfiguration")

  //yarnConfiguration.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,1000 * 60 * 60 * 2)//设置单个 application超时时间 未起作用

  private val hamalConf:HamalConf = new HamalConf()

  private val executorCores = hamalConf.getInt(Constants.DATAX_EXECUTOR_CORES,1)
  private var executorResource:Resource = Records.newRecord(classOf[Resource])
  private var executorPriority:Priority = Records.newRecord(classOf[Priority])



  executorResource = Records.newRecord(classOf[Resource])
  executorResource.setMemory(1636)
  executorResource.setVirtualCores(executorCores)

  executorPriority = Records.newRecord(classOf[Priority])
  executorPriority.setPriority(0)
  executorPriority = Records.newRecord(classOf[Priority])

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

  private val runEnv = hamalConf.getString(Constants.RUN_ENV, Constants.RUN_ENV_PRODUCTION).toLowerCase()

  private val (executorCP, executorLocalCmd) = if(Constants.RUN_ENV_DEVELOPMENT.equals(runEnv)) {
    val classPath = System.getProperty("java.class.path")
    val localCmd =  s"java -classpath $classPath " +
              s" -Ddatax.home=$dataxHome -Xms512M -Xmx1024M " +
              s" -XX:PermSize=128M -XX:MaxPermSize=512M com.tianlangstudio.data.datax.Executor "
    (classPath, localCmd)
  }else {
    val classPath = hamalConf.getString(
      Constants.DATAX_EXECUTOR_CP,
      s"$archiveName/*:$archiveName/lib/*:$archiveName/common/*:$archiveName/conf/*:$archiveName/datax/*:$archiveName/datax/lib/*:$archiveName/datax/common/*:$archiveName/datax/conf/*"
    )
    val localCmd  = hamalConf.getString(Constants.DATAX_EXECUTOR_LOCAL_CMD, "./startLocalExecutor.sh")
    (classPath, localCmd)
  }

  private val executorCmd:String = hamalConf.getString(
                Constants.DATAX_EXECUTOR_CMD,
                s"java -classpath $executorCP -Ddatax.home=$dataxHome -Xms512M -Xmx1024M -XX:PermSize=128M -XX:MaxPermSize=512M com.tianlangstudio.data.datax.Executor "
              ) +
              s" $taskSchedulerHostPost ${Constants.PLACEHOLDER_EXECUTOR_ID} ${Constants.EXECUTOR_RUN_ON_TYPE_YARN}" +
              s" 1> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout " +
              s" 2> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr"
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
  private val nmClient:NMClient = NMClient.createNMClient()
  private val amrmClientAsync:AMRMClientAsync[AMRMClient.ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(2000,new RMCallbackHandler(nmClient,getContainerCmd _,hamalConf,yarnConfiguration))

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

  val amActor = schedulerSystem.actorOf(Props(classOf[ApplicationMaster],hamalConf,yarnConfiguration,executorResource,executorPriority,amrmClientAsync,nmClient),Constants.AKKA_AM_ACTOR)

  val taskSchedulerActor = schedulerSystem.actorOf(Props(classOf[TaskScheduler],hamalConf,amActor),Constants.AKKA_JOB_SCHEDULER_ACTOR)
  taskSchedulerActor ! "taskSchedulerActor started"

  logger.info(s"address:${taskSchedulerActor.path.address.hostPort}   ${taskSchedulerActor.path}  ${taskSchedulerActor.path.address}")

  logger.info(s"start thrift server begin")
  val thriftPort = hamalConf.getInt(Constants.THRIFT_SERVER_PORT,9777)
  val thriftHost = hamalConf.getString(Constants.THRIFT_SERVER_HOST,"127.0.0.1")
  val thriftConcurrence = hamalConf.getInt(Constants.THRIFT_SERVER_CONCURRENCE,8)
  val thriftServerHandler = new AkkaThriftTaskHandler(taskSchedulerActor)

  logger.info(s"start thrift server on  $thriftHost:$thriftPort")
  try{
    ThriftServerApp.start(thriftHost,thriftPort,thriftServerHandler)
  }catch {
    case ex:Exception =>
      amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.UNDEFINED,"","")
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
    applyExecutorYarn(count)
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
