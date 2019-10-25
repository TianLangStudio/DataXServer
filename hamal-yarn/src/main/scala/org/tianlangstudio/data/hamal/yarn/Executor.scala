package org.tianlangstudio.data.hamal.yarn

import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor._
import org.slf4j.LoggerFactory
import org.tianlangstudio.data.hamal.core.Engine
import org.tianlangstudio.data.hamal.yarn.util.AkkaUtils
import org.tianlangstuido.data.hamal.common.TaskResult
import org.tianlangstuido.data.hamal.core.{Constants, HamalConf}



/**
 * Created by zhuhq on 2016/4/27.
 */
object Executor extends App{
  private val logging = LoggerFactory.getLogger(classOf[Executor])
  val dataxConf = new HamalConf(Constants.DATAX_EXECUTOR_CONF_NAME)
  implicit val (system,port) = AkkaUtils.createActorSystem(
     Constants.AKKA_JOB_SCHEDULER_SYSTEM,
     "127.0.0.1",
     0,
     dataxConf
  )
  val taskSchedulerHostPort = args(0);

  val id = if(args.size > 1) {
    args(1)
  }else {
    UUID.randomUUID().toString
  }
  val runOnType = if(args.size > 2) {
    args(2)
  }else {
    Constants.EXECUTOR_RUN_ON_TYPE_LOCAL
  }
  val schedulerSelector = AkkaUtils.address(
    Constants.AKKA_PROTOCOL,
    Constants.AKKA_JOB_SCHEDULER_SYSTEM,
    taskSchedulerHostPort,
    Constants.AKKA_JOB_SCHEDULER_ACTOR
  )
  logging.info(s"start executor $id begin")
  val executorActor = system.actorOf(Props(classOf[Executor],schedulerSelector,id,runOnType),id)
  executorActor ! Register2Scheduler()
  sys.addShutdownHook {
    system.terminate()
  }
  logging.info(s"start executor $id end")
}
class Executor(schedulerPath:String,id:String,runOnType:String) extends Actor with ActorLogging{
  val engine = new Engine();
  import context.dispatcher
  //在一定时间内未向Scheduler注册成功 就停止运行
  var shutdown = context.system.scheduler.scheduleOnce(60.seconds,self,Shutdown())
  var taskScheduler:ActorRef = _
  override def receive={
    case SubmitTask(taskId,taskDesc) =>


      Future{
        log.info(s"executor $id receive task $taskId")
        val result = try {
          engine.start(taskDesc, taskId)
          new TaskResult()
        }catch {
          case ex:Exception =>
            log.error(ex,s"task $taskId error")
            new TaskResult(ex)
        }
        log.info(s"executor $id complete task $taskId result.success ${result.isSuccess} result.msg ${result.getMsg}")
        taskScheduler ! TaskCompleted(taskId,result)
      }


    case CancelTask(taskId) =>
      log.info(s"cancel task $taskId")
    case Register2Scheduler() =>
      log.info(s"to register on task scheduler:$schedulerPath")
      context.actorSelection(schedulerPath) ! Identify(schedulerPath)
    case ActorIdentity(`schedulerPath`,Some(ref)) => {
      log.info("register executor")
      if(taskScheduler != null) {
        ref ! RegisterExecutor(id,runOnType = runOnType)
      }else {//第一次注册
        ref ! RegisterExecutor(id,true,runOnType = runOnType)
      }
      taskScheduler = ref
      context.watch(ref)
    }
    case ActorIdentity(`schedulerPath`,None) => {
      log.warning(s"Remote actor not available: $schedulerPath")
    }
    case Shutdown() => {
      log.info("shutdown executor")
      context.system.terminate()
      log.info("sys exit")
      sys.exit()
    }
    case RegisterSuccess() =>
      if(shutdown != null && !shutdown.isCancelled) {
        shutdown.cancel()
        log.info("register success cancel shutdown")
      }
    case Terminated(_) =>
      //向task scheduler再次注册确认可以 正常访问master  如果不能正常访问 关闭当前executor
      if(shutdown != null && !shutdown.isCancelled) {
        shutdown.cancel()
      }
      log.info("shutdown is running")
      shutdown = context.system.scheduler.scheduleOnce(6.seconds,self,Shutdown())
      taskScheduler ! RegisterExecutor(id,true,runOnType = runOnType)
      log.info("Terminated event")
    case _ => {}
  }

}
case class Register2Scheduler()
