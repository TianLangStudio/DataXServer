package org.tianlangstudio.data.hamal.yarn

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.remote.transport.AssociationHandle.Disassociated

import org.tianlangstuido.data.hamal.common.TaskResult
import org.tianlangstuido.data.hamal.core.{Constants, HamalConf}

import scala.collection.mutable
import scala.concurrent.duration._

/**
 * Created by zhuhq on 2016/4/27.
 */
/** *
  *
  * 与YarnMaster通信 启动 executor
  * 向executor分发任务
  * 记录executor任务状态
  *
  */
class TaskScheduler(dataxConf:HamalConf, amActor:ActorRef) extends Actor with ActorLogging{
  private val executorId2Executor = new mutable.HashMap[String,ActorRef]()//executors
  private val executorLocalList = new mutable.TreeSet[String]()
  private val executorYarnList = new mutable.TreeSet[String]()
  private val rerunTaskIds  = TaskInfo.rerunTaskIds
  private val taskId2TaskConf = TaskInfo.taskId2TaskConf
  private val acceptedTaskIds = TaskInfo.acceptedTaskIds//accepted tasks
  private val taskId2Result = TaskInfo.taskId2Result//complete tasks
  private val taskId2FailTimes = TaskInfo.taskId2FailTimes//need retry tasks
  private val taskId2ExecutorId = TaskInfo.taskId2ExecutorId//running tasks
  private val maxExecutor = dataxConf.getInt(Constants.DATAX_EXECUTOR_NUM_MAX,19)
  private val maxExecutorLocal = dataxConf.getInt(Constants.DATAX_EXECUTOR_LOCAL_NUM_MAX,5)
  private val applyExecutorLocalFirst = dataxConf.getBoolean(Constants.DATAX_EXECUTOR_APPLY_LOCAL_FIRST,true)
  private val taskInfoMaxNum = 10000
  //private val executorId2CheckNotifyTime = new mutable.HashMap[String,Deadline]()
  private val executorId2LiveTime = new mutable.HashMap[String,Deadline]()
  private val executorId2IdleTime = new mutable.HashMap[String,Deadline]()
  private val pollInterval = 2
  private val executorLiveTime = dataxConf.getInt(Constants.DATAX_EXECUTOR_REGISTER_INTERVAL_TIME,pollInterval * 6).seconds
  private val executorIdleTime = dataxConf.getInt(Constants.DATAX_EXECUTOR_IDLE_TIME,pollInterval * 18).seconds
  private var applyExecutorYarnCount = 0
  private var applyExecutorLocalCount = 0
  private val applyExecutorWaitTime = 3.minutes
  private var applyExecutorYarnCountResetLine = applyExecutorWaitTime.fromNow
  private var applyExecutorLocalCountResetLine = applyExecutorWaitTime.fromNow
  import context.dispatcher
  context.system.scheduler.scheduleOnce(pollInterval.seconds,self,Polling())
  log.info(s"executorLiveTime:$executorLiveTime idleTime:$executorIdleTime pollInterval:$pollInterval" +
    s" maxExecutor:$maxExecutor maxExecutorLocal:$maxExecutorLocal")
  override def receive={
    case msg:String =>
         log.info(s"${self.path} received message:$msg ")
    case RegisterExecutor(executorId,reply,runOnType) =>
      //注册包括 注册 和 心跳 监测  同一个Executor首次注册是注册 以后的是心跳
      var isCheck = true
      if(executorId2Executor.get(executorId).isEmpty) {//首次注册检查
        if(Constants.EXECUTOR_RUN_ON_TYPE_LOCAL == runOnType && executorLocalList.size >= maxExecutorLocal) {
          //local executor is grant to the max num
          isCheck = false
          sender ! Shutdown()
        }
        if(executorId2Executor.size >= maxExecutor) {
          isCheck = false
          sender ! Shutdown()
          if(runOnType == Constants.EXECUTOR_RUN_ON_TYPE_YARN) {
            amActor ! ReturnExecutor(executorId)
          }
        }
      }
      if(isCheck) {
        executorId2Executor.put(executorId,sender)
        if(Constants.EXECUTOR_RUN_ON_TYPE_LOCAL == runOnType) {
          executorLocalList += executorId
        }else {
          executorYarnList += executorId
        }
        executorId2LiveTime.put(executorId,executorLiveTime.fromNow)
        if(reply) {
          sender ! RegisterSuccess()
        }
        log.info(s"register executor executorId:$executorId")
      }

    case UnRegisterExecutor(executorId) =>
      unRegisterExecutor(executorId)
    case SubmitTask(taskId,taskConf) =>
      log.info(s"scheduler receive task $taskId");
      rerunTaskIds.add(taskId)
      if (acceptedTaskIds.contains(taskId)  || taskId2ExecutorId.contains(taskId)) {
        cancelTask(taskId)
      }
      taskId2TaskConf.put(taskId,taskConf)
      acceptedTaskIds.add(taskId)
      rerunTaskIds.remove(taskId)
    case TaskCompleted(taskId,taskResult) =>
      //log.info(s"task $taskId completed ${taskResult.success}   ${taskResult.getMsg}")
      taskId2Result.put(taskId,taskResult);
      taskId2FailTimes.remove(taskId);
      taskId2ExecutorId.remove(taskId);
    case CancelTask(taskId) =>
      log.info(s"cancel task $taskId begin")
      cancelTask(taskId)
      log.info(s"cancel task $taskId end")
    case GetTaskStatus(taskId) =>
      if(taskId2Result.get(taskId).isDefined) {
        Constants.TASK_STATUS_DONE
      }else if(taskId2TaskConf.get(taskId).isDefined) {
        Constants.TASK_STATUS_RUNNING
      }else {
        ""
      }
    case GetTaskResult(taskId) =>
      val resultOption = taskId2Result.get(taskId)
      if(resultOption.isDefined) {
        resultOption.get
      }else {
        null
      }
    case Polling() =>
      //巡检

      log.debug("polling begin")
      submitTask2Executor()
      clearUnnecessaryInfo[String](taskId2TaskConf,taskInfoMaxNum)
      clearUnnecessaryInfo[Int](taskId2FailTimes,taskInfoMaxNum)
      clearUnnecessaryInfo[TaskResult](taskId2Result,taskInfoMaxNum)
      checkExecutor()
      if(!applyExecutorLocalCountResetLine.hasTimeLeft()) {
        applyExecutorLocalCount = 0
      }
      if(!applyExecutorYarnCountResetLine.hasTimeLeft()) {
        applyExecutorYarnCount = 0
      }
      log.debug("polling end")
      context.system.scheduler.scheduleOnce(pollInterval.seconds,self,Polling())
    case _ => {

    }
  }
  private def checkExecutor(): Unit = {
      //从空闲列表里删除有任务在执行的Executor
      for((taskId,executorId) <- taskId2ExecutorId) {
        log.info(s"task $taskId  at executor $executorId")
        executorId2IdleTime.remove(executorId)
      }
      //注销空闲时间过长的Executor
      for((executorId,line) <- executorId2IdleTime if !line.hasTimeLeft()) {
        unRegisterExecutor(executorId)
      }
      //注销   live time已过 还没有重新注册的executor
      for((executorId,liveTime) <- executorId2LiveTime if !liveTime.hasTimeLeft()) {
        unRegisterExecutor(executorId)
      }
      //通知executor 重新注册以延长live time
      for((executorId,executor) <- executorId2Executor) {
        executorId2LiveTime.get(executorId) match {
          case Some(line) =>
            if(line.hasTimeLeft() && line.timeLeft <= executorLiveTime/2) {
              executor ! Register2Scheduler()
            }
        }
      }

      //添加空闲的Executor 到空闲列表
      for(executorId <- getIdleExecutor) {
        if(executorId2IdleTime.get(executorId).isEmpty) {
          println(s"add executor $executorId to idle list")
          executorId2IdleTime.put(executorId,executorIdleTime.fromNow)
        }

      }
  }
  private def unRegisterExecutor(executorId:String): Unit = {
    log.info(s"unregister executor executorId:$executorId begin")

    executorId2Executor.remove(executorId) match {
      case Some(executor) =>
        executor ! Shutdown()
      case _ => {}
    }
    if(executorLocalList.remove(executorId)) {
      if(applyExecutorLocalCount > 0) {
        applyExecutorLocalCount -= 1
      }
    }
    if(executorYarnList.remove(executorId)) {
      amActor ! ReturnExecutor(executorId)
      if(applyExecutorYarnCount > 0) {
        applyExecutorYarnCount -= 1
      }
    }
    executorId2LiveTime.remove(executorId)
    executorId2IdleTime.remove(executorId)
    //如果是cancelTask的 taskId 对应 的Executor已经被清空
    taskId2ExecutorId.find(j2e => j2e._2 == executorId) match {
      case Some((taskId,_)) =>
        taskId2ExecutorId.remove(taskId)
        self ! SubmitTask(taskId,taskId2TaskConf.get(taskId).get)
        log.info(s"resubmit running on unregister executor $executorId task $taskId ")
      case _ =>
        log.debug(s"no task on executor $executorId")
    }

    log.info(s"unregister executor executorId:$executorId end")
  }
  private def clearUnnecessaryInfo[B](info:mutable.LinkedHashMap[String,B],leaveNum:Int): Unit = {
    if(info.size > leaveNum) {
      info.drop(info.size - leaveNum)
    }
  }
  //如果有空闲executor 就执行待执行的task（accepted tasks &　need retry tasks）
  private var _firstToggle = true;
  private  def submitTask2Executor(): Unit = {
      log.debug("submitTask2Executor begin")
      val waitTaskNum = acceptedTaskIds.size()
      val idleExecutorIds = getIdleExecutor().toArray
      val idleExecutorNum = idleExecutorIds.size
      val currentExecutorNum = executorId2Executor.size
      log.info("executorNum:" + currentExecutorNum +
        " executorOnYarnNum:" + executorYarnList.size +
        " executorOnLocalNum:" + executorLocalList.size +
        " applyExecutorOnYarnNum:" + applyExecutorYarnCount +
        " applyExecutorOnLocalNum:" + applyExecutorLocalCount +
        " idleExecutorNum:" + idleExecutorNum +
        " runningTaskNum:" + taskId2ExecutorId.size +
        " waitTaskNum:" + waitTaskNum

      )
      if(waitTaskNum > 0) {
        log.info(s"idle executorNum:$idleExecutorNum")
        if(idleExecutorNum > 0) {
          val min = math.min(waitTaskNum,idleExecutorNum)
          for(i <- 0 until min) {
            val idleExecutorId = idleExecutorIds(i)
            executorId2Executor.get(idleExecutorId) match {
              case Some(executor) =>

                val taskId = if(_firstToggle) {
                  acceptedTaskIds.pollFirst()
                }else {
                  acceptedTaskIds.pollLast()
                }
                _firstToggle = !_firstToggle
                taskId2ExecutorId.get(taskId) match {//如果这个任务正在执行 取消执行
                  case Some(_) =>
                    cancelTask(taskId)
                  case _ => {}
                }
                taskId2TaskConf.get(taskId) match {
                  case Some(taskDesc) =>
                    executor ! SubmitTask(taskId,taskDesc)
                    taskId2ExecutorId.put(taskId,idleExecutorId)
                    executorId2IdleTime.remove(idleExecutorId)
                    log.info(s"task $taskId  at executor $idleExecutorId")
                  case _ =>
                    self ! TaskCompleted(taskId,new TaskResult("task config is lost"))
                }
              case _ =>
                self ! UnRegisterExecutor(idleExecutorId)
            }


          }
        }
        val needExecutorNum = acceptedTaskIds.size()
        log.info("need executor num:" + needExecutorNum)
        if(needExecutorNum > 0) {
          if(currentExecutorNum <  maxExecutor) {
            val applyNum = math.min(maxExecutor - currentExecutorNum,needExecutorNum)
            applyExecutor(applyNum);
          }
        }

      }
      log.debug("submitTask2Executor end")
  }
  private def applyExecutor(applyNum:Int): Unit = {
    if(applyExecutorLocalFirst) {
      applyExecutorLocal(applyNum)
    }else {
      applyExecutorYarn(applyNum)
    }

  }
  private def applyExecutorLocal(applyNum:Int): Unit = {
    log.info(s"apply executor local $applyNum")
    val executorLocalListSize = executorLocalList.size
    val waitRegisterLocalExecutorSize = math.max(applyExecutorLocalCount - executorLocalListSize,0);
    val needNum = applyNum - waitRegisterLocalExecutorSize
    if(needNum > 0) {
      if( executorLocalListSize < maxExecutorLocal
        && applyExecutorLocalCount < maxExecutorLocal) {
        //可以申请的 local Executor 数量
        val ableNum = math.min(maxExecutorLocal - executorLocalListSize,needNum)
        amActor ! ApplyExecutorLocal(ableNum)
        applyExecutorLocalCount += ableNum
        applyExecutorLocalCountResetLine = applyExecutorWaitTime.fromNow
        if(applyExecutorLocalFirst && needNum > ableNum) {
          log.info("local executor不足 申请 yarn executor")
          applyExecutorYarn(needNum - ableNum)
        }
      }else {
        if(applyExecutorLocalFirst) {
          log.info("local executor不足 申请 yarn executor")
          applyExecutorYarn(needNum)
        }
      }

    }
  }

  private def applyExecutorYarn(applyNum:Int): Unit = {
    log.info(s"apply executor yarn $applyNum")
    val executorYarnListSize = executorYarnList.size
    val executorLocalListSize = executorLocalList.size
    val waitRegisterYarnExecutorSize = math.max(applyExecutorYarnCount - executorYarnListSize,0);
    val needNum = applyNum - waitRegisterYarnExecutorSize
    if(
      needNum > 0
    ) {
      if(executorYarnListSize + executorLocalListSize < maxExecutor
        && applyExecutorYarnCount < maxExecutor) {
        val ableNum = math.min(maxExecutor - executorYarnListSize - executorLocalListSize,needNum)
        amActor ! ApplyExecutorYarn(ableNum)
        applyExecutorYarnCount += ableNum
        applyExecutorYarnCountResetLine = applyExecutorWaitTime.fromNow
        if(!applyExecutorLocalFirst && needNum > ableNum) {
          log.info("yarn executor不足 申请 local executor")
          applyExecutorLocal(needNum - ableNum)
        }
      }else {
        if(!applyExecutorLocalFirst) {
          log.info("yarn executor不足 申请 local executor")
          applyExecutorLocal(needNum)
        }
      }

    }
  }

  private def getIdleExecutor() = {
    val workingIds = taskId2ExecutorId.values.toVector
    log.debug(s"workingIds:$workingIds")
    for(id <- executorId2Executor.keySet if !workingIds.contains(id)) yield id
  }
  private def cancelTask(taskId:String): Unit = {
    log.info(s"cancel task $taskId")
    taskId2TaskConf.remove(taskId)
    acceptedTaskIds.remove(taskId)
    taskId2FailTimes.remove(taskId)
    taskId2ExecutorId.remove(taskId) match {//取消任务直接关闭当前Executor
      case Some(executorId) =>
        unRegisterExecutor(executorId)
      case _ => {

      }
    }
  }
}

case class RegisterExecutor(executorId:String,reply:Boolean = false,runOnType:String = Constants.EXECUTOR_RUN_ON_TYPE_LOCAL)
case class UnRegisterExecutor(executorId:String)
case class SubmitTask(taskId:String,taskConf: String)
case class CancelTask(taskId:String)
case class TaskCompleted(taskId:String,taskResult:TaskResult)
case class GetTaskStatus(taskId:String)
case class GetTaskResult(taskId:String)
case class Polling()
case class Shutdown()
case class RegisterSuccess()