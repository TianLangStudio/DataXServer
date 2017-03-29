package com.tianlangstudio.data.datax

import java.util.Date

import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.remote.transport.AssociationHandle.Disassociated
import com.tianlangstudio.data.datax.ext.thrift.TaskResult
import com.tianlangstudio.data.datax.yarn.{ApplyExecutorYarn, ApplyExecutorLocal, ReturnExecutor, ApplyExecutor}

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
class JobScheduler(dataxConf:DataxConf,amActor:ActorRef) extends Actor with ActorLogging{
  private val executorId2Executor = new mutable.HashMap[String,ActorRef]()//executors
  private val executorLocalList = new mutable.TreeSet[String]()
  private val executorYarnList = new mutable.TreeSet[String]()
  private val rerunJobIds  = JobInfo.rerunJobIds
  private val jobId2JobConf = JobInfo.jobId2JobConf
  private val acceptedJobIds = JobInfo.acceptedJobIds//accepted jobs
  private val jobId2Result = JobInfo.jobId2Result//complete jobs
  private val jobId2FailTimes = JobInfo.jobId2FailTimes//need retry jobs
  private val jobId2ExecutorId = JobInfo.jobId2ExecutorId//running jobs
  private val maxExecutor = dataxConf.getInt(Constants.DATAX_EXECUTOR_NUM_MAX,19)
  private val maxExecutorLocal = dataxConf.getInt(Constants.DATAX_EXECUTOR_LOCAL_NUM_MAX,5)
  private val applyExecutorLocalFirst = dataxConf.getBoolean(Constants.DATAX_EXECUTOR_APPLY_LOCAL_FIRST,true)
  private val jobInfoMaxNum = 10000
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
    case SubmitJob(jobId,jobConf) =>
      log.info(s"scheduler receive job $jobId");
      rerunJobIds.add(jobId)
      if (acceptedJobIds.contains(jobId)  || jobId2ExecutorId.contains(jobId)) {
        cancelJob(jobId)
      }
      jobId2JobConf.put(jobId,jobConf)
      acceptedJobIds.add(jobId)
      rerunJobIds.remove(jobId)
    case JobCompleted(jobId,jobResult) =>
      log.info(s"job $jobId completed ${jobResult.success}   ${jobResult.getMsg}")
      jobId2Result.put(jobId,jobResult);
      jobId2FailTimes.remove(jobId);
      jobId2ExecutorId.remove(jobId);
    case CancelJob(jobId) =>
      log.info(s"cancel job $jobId begin")
      cancelJob(jobId)
      log.info(s"cancel job $jobId end")
    case GetJobStatus(jobId) =>
      if(jobId2Result.get(jobId).isDefined) {
        Constants.JOB_STATUS_DONE
      }else if(jobId2JobConf.get(jobId).isDefined) {
        Constants.JOB_STATUS_RUNNING
      }else {
        ""
      }
    case GetJobResult(jobId) =>
      val resultOption = jobId2Result.get(jobId)
      if(resultOption.isDefined) {
        resultOption.get
      }else {
        null
      }
    case Polling() =>
      //巡检

      log.debug("polling begin")
      submitJob2Executor()
      clearUnnecessaryInfo[String](jobId2JobConf,jobInfoMaxNum)
      clearUnnecessaryInfo[Int](jobId2FailTimes,jobInfoMaxNum)
      clearUnnecessaryInfo[TaskResult](jobId2Result,jobInfoMaxNum)
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
      for((jobId,executorId) <- jobId2ExecutorId) {
        log.info(s"job $jobId  at executor $executorId")
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
    //如果是cancelJob的 jobId 对应 的Executor已经被清空
    jobId2ExecutorId.find(j2e => j2e._2 == executorId) match {
      case Some((jobId,_)) =>
        jobId2ExecutorId.remove(jobId)
        self ! SubmitJob(jobId,jobId2JobConf.get(jobId).get)
        log.info(s"resubmit running on unregister executor $executorId job $jobId ")
      case _ =>
        log.debug(s"no job on executor $executorId")
    }

    log.info(s"unregister executor executorId:$executorId end")
  }
  private def clearUnnecessaryInfo[B](info:mutable.LinkedHashMap[String,B],leaveNum:Int): Unit = {
    if(info.size > leaveNum) {
      info.drop(info.size - leaveNum)
    }
  }
  //如果有空闲executor 就执行待执行的job（accepted jobs &　need retry jobs）
  private var _firstToggle = true;
  private  def submitJob2Executor(): Unit = {
      log.debug("submitJob2Executor begin")
      val waitJobNum = acceptedJobIds.size()
      val idleExecutorIds = getIdleExecutor().toArray
      val idleExecutorNum = idleExecutorIds.size
      val currentExecutorNum = executorId2Executor.size
      log.info("executorNum:" + currentExecutorNum +
        " executorOnYarnNum:" + executorYarnList.size +
        " executorOnLocalNum:" + executorLocalList.size +
        " applyExecutorOnYarnNum:" + applyExecutorYarnCount +
        " applyExecutorOnLocalNum:" + applyExecutorLocalCount +
        " idleExecutorNum:" + idleExecutorNum +
        " runningJobNum:" + jobId2ExecutorId.size +
        " waitJobNum:" + waitJobNum

      )
      if(waitJobNum > 0) {
        log.info(s"idle executorNum:$idleExecutorNum")
        if(idleExecutorNum > 0) {
          val min = math.min(waitJobNum,idleExecutorNum)
          for(i <- 0 until min) {
            val idleExecutorId = idleExecutorIds(i)
            executorId2Executor.get(idleExecutorId) match {
              case Some(executor) =>

                val jobId = if(_firstToggle) {
                  acceptedJobIds.pollFirst()
                }else {
                  acceptedJobIds.pollLast()
                }
                _firstToggle = !_firstToggle
                jobId2ExecutorId.get(jobId) match {//如果这个任务正在执行 取消执行
                  case Some(executorId) =>
                    cancelJob(jobId)
                  case _ => {}
                }
                jobId2JobConf.get(jobId) match {
                  case Some(jobDesc) =>
                    executor ! SubmitJob(jobId,jobDesc)
                    jobId2ExecutorId.put(jobId,idleExecutorId)
                    executorId2IdleTime.remove(idleExecutorId)
                    log.info(s"job $jobId  at executor $idleExecutorId")
                  case _ =>
                    self ! JobCompleted(jobId,new TaskResult(false,"job config is lost"))
                }
              case _ =>
                self ! UnRegisterExecutor(idleExecutorId)
            }


          }
        }
        val needExecutorNum = acceptedJobIds.size()
        log.info("need executor num:" + needExecutorNum)
        if(needExecutorNum > 0) {
          if(currentExecutorNum <  maxExecutor) {
            val applyNum = math.min(maxExecutor - currentExecutorNum,needExecutorNum)
            applyExecutor(applyNum);
          }
        }

      }
      log.debug("submitJob2Executor end")
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
    val workingIds = jobId2ExecutorId.map(o => o._2).toVector
    log.debug(s"workingIds:$workingIds")
    for(id <- executorId2Executor.keySet if !workingIds.contains(id)) yield id
  }
  private def cancelJob(jobId:String): Unit = {
    log.info(s"cancel job $jobId")
    jobId2JobConf.remove(jobId)
    acceptedJobIds.remove(jobId)
    jobId2FailTimes.remove(jobId)
    jobId2ExecutorId.remove(jobId) match {//取消任务直接关闭当前Executor
      case Some(executorId) =>
        unRegisterExecutor(executorId)
      case _ => {

      }
    }
  }
}

case class RegisterExecutor(executorId:String,reply:Boolean = false,runOnType:String = Constants.EXECUTOR_RUN_ON_TYPE_LOCAL)
case class UnRegisterExecutor(executorId:String)
case class SubmitJob(jobId:String,jobConf: String)
case class CancelJob(jobId:String)
case class JobCompleted(jobId:String,jobResult:TaskResult)
case class GetJobStatus(jobId:String)
case class GetJobResult(jobId:String)
case class Polling()
case class Shutdown()
case class RegisterSuccess()