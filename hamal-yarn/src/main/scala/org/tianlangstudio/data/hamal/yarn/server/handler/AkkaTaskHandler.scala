package org.tianlangstudio.data.hamal.yarn.server.handler

import java.util
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.util.Timeout
import org.tianlangstudio.data.hamal.common.{TaskCost, TaskResult}
import org.tianlangstudio.data.hamal.core.{ConfigUtil, Constants}
import org.tianlangstudio.data.hamal.core.handler.ITaskHandler
import org.tianlangstudio.data.hamal.server.thrift.{ThriftServerUtil, ThriftTaskCost, ThriftTaskResult}
import org.tianlangstudio.data.hamal.yarn.{CancelTask, SubmitTask, TaskInfo}
import org.tianlangstudio.data.hamal.yarn.util.Utils
import org.tianlangstudio.data.hamal.common.TaskCost


/**
 * Created by zhuhq on 2016/4/27.
 */
class AkkaTaskHandler(taskSchedulerActor:ActorRef) extends ITaskHandler{

  implicit val timeout = Timeout(30, TimeUnit.SECONDS)

  def submitTask(taskConfPath: String): String = {
    submitTaskWithParams(taskConfPath,null)
  }

  def getTaskStatus(taskId: String): String = {
    if(TaskInfo.taskId2ExecutorId.contains(taskId) || TaskInfo.acceptedTaskIds.contains(taskId) || TaskInfo.rerunTaskIds.contains(taskId)) {
      Constants.TASK_STATUS_RUNNING
    }else if(TaskInfo.taskId2Result.contains(taskId)) {
      Constants.TASK_STATUS_DONE
    }else {
      ""
    }
  }

  def getTaskCost(taskId: String): TaskCost = {
    null
  }
  def getThriftTaskCost(taskId: String): ThriftTaskCost = ThriftServerUtil.taskCost2Thrift(getTaskCost(taskId))
  def submitTaskWithParams(taskConfPath: String, params: util.Map[String, String]): String = {
    //val taskId = UUID.randomUUID().toString
    val taskDesc = ConfigUtil.readTaskDescIfInFileAndReplaceHolder(taskConfPath,params)
    //val taskId = DigestUtils.md5Hex(taskDesc);
    val taskId = Utils.genTaskId()
    taskSchedulerActor ! SubmitTask(taskId,taskDesc)
    taskId
  }

  def cancelTask(taskId: String): Boolean = {
    taskSchedulerActor ! CancelTask(taskId)
    true
  }

  def getTaskResult(taskId: String): TaskResult = {
    if(Constants.TASK_STATUS_DONE.equals(getTaskStatus(taskId))) {
      TaskInfo.taskId2Result.get(taskId) match {
        case Some(taskResult) =>
          taskResult
        case _ =>
          null
      }
    }else {
      null
    }
  }

  def getThriftTaskResult(taskId: String): ThriftTaskResult = ThriftServerUtil.taskResult2Thrift(getTaskResult(taskId))
}
