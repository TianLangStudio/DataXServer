package org.tianlangstudio.data.hamal.yarn.thrift

import java.util
import java.util.UUID

import akka.pattern.ask
import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.commons.codec.digest.DigestUtils
import org.tianlangstudio.data.hamal.common.{TaskCost, TaskResult}

import scala.concurrent.Await
import scala.concurrent.duration._
import org.tianlangstudio.data.hamal.core.{ConfigUtil, Constants}
import org.tianlangstudio.data.hamal.server.thrift.ThriftServer
import org.tianlangstudio.data.hamal.yarn.{CancelTask, SubmitTask, TaskInfo}
import org.tianlangstudio.data.hamal.yarn.server.handler.AkkaTaskHandler
import org.tianlangstudio.data.hamal.yarn.util.Utils
import org.tianlangstudio.data.hamal.common.TaskCost
/**
 * Created by zhuhq on 2016/4/27.
 */
class AkkaThriftTaskHandler(taskSchedulerActor:ActorRef) extends AkkaTaskHandler(taskSchedulerActor = taskSchedulerActor) with ThriftServer.Iface{

  override def submitTask(taskConfPath: String): String = {
    submitTaskWithParams(taskConfPath,null)
  }

  override def getTaskStatus(taskId: String): String = {
    if(TaskInfo.taskId2ExecutorId.contains(taskId) || TaskInfo.acceptedTaskIds.contains(taskId) || TaskInfo.rerunTaskIds.contains(taskId)) {
      Constants.TASK_STATUS_RUNNING
    }else if(TaskInfo.taskId2Result.contains(taskId)) {
      Constants.TASK_STATUS_DONE
    }else {
      ""
    }
  }

  override def getTaskCost(taskId: String): TaskCost = {
    null
  }

  override def submitTaskWithParams(taskConfPath: String, params: util.Map[String, String]): String = {
    //val taskId = UUID.randomUUID().toString
    val taskDesc = ConfigUtil.readTaskDescIfInFileAndReplaceHolder(taskConfPath,params)
    //val taskId = DigestUtils.md5Hex(taskDesc);
    val taskId = Utils.genTaskId()
    taskSchedulerActor ! SubmitTask(taskId,taskDesc)
    taskId
  }

  override def cancelTask(taskId: String): Boolean = {
    taskSchedulerActor ! CancelTask(taskId)
    true
  }

  override def getTaskResult(taskId: String): TaskResult = {
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
}
