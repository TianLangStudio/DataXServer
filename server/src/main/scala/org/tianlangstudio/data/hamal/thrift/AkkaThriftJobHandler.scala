package org.tianlangstudio.data.hamal.thrift

import java.util
import java.util.UUID

import akka.pattern.ask
import akka.actor.ActorRef
import akka.util.Timeout
import com.tianlangstudio.data.datax.{Constants, JobInfo}
import com.tianlangstudio.data.datax.ext.thrift.{TaskCost, TaskResult, ThriftServer}
import com.tianlangstudio.data.datax.util.{ConfigUtil, Utils}
import com.tianlangstudio.data.datax.{CancelJob, GetJobResult, GetJobStatus, SubmitJob}
import org.apache.commons.codec.digest.DigestUtils

import scala.concurrent.Await
import scala.concurrent.duration._
import com.tianlangstudio.data.datax.JobInfo._
import com.tianlangstudio.data.datax.server.handler.AkkaJobHandler
/**
 * Created by zhuhq on 2016/4/27.
 */
class AkkaThriftJobHandler(jobSchedulerActor:ActorRef) extends AkkaJobHandler(jobSchedulerActor = jobSchedulerActor) with ThriftServer.Iface{

  override def submitJob(jobConfPath: String): String = {
    submitJobWithParams(jobConfPath,null)
  }

  override def getJobStatus(jobId: String): String = {
    if(jobId2ExecutorId.contains(jobId) || acceptedJobIds.contains(jobId) || rerunJobIds.contains(jobId)) {
      Constants.JOB_STATUS_RUNNING
    }else if(jobId2Result.contains(jobId)) {
      Constants.JOB_STATUS_DONE
    }else {
      ""
    }
  }

  override def getJobCost(jobId: String): TaskCost = {
    null
  }

  override def submitJobWithParams(jobConfPath: String, params: util.Map[String, String]): String = {
    //val jobId = UUID.randomUUID().toString
    val jobDesc = ConfigUtil.readJobDescIfInFileAndReplaceHolder(jobConfPath,params)
    //val jobId = DigestUtils.md5Hex(jobDesc);
    val jobId = Utils.genJobId()
    jobSchedulerActor ! SubmitJob(jobId,jobDesc)
    jobId
  }

  override def cancelJob(jobId: String): Boolean = {
    jobSchedulerActor ! CancelJob(jobId)
    true
  }

  override def getJobResult(jobId: String): TaskResult = {
    if(Constants.JOB_STATUS_DONE.equals(getJobStatus(jobId))) {
      jobId2Result.get(jobId) match {
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
