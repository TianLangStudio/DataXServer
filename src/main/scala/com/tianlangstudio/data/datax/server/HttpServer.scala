package com.tianlangstudio.data.datax.server

import _root_.akka.actor.{ActorRef, ActorSystem}
import _root_.akka.http.scaladsl.server.Directives._
import _root_.akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import com.tianlangstudio.data.datax.ext.thrift.{TaskCost, TaskResult}
import com.tianlangstudio.data.datax.server.handler.AkkaJobHandler
import com.tianlangstudio.data.datax.{Constants, DataxConf}
import spray.json.{DefaultJsonProtocol, JsBoolean, JsObject, JsString, JsValue, RootJsonFormat, RootJsonWriter}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.tianlangstudio.data.datax.common.Logging

import collection.JavaConversions._
import spray.json.DefaultJsonProtocol._
import JsonProtocol._
/**
  *
  * Created by zhuhq on 17-4-6.
  */
class HttpServer(jobSchedulerActor: ActorRef, dataxConf: DataxConf, implicit val actorSystem: ActorSystem) extends Logging {

  implicit val executionContext = actorSystem.dispatcher
  implicit val mat = ActorMaterializer()

  val jobHandler = new AkkaJobHandler(jobSchedulerActor)
  val host = dataxConf.getString(Constants.HTTP_SERVER_HOST, "127.0.0.1")
  val port = dataxConf.getInt(Constants.HTTP_SERVER_HOST, 9808)

  val pathRoot = dataxConf.getString(Constants.HTTP_SERVER_ROOT, "")


  val route = {
    path(s"$pathRoot/job") {
      get {
        pathPrefix(Segment) { jobId =>
          val taskResult = jobHandler.getJobResult(jobId)
          complete(taskResult)
        }
      } ~
      get {
        pathPrefix("status" / Segment) { jobId =>
          val taskStatus = jobHandler.getJobStatus(jobId)
          complete(taskStatus)
        }
      } ~
      get {
        pathPrefix("cost" / Segment) { jobId =>
          val taskCost = jobHandler.getJobCost(jobId)
          complete(taskCost)
        }
      } ~
      post {
        entity(as[String]) { jobDesc =>
          parameterMap {parameterMap =>
            complete(jobHandler.submitJobWithParams(jobDesc, parameterMap))
          }
        }
      }
    }
  }
  logger.info(s"http server host:$host,port:$port")
  val bindingFuture = Http().bindAndHandle(route, host, port)
  logger.info(s"http server is running on $host:$port")

  def stop(): Unit = {
    bindingFuture.flatMap(_.unbind())
  }



}

object JsonProtocol extends DefaultJsonProtocol {
  implicit object TaskResultJsonFormat extends RootJsonWriter[TaskResult] {
    override def write(taskResult: TaskResult): JsValue = {
      JsObject(
        "success" -> JsBoolean(taskResult.success),
        "msg" -> JsString(taskResult.msg)
      )
    }
  }
  implicit object TaskCostJsonFormat extends RootJsonWriter[TaskCost] {
    override def write(taskCost: TaskCost): JsValue = {
      JsObject(
        "beginTime" -> JsString(taskCost.beginTime),
        "endTime" -> JsString(taskCost.endTime),
        "cost" -> JsString(taskCost.cost)
      )
    }
  }
}