package com.tianlangstudio.data.datax.server

import _root_.akka.actor.{ActorRef, ActorSystem}
import _root_.akka.http.scaladsl.server.Directives._
import _root_.akka.stream.ActorMaterializer
import com.tianlangstudio.data.datax.ext.thrift.TaskResult
import com.tianlangstudio.data.datax.server.handler.AkkaJobHandler
import com.tianlangstudio.data.datax.{Constants, DataxConf}
import spray.json.{DefaultJsonProtocol, JsBoolean, JsObject, JsString, JsValue, RootJsonFormat, RootJsonWriter}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._

/**
  *
  * Created by zhuhq on 17-4-6.
  */
class HttpServer(jobSchedulerActor: ActorRef, dataxConf: DataxConf, implicit val actorSystem: ActorSystem) {

  implicit val executionContext = actorSystem.dispatcher
  implicit val mat = ActorMaterializer()

  val jobHandler = new AkkaJobHandler(jobSchedulerActor)
  val host = dataxConf.getString(Constants.HTTP_SERVER_HOST, "127.0.0.1")
  val port = dataxConf.getString(Constants.HTTP_SERVER_HOST, "9808")
  val root = dataxConf.getString(Constants.HTTP_SERVER_ROOT, "")
  implicit import JsonProtocol._
  val route = {
    path(s"$root/job") {
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
      }
    }
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
}