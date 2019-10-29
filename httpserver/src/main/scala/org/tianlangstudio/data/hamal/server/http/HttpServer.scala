package org.tianlangstudio.data.hamal.server.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import org.tianlangstudio.data.hamal.common.{Logging, TaskCost, TaskResult}
import org.tianlangstudio.data.hamal.core.handler.ITaskHandler
import org.tianlangstudio.data.hamal.common.TaskCost
import org.tianlangstudio.data.hamal.core.HamalConf
import spray.json.{JsBoolean, JsObject, JsString, JsValue, RootJsonWriter}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.tianlangstudio.data.hamal.core.{Constants, HamalConf}

import scala.jdk.CollectionConverters._
/**
  *
  * Created by zhuhq on 17-4-6.
  */
class HttpServer(taskHandler: ITaskHandler, hamalConf: HamalConf)(implicit val actorSystem: ActorSystem) extends Logging {

  implicit val executionContext = actorSystem.dispatcher
  implicit val mat = ActorMaterializer()
  implicit object TaskResultJsonFormat extends RootJsonWriter[TaskResult] {
    override def write(taskResult: TaskResult): JsValue = {
      JsObject(
        "success" -> JsBoolean(taskResult.isSuccess),
        "msg" -> JsString(taskResult.getMsg)
      )
    }
  }
  implicit object TaskCostJsonFormat extends RootJsonWriter[TaskCost] {
    override def write(taskCost: TaskCost): JsValue = {
      JsObject(
        "beginTime" -> JsString(taskCost.getBeginTime),
        "endTime" -> JsString(taskCost.getEndTime),
        "cost" -> JsString(taskCost.getCost)
      )
    }
  }

  val host = hamalConf.getString(Constants.HTTP_SERVER_HOST, "127.0.0.1")
  val port = hamalConf.getInt(Constants.HTTP_SERVER_PORT, 9808)

  val pathRoot = hamalConf.getString(Constants.HTTP_SERVER_ROOT, "dataxserver")

  val route: Route =
    pathPrefix(s"$pathRoot" / "task") {
      concat(
          get {
            path("status" / Segment) { taskId =>
              val taskStatus = taskHandler.getTaskStatus(taskId)
              complete(taskStatus)
            }
          },
          get {
            path("cost" / Segment) { taskId =>
              val taskCost = taskHandler.getTaskCost(taskId)
              complete(taskCost)
            }
          },
          post {
            entity(as[String]) { taskDesc =>
              parameterMap {parameterMap =>
                logger.info(s"taskDesc: $taskDesc")
                complete(taskHandler.submitTaskWithParams(taskDesc, parameterMap.asJava))
              }
            }
          },
          get {
            pathPrefix(Segment) { taskId =>
              val taskResult  = taskHandler.getTaskResult(taskId)
              if(taskResult == null) {
                complete(new TaskResult(s"Not Found task[$taskId]"))
              } else {
                complete(taskResult)
              }

            }
          }
      )
    } ~ path(s"$pathRoot") {
      get {
        complete(
          s"""Usage: [get $pathRoot/task/#taskId -> TaskResult]
             |curl ip:port/$pathRoot/task/#taskId
             |[get $pathRoot/task/status/#taskId -> Task Status:
             |${Constants.TASK_STATUS_DONE} or ${Constants.TASK_STATUS_RUNNING}]
             |curl ip:port/$pathRoot/task/status/#taskId
             |[get $pathRoot/task/cost/#taskId -> TaskCost]
             |curl ip:port/$pathRoot/task/cost/#taskId
             |[post $pathRoot/task TaskDesc -> taskId of submit task]
             |curl  -XPOST -d '@job.json' ip:port/$pathRoot/task
           """.stripMargin)
      }
    }

  logger.info(s"http server host:$host,port:$port, rootPath:$pathRoot")
  val bindingFuture = Http().bindAndHandle(route, host, port)
  logger.info(s"http server is running on $host:$port/$pathRoot")

  def stop(): Unit = {
    bindingFuture.flatMap(_.unbind()).onComplete(_ => actorSystem.terminate())
  }



}
