package com.tianlangstudio.data.datax.server

import java.lang.String

import _root_.akka.http.scaladsl.Http
import _root_.akka.http.scaladsl.model.StatusCode
import _root_.akka.http.scaladsl.server.Directives._
import _root_.akka.actor.ActorRef
import _root_.akka.actor.ActorSystem
import _root_.akka.stream.ActorMaterializer
import com.tianlangstudio.data.datax.server.akka.AkkaJobHandler
import com.tianlangstudio.data.datax.{Constants, DataxConf}

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

  val route = {
    path(s"$root/job") {
      get {
        pathPrefix(LongNumber) { jobId =>
          complete(s"$jobId")
        }
      }
    }
  }
}
