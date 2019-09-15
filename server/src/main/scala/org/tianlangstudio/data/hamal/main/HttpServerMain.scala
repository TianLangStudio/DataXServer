package org.tianlangstudio.data.hamal.main

import akka.actor.ActorSystem
import com.tianlangstudio.data.datax.DataxConf
import com.tianlangstudio.data.datax.ext.server.handler.LocalServerHandler
import org.tianlangstudio.data.hamal.server.HttpServer

/**
  *
  * Created by zhuhq on 17-4-13.
  */
object HttpServerMain extends App{
  implicit val system = ActorSystem("datax-http-server")
  val concurrence = if(args.length > 1) {
    args(0).toInt
  } else  {
    3
  }
  val httpServer = new HttpServer(new LocalServerHandler(concurrence), new DataxConf())
  sys.addShutdownHook {
    httpServer.stop()
  }

  while(true){
    Thread.sleep(10000)
  }
}
