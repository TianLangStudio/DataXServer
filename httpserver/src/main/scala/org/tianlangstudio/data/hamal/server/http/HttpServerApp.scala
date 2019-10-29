package org.tianlangstudio.data.hamal.server.http

import akka.actor.ActorSystem
import org.tianlangstudio.data.hamal.common.Logging
import org.tianlangstudio.data.hamal.core.{Constants, HamalConf}
import org.tianlangstudio.data.hamal.core.handler.{ITaskHandler, LocalServerHandler}
import org.tianlangstudio.data.hamal.core.HamalConf

/**
  *
  * Created by zhuhq on 17-4-13.
  */
object HttpServerApp extends App with Logging{

  def start(taskHandler: ITaskHandler, hamalConf: HamalConf)(implicit  system: ActorSystem): Unit = {
    val httpServer = new HttpServer(taskHandler, hamalConf)
    sys.addShutdownHook {
      httpServer.stop()
    }
  }
  val concurrence = if(args.length > 1) {
    args(0).toInt
  } else  {
    3
  }
  //使用LocalServerHandler多线程方式执行任务
  implicit val system = ActorSystem(Constants.NAME_HTTP_SERVER)
  start(new LocalServerHandler(concurrence), new HamalConf())
  while(true){
    Thread.sleep(10000)
  }
}
