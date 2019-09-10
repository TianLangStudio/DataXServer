package esun.fbi.akka

import akka.actor.{Props, ActorSystem, Actor}
import akka.actor.Actor.Receive

/**
 * Created by zhuhq on 2016/5/5.
 */
object TestAkka extends App{
  val system = ActorSystem("test")
  val actor = system.actorOf(Props(classOf[TestAkka]))
  for(i <- 0 to 10) {
    actor ! Remove()
    actor ! Add()
  }
}
class TestAkka extends Actor{
  override def receive: Receive = {
    case Remove() =>
      println("remove begin")
      Thread.sleep((1000 * math.ceil(math.random) * 10).toLong)
      println("remove end")
    case Add() =>
      println("add begin")
      Thread.sleep((1000 * math.ceil(math.random) * 10).toLong)
      println("add end")

  }
}
case class Remove()
case class Add()