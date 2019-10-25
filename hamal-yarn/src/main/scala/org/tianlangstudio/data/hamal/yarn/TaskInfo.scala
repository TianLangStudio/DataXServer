package org.tianlangstudio.data.hamal.yarn

import org.tianlangstuido.data.hamal.common.TaskResult

import scala.collection.mutable

/**
 * Created by zhuhq on 2016/5/4.
 */
object TaskInfo {
  val taskId2TaskConf = new mutable.LinkedHashMap[String,String]()//
  val acceptedTaskIds = new java.util.LinkedList[String]()//accepted tasks
  val rerunTaskIds = new mutable.TreeSet[String]() //正在重新提交的task
  val taskId2Result = new mutable.LinkedHashMap[String,TaskResult]()//complete tasks
  val taskId2FailTimes = new mutable.LinkedHashMap[String,Int]()//need retry tasks
  val taskId2ExecutorId = new mutable.HashMap[String,String]()//running tasks
}
