package com.tianlangstudio.data.datax

import java.util.concurrent.ConcurrentLinkedQueue

import com.tianlangstudio.data.datax.ext.thrift.TaskResult

import scala.collection.mutable

/**
 * Created by zhuhq on 2016/5/4.
 */
object JobInfo {
  val jobId2JobConf = new mutable.LinkedHashMap[String,String]()//
  val acceptedJobIds = new java.util.LinkedList[String]()//accepted jobs
  val rerunJobIds = new mutable.TreeSet[String]() //正在重新提交的job
  val jobId2Result = new mutable.LinkedHashMap[String,TaskResult]()//complete jobs
  val jobId2FailTimes = new mutable.LinkedHashMap[String,Int]()//need retry jobs
  val jobId2ExecutorId = new mutable.HashMap[String,String]()//running jobs
}
