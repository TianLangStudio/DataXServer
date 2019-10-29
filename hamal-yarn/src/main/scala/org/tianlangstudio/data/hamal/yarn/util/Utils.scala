package org.tianlangstudio.data.hamal.yarn.util

import java.net.BindException
import java.util.Calendar

import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId, ContainerId, NodeId}
import org.tianlangstudio.data.hamal.common.exp.DataHamalException
import org.tianlangstudio.data.hamal.core.HamalConf

private[hamal] object Utils {
  /**
   * Attempt to start a service on the given port, or fail after a number of attempts.
   * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
   *
   * @param startPort The initial port to start the service on.
   * @param startService Function to start service on a given port.
   *                     This is expected to throw java.net.BindException on port collision.
   * @param conf A SparkConf used to get the maximum number of retries when binding to a port.
   * @param serviceName Name of the service.
   * @return (service: T, port: Int)
   */
  def startServiceOnPort[T](
                             startPort: Int,
                             startService: Int => (T,Int),
                             conf: HamalConf,
                             serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = 100
    for (offset <- 0 to maxRetries) {

      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        // If the new port wraps around, do not try a privilege port
        ((startPort + offset - 1024) % (65536 - 1024)) + 1024
      }
      println(s"start port is $startPort try start server on port:$tryPort try num:$offset")
      try {
        val (service,port) = startService(tryPort)
        println(s"Successfully started service$serviceString on port $port.")
        //logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage = s"${e.getMessage}: Service$serviceString failed after " +
              s"$maxRetries retries! Consider explicitly setting the appropriate port for the " +
              s"service$serviceString (for example spark.ui.port for SparkUI) to an available " +
              "port or increasing spark.port.maxRetries."
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          //logWarning(s"Service$serviceString could not bind on port $tryPort. " +
          //  s"Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new DataHamalException(s"Failed to start service$serviceString on port $startPort")
  }

  /**
   * Return whether the exception is caused by an address-port collision when binding.
   */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  def containerIdNodeId2ExecutorId(containerId:ContainerId,nodeId:NodeId): String = {
    val appAttId= containerId.getApplicationAttemptId
    val applicationId = appAttId.getApplicationId
    val appClusterTs = applicationId.getClusterTimestamp
    val appId = applicationId.getId
    val attId = appAttId.getAttemptId
    val conId = containerId.getContainerId
    val nodeHost = nodeId.getHost
    val nodePort = nodeId.getPort
    s"$appClusterTs:$appId:$attId:$conId:$nodeHost:$nodePort"
  }
  def executorId2ContainerIdNodeId(executorId:String) =  {
    executorId.split(":") match {
      case Array(appClusterTs,appId,attId,conId,nodeHost,nodePort) =>
        val appAttId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(appClusterTs.toLong,appId.toInt),attId.toInt)
        val containerId = ContainerId.newContainerId(appAttId,conId.toInt)
        val nodeId = NodeId.newInstance(nodeHost,nodePort.toInt);
        Some(containerId,nodeId)
      case _ =>
        None
    }
  }

  private val taskIdLock = new Object
  private val preId = "";
  def  genTaskId():String = {
    val now = Calendar.getInstance
    val hour = now.get(Calendar.HOUR_OF_DAY)
    val min = now.get(Calendar.MINUTE)
    val seconds = now.get(Calendar.SECOND)
    val ms = now.get(Calendar.MILLISECOND)
    val id = (hour * 3600 * 1000 + min * 60 * 1000 + seconds * 1000 + ms) + ""
    taskIdLock.synchronized(
      if(id.equals(preId)) {
        genTaskId()
      }else {
        id
      }
    )
  }
}
