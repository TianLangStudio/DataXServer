package com.tianlangstudio.data.datax

/**
 * Created by zhuhq on 2016/4/27.
 */
object Constants {
  val AKKA_PROTOCOL = "akka.tcp";
  val AKKA_JOB_SCHEDULER_SYSTEM = "jobSchedulerSystem";
  val AKKA_JOB_SCHEDULER_ACTOR = "jobSchedulerActor";
  val AKKA_AM_ACTOR = "amActor";
  val AKKA_REMOTE_NETTY_PORT = "akka.remote.netty.port";
  val DATAX_MASTER_HOST = "datax.master.host";
  val DATAX_MASTER_PORT = "datax.master.port";
  val DATAX_MASTER_HOST_PORT = "datax.master.hostPort"
  val DATAX_MASTER_CONF_NAME = "master.conf"
  val DATAX_EXECUTOR_CONF_NAME = "executor.conf"
  val DATAX_EXECUTOR_NUM_MAX = "datax.executor.num.max"
  val DATAX_EXECUTOR_LOCAL_NUM_MAX = "datax.executor.local.num.max"
  val DATAX_EXECUTOR_FILE="datax.executor.file"
  val DATAX_EXECUTOR_CP="datax.executor.classpath"
  val DATAX_EXECUTOR_CMD="datax.executor.cmd"
  val DATAX_EXECUTOR_LOCAL_CMD="datax.executor.local.cmd"
  val DATAX_EXECUTOR_IDLE_TIME="datax.executor.idle.time"
  val DATAX_EXECUTOR_REGISTER_INTERVAL_TIME="datax.executor.register.interval.time"
  val DATAX_EXECUTOR_ARCHIVE_FILE_NAME = "archive"
  val DATAX_EXECUTOR_CORES="datax.executor.cores"
  val DATAX_EXECUTOR_APPLY_LOCAL_FIRST="datax.executor.apply.local.first"
  val THRIFT_SERVER_HOST="server.ip"
  val THRIFT_SERVER_PORT="server.port"
  val THRIFT_SERVER_CONCURRENCE="server.concurrence"
  val JOB_STATUS_DONE="done"
  val JOB_STATUS_RUNNING="running"
  val PLACEHOLDER_EXECUTOR_ID = "VAR_EXECUTOR_ID"
  val EXECUTOR_RUN_ON_TYPE_LOCAL = "local"
  val EXECUTOR_RUN_ON_TYPE_YARN = "yarn"
}
