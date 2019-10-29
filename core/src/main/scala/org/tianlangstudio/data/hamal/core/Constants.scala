package org.tianlangstudio.data.hamal.core

import org.tianlangstudio.data.hamal.common.Consts

/**
 * Created by zhuhq on 2016/4/27.
 */
object Constants extends Consts{
  val NAME_HTTP_SERVER = "hamal-http-server";
  val AKKA_PROTOCOL = "akka.tcp";
  val AKKA_JOB_SCHEDULER_SYSTEM = "jobSchedulerSystem";
  val AKKA_JOB_SCHEDULER_ACTOR = "jobSchedulerActor";
  val AKKA_AM_ACTOR = "amActor";
  val AKKA_CONFIG_FILE = "akka_config.conf";
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
  val DATAX_HOME = "datax.home"
  val THRIFT_SERVER_HOST="thrift.server.ip"
  val THRIFT_SERVER_PORT="thrift.server.port"
  val THRIFT_SERVER_CONCURRENCE="thrift.server.concurrence"
  val HTTP_SERVER_HOST="http.server.ip"
  val HTTP_SERVER_PORT="http.server.port"
  val HTTP_SERVER_CONCURRENCE="http.server.concurrence"
  val HTTP_SERVER_ROOT="http.server.root"


  val TASK_STATUS_DONE="done"
  val TASK_STATUS_RUNNING="running"
  val PLACEHOLDER_EXECUTOR_ID = "VAR_EXECUTOR_ID"
  val EXECUTOR_RUN_ON_TYPE_LOCAL = "local"
  val EXECUTOR_RUN_ON_TYPE_YARN = "yarn"
  val RUN_ENV = "env"
  val RUN_ENV_DEVELOPMENT = "dev"
  val RUN_ENV_PRODUCTION = "pro"
}
