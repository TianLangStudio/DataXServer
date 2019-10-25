package org.tianlangstuido.data.hamal.common;

public interface Consts {
    //task
    String TASK_STATUS_DONE = "done";//done;
    String TASK_STATUS_RUNNING = "running";//running

    //message
    String MSG_JOBDES_IS_REQUIRED = "job desc file is required and valid";
    String MSG_SUCCESS = "success";

    //monitor
    String MONITOR_PRE = "datax-server-worker-";
    String MONITOR_JOB_TIME_MONITOR = "job-time-monitor";
    String MONITOR_JOB_RNNING_COUNTER = "running-job-counter";
    String MONITOR_JOB_FAIL_COUNTER = "fail-job-counter";

    //config
    String CONFIG_REPORT = "report";
    String CONFIG_REPORT_GANGLIA_IP = "ganglia.ip";
    String CONFIG_REPORT_GANGLIA_PORT = "ganglia.port";

    //default
    int DEFAULT_CONCURRENCE = 3;
}
