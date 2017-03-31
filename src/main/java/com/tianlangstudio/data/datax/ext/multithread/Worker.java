package com.tianlangstudio.data.datax.ext.multithread;


import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;


import com.tianlangstudio.data.datax.exception.DataXException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.tianlangstudio.data.datax.ext.monitor.JobCounterMetricSet;
import com.tianlangstudio.data.datax.ext.thrift.TaskCost;
import com.tianlangstudio.data.datax.ext.thrift.TaskResult;
import com.tianlangstudio.data.datax.util.ConfigUtil;
import info.ganglia.gmetric4j.gmetric.GMetric;

import org.apache.commons.io.FileUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.tianlangstudio.data.datax.core.Engine;

/**
 *
 *Created by zhuhq on 2015/11/18.
 */
public class Worker {
    private static final Logger logger = Logger.getLogger(Worker.class);
    public    int concurrence = 10;
    private   ExecutorService executor = Executors.newFixedThreadPool(10);
    private GenericObjectPool<Engine> enginePool;
    private String server;
    private int serverPort;
    private  final Monitor monitor = new Monitor();
    private Timer jobTimeMonitor;
    private Counter runningJobCounter;
    private Counter failJobCounter;
    private Map<String,Task> taskIdMap = new HashMap<String, Task>();
    private final static String MONITOR_PRE = "datax-server-worker-";
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss SSS");
    public void init(int concurrence,String server,int serverPort) {
        this.concurrence = concurrence;
        executor = Executors.newFixedThreadPool(concurrence);
        enginePool = new GenericObjectPool(new EngineFactory());
        this.server = server;
        this.serverPort = serverPort;
        initMonitor();

    }
    private void initMonitor() {
        monitor.start();
        jobTimeMonitor = monitor.REGISTER.timer(MONITOR_PRE + "job-time-monitor");

        String runningJobCounterName = MONITOR_PRE + "running-job-counter";
        String failJobCounterName = MONITOR_PRE + "fail-job-counter";

        runningJobCounter =new Counter();
        failJobCounter = new Counter();
        JobCounterMetricSet jobCounterMetricSet = new JobCounterMetricSet();
        jobCounterMetricSet.put(runningJobCounterName,runningJobCounter);
        jobCounterMetricSet.put(failJobCounterName,failJobCounter);
        monitor.REGISTER.registerAll(jobCounterMetricSet);
        monitor.REGISTER.registerAll(new MemoryUsageGaugeSet());
        monitor.REGISTER.registerAll(new GarbageCollectorMetricSet());
        monitor.REGISTER.registerAll(new ThreadStatesGaugeSet());
    }
    /**
     * 提交任务 返回值为 taskId
     *
     * ***/
    public String submitJob(String jobDesc,Map<String,String> params) {
        logger.info("submit job begin");
        if(jobDesc == null) {
            throw new IllegalArgumentException("job desc is required");
        }
        String taskId = Task.genId(this);
        logger.info("replace config content placeholder begin");
        jobDesc = ConfigUtil.readJobDescIfInFileAndReplaceHolder(jobDesc,params);
        logger.info("replace config content placeholder end");
        Task task = new Task(taskId,enginePool,this,jobDesc);
        taskIdMap.put(taskId,task);
        task.start(executor);

        return taskId;
    }

    /**
     * 提交任务 返回值为 taskId
     *
     * ***/
    public String submitJob(String jobDesc) {
        return  submitJob(jobDesc,null);
    }
    /**
     * 获取task状态
     * **/
    public String getTaskStatus(String taskId) {
        Task task = taskIdMap.get(taskId);
        return task == null ? "" : task.getStatus();
    }
    /**
     * 获取task结果
     * **/
     public TaskResult getTaskResult(String taskId) {
         Task task = taskIdMap.get(taskId);
         return task == null ? null : task.getResult();
     }

     public boolean cancelTask(String taskId) {
         Task task = taskIdMap.get(taskId);
         return  task == null || task.cancel();
     }
    public TaskCost getTaskCost(String taskId) {
        TaskCost taskCost = new TaskCost();
        Task task = taskIdMap.get(taskId);
        if(task == null) {
            return taskCost;
        }
        Date beginTime = task.getBeginTime();
        taskCost.setBeginTime(dateFormat.format(beginTime));
        if(task.isDone()) {
            Date endTime = task.getEndTime();
            taskCost.setEntTime(dateFormat.format(endTime));
            String cost = (endTime.getTime() - beginTime.getTime())/1000 + "s";
            taskCost.setCost(cost);
        }
        return  taskCost;
    }
    public String getServer() {
        return server;
    }

    public int getServerPort() {
        return serverPort;
    }
    public Timer getJobTimeMonitor() {
        return  jobTimeMonitor;
    }
    public Counter getRunningJobCounter() {
        return runningJobCounter;
    }
    public Counter getFailJobCounter() {
        return failJobCounter;
    }
}
class  EngineFactory extends BasePooledObjectFactory<Engine> {
    @Override
    public Engine create() throws Exception {
        return new Engine();
    }

    @Override
    public PooledObject<Engine> wrap(Engine engine) {
        return  new DefaultPooledObject<Engine>(engine);
    }
}
class Monitor {
    private static final Logger logger = Logger.getLogger(Monitor.class);
    final MetricRegistry REGISTER = new MetricRegistry();
    //private final GarbageCollectorMXBean garbageCollectorMXBean = new GarbageCollectorMXBean();

    public boolean start(){
        Config config  = ConfigFactory.load();
        Config reportConfig = config.getConfig("report");
        boolean startSuccess;
        try {
            String ip = reportConfig.getString("ganglia.ip");
            int port = reportConfig.getInt("ganglia.port");

            final GMetric ganglia = new GMetric(ip, port, GMetric.UDPAddressingMode.UNICAST, 1);
            final GangliaReporter reporter = GangliaReporter.forRegistry(REGISTER)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(ganglia);


            reporter.start(10, TimeUnit.SECONDS);
            startSuccess = true;
        }catch (Exception ex) {
            startSuccess = false;
            logger.warn("monitor start error",ex);
        }
        return startSuccess;

    }

}
