package org.tianlangstudio.data.hamal.core.multithread;


import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tianlangstudio.data.hamal.core.ConfigUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.tianlangstudio.data.hamal.core.Engine;
import org.tianlangstuido.data.hamal.common.TaskCost;
import org.tianlangstuido.data.hamal.common.TaskResult;
import org.tianlangstuido.data.hamal.common.monitor.TaskCounterMetricSet;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.tianlangstuido.data.hamal.common.Consts.*;

/**
 *
 *Created by zhuhq on 2015/11/18.
 */
public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    public    int concurrence = 10;
    private   ExecutorService executor = Executors.newFixedThreadPool(10);
    private GenericObjectPool<Engine> enginePool;
    private  final Monitor monitor = new Monitor();
    private Timer taskTimeMonitor;
    private Counter runningTaskCounter;
    private Counter failTaskCounter;
    private Map<String,Task> taskIdMap = new HashMap<String, Task>();

    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss SSS");
    public void init(int concurrence) {
        this.concurrence = concurrence;
        executor = Executors.newFixedThreadPool(concurrence);
        enginePool = new GenericObjectPool<>(new EngineFactory());
        initMonitor();
    }
    private void initMonitor() {
        monitor.start();
        taskTimeMonitor = monitor.REGISTER.timer(MONITOR_PRE + MONITOR_JOB_TIME_MONITOR);
        String runningTaskCounterName = MONITOR_PRE + MONITOR_JOB_RNNING_COUNTER;
        String failTaskCounterName = MONITOR_PRE + MONITOR_JOB_FAIL_COUNTER;

        runningTaskCounter =new Counter();
        failTaskCounter = new Counter();
        TaskCounterMetricSet taskCounterMetricSet = new TaskCounterMetricSet();
        taskCounterMetricSet.put(runningTaskCounterName,runningTaskCounter);
        taskCounterMetricSet.put(failTaskCounterName,failTaskCounter);
        monitor.REGISTER.registerAll(taskCounterMetricSet);
        monitor.REGISTER.registerAll(new MemoryUsageGaugeSet());
        monitor.REGISTER.registerAll(new GarbageCollectorMetricSet());
        monitor.REGISTER.registerAll(new ThreadStatesGaugeSet());
    }
    /**
     * 提交任务 返回值为 taskId
     *
     * ***/
    public String submitTask(String taskDesc,Map<String,String> params) {
        logger.info("submit task begin");
        if(!ConfigUtil.validTaskDesc(taskDesc)) {
            logger.warn(MSG_JOBDES_IS_REQUIRED);
            throw new IllegalArgumentException(MSG_JOBDES_IS_REQUIRED);
        }
        String taskId = Task.genId(this);
        logger.info("replace config content placeholder begin");
        taskDesc = ConfigUtil.readTaskDescIfInFileAndReplaceHolder(taskDesc,params);
        logger.info("replace config content placeholder end");
        Task task = new Task(taskId,enginePool,this,taskDesc);
        taskIdMap.put(taskId,task);
        task.start(executor);

        return taskId;
    }

    /**
     * 提交任务 返回值为 taskId
     *
     * ***/
    public String submitTask(String taskDesc) {
        return  submitTask(taskDesc,null);
    }
    /**
     * 获取task状态
     * **/
    public String getTaskStatus(String taskId) {
        Task task = taskIdMap.get(taskId);
        return task == null ? "not found task:" + taskId : task.getStatus();
    }
    /**
     * 获取task结果
     * **/
     public TaskResult getTaskResult(String taskId) {
         Task task = taskIdMap.get(taskId);
         return task == null ? new TaskResult("not found task:" + taskId) : task.getResult();
     }

     public boolean cancelTask(String taskId) {
         Task task = taskIdMap.get(taskId);
         return  task == null || task.cancel();
     }
    public TaskCost getTaskCost(String taskId) {

        Task task = taskIdMap.get(taskId);
        if(task == null) {
            return new TaskCost();
        }
        Date beginTime = task.getBeginTime();
        TaskCost taskCost = new TaskCost(beginTime);
        if(task.isDone()) {
            Date endTime = task.getEndTime();
            taskCost.setEndTime(endTime);
        }
        return  taskCost;
    }

    public Timer getTaskTimeMonitor() {
        return  taskTimeMonitor;
    }
    public Counter getRunningTaskCounter() {
        return runningTaskCounter;
    }
    public Counter getFailTaskCounter() {
        return failTaskCounter;
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
    private static final Logger logger = LoggerFactory.getLogger(Monitor.class);
    final MetricRegistry REGISTER = new MetricRegistry();
    //private final GarbageCollectorMXBean garbageCollectorMXBean = new GarbageCollectorMXBean();

    public boolean start(){
        Config config  = ConfigFactory.load();
        Config reportConfig = config.getConfig(CONFIG_REPORT);
        boolean startSuccess;
        try {
            String ip = reportConfig.getString(CONFIG_REPORT_GANGLIA_IP);
            int port = reportConfig.getInt(CONFIG_REPORT_GANGLIA_PORT);

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
