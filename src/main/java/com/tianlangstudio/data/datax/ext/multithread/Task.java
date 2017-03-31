package com.tianlangstudio.data.datax.ext.multithread;

import com.alibaba.datax.core.util.ExceptionTracker;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;


import com.tianlangstudio.data.datax.core.Engine;
import com.tianlangstudio.data.datax.ext.thrift.TaskResult;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.*;

/**
 * Created by zhuhq on 2015/11/18.
 */
public class Task implements Callable<TaskResult> {
    private static final Logger logger = Logger.getLogger(Task.class);
    public static String TASK_STATUS_DONE = "done";//done;
    public static String TASK_STATUS_RUNNING = "running";//running
    private Engine engine;
    private GenericObjectPool<Engine> enginePool;
    private String jobConfStr;
    private String id;
    private Date beginTime;
    private Date endTime;
    private Future<TaskResult> future;
    private TaskResult result;
    private Worker worker;
    private boolean isDoneFlag = false;
    public Task(final String id,final GenericObjectPool<Engine> enginePool,final Worker worker,final  String jobConfStr){
        this.id = id;
        this.enginePool = enginePool;
        this.jobConfStr = jobConfStr;
        this.worker = worker;
    }
    public Future<TaskResult> start(ExecutorService executor) {
        this.future = executor.submit(this);
        return this.future;
    }
    public boolean isDone() {
        if(isDoneFlag) {
            return  true;
        }
        if(this.future == null) {
            return  false;
        }
        isDoneFlag = this.future.isDone();
        if(isDoneFlag) {
            try {
                result = this.future.get();

            } catch (Exception e) {
                result = new TaskResult();
                result.success = false;
                result.msg = ExceptionTracker.trace(e);
            }
            this.future = null;//方便垃圾回收
        }
        return isDoneFlag;
    }
    public String getStatus() {

        if(isDone()) {

            return TASK_STATUS_DONE;
        }
        return TASK_STATUS_RUNNING;
    }
    public TaskResult getResult() {
        if(isDone()) {
            return this.result;
        }
        return  null;
    }
    public boolean cancel() {
        if(this.future != null) {
            return this.future.cancel(true);
        }
        return  true;
    }
    @Override
    public TaskResult call() {
        logger.info("job["+id+"] begin");
        TaskResult taskResult = new TaskResult();//默认为成功
        if(StringUtils.isBlank(jobConfStr)) {
            taskResult.success = false;
            taskResult.msg = "job desc file is required";
            beginTime = new Date();
            endTime = beginTime;
            return taskResult;
        }
        Timer.Context context = worker.getJobTimeMonitor().time();
        Counter counter = worker.getRunningJobCounter();
        try {
            counter.inc();
            engine = enginePool.borrowObject();
            //根据jobConfPath中是否含有"<"判断 是文件路径还是配置文件字符串
            //JobConf jobConf = jobConfPath.contains("<")?ParseXMLUtil.xmlStr2JobConf(jobConfPath):ParseXMLUtil.loadJobConfig(jobConfPath);


            beginTime = new Date();
            engine.start(jobConfStr, id);
            endTime = new Date();
        }catch (Exception e) {
            logger.error("error",e);
            taskResult.success = false;
            taskResult.msg = ExceptionTracker.trace(e);
            worker.getFailJobCounter().inc();
        }finally {
            enginePool.returnObject(engine);
            context.stop();
            counter.dec();
            logger.info("job["+id+"] end");
        }
        return taskResult;
    }

    public String getId() {
        return  id;
    }
    private static int idSeq = 0;
    public static String genId(Worker worker) {
        Calendar now = Calendar.getInstance();
        int hour = now.get(Calendar.HOUR_OF_DAY);
        int min = now.get(Calendar.MINUTE);
        int seconds  = now.get(Calendar.SECOND);
        int ms = now.get(Calendar.MILLISECOND);
        return (hour * 3600 * 1000 + min * 60 * 1000 + seconds * 1000 + ms) + "";
        //return worker.getServer() +":"+ worker.getServerPort()+"/" + Long.toString(System.nanoTime(), 36);
    }

    public Date getBeginTime() {
        return beginTime;
    }

    public Date getEndTime() {
        return endTime;
    }
    public Worker getWorker() {
        return  worker;
    }
}
