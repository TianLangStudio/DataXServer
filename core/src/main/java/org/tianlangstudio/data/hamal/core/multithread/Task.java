package org.tianlangstudio.data.hamal.core.multithread;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tianlangstudio.data.hamal.core.ConfigUtil;
import org.tianlangstudio.data.hamal.core.Engine;
import org.tianlangstuido.data.hamal.common.TaskResult;
import org.tianlangstuido.data.hamal.common.exp.ExceptionUtil;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.tianlangstuido.data.hamal.common.Consts.*;

/**
 * Created by zhuhq on 2015/11/18.
 */
public class Task implements Callable<TaskResult> {
    private static final Logger logger = LoggerFactory.getLogger(Task.class);

    private Engine engine;
    private GenericObjectPool<Engine> enginePool;
    private String taskConfStr;
    private String id;
    private Date beginTime;
    private Date endTime;
    private Future<TaskResult> future;
    private TaskResult result;
    private Worker worker;
    private boolean isDoneFlag = false;
    public Task(final String id,final GenericObjectPool<Engine> enginePool,final Worker worker,final  String taskConfStr){
        this.id = id;
        this.enginePool = enginePool;
        this.taskConfStr = taskConfStr;
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
                logger.error("task error:", e);
                result = new TaskResult(e);
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
        logger.info("task["+id+"] begin");
        TaskResult taskResult = new TaskResult();//默认为成功
        if(!ConfigUtil.validTaskDesc(taskConfStr)) {
            beginTime = new Date();
            endTime = beginTime;
            logger.error("task:" + id + " taskConfStr is required and valid:" + taskConfStr);
            return new TaskResult(MSG_JOBDES_IS_REQUIRED);
        }
        Timer.Context context = worker.getTaskTimeMonitor().time();
        Counter counter = worker.getRunningTaskCounter();
        try {
            counter.inc();
            engine = enginePool.borrowObject();
            beginTime = new Date();
            engine.start(taskConfStr, id);
            endTime = new Date();
        }catch (Exception e) {
            logger.error("error",e);
            taskResult = new TaskResult(e);
            worker.getFailTaskCounter().inc();
        }finally {
            enginePool.returnObject(engine);
            context.stop();
            counter.dec();
            logger.info("task["+id+"] end");
        }
        return taskResult;
    }

    public String getId() {
        return  id;
    }
    private static AtomicInteger idIndex = new AtomicInteger(0);

    public static String genId(Worker worker) {
        return idIndex.getAndAdd(1) + "";
        //datax job id 需要是一个整型类型的数字
        //return System.nanoTime() + "";
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
