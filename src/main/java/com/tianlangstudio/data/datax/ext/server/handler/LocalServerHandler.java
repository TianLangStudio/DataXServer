package com.tianlangstudio.data.datax.ext.server.handler;


import com.tianlangstudio.data.datax.ext.multithread.Worker;
import com.tianlangstudio.data.datax.ext.thrift.TaskCost;
import com.tianlangstudio.data.datax.ext.thrift.TaskResult;

import java.util.Map;

public class LocalServerHandler implements IJobHandler {
    private Worker worker;
    @Override
    public String submitJob(String jobConfPath){
        return worker.submitJob(jobConfPath);
    }

    @Override
    public String submitJobWithParams(final String jobConfPath, final Map<String, String> params){
        return worker.submitJob(jobConfPath,params);
    }

    public String getJobStatus(String jobId) {
        return worker.getTaskStatus(jobId);
    }
    public TaskResult getJobResult(String jobId) {
        return worker.getTaskResult(jobId);
    }
    public TaskCost getJobCost(String jobId) {
        return worker.getTaskCost(jobId);
    }
    public boolean cancelJob(String jobId) {
           return  worker.cancelTask(jobId);
    }


    public LocalServerHandler(int concurrence) {
        worker = new Worker();
        worker.init(concurrence);
    }
}
