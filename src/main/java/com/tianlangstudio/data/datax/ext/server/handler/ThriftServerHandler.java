package com.tianlangstudio.data.datax.ext.server.handler;


import com.tianlangstudio.data.datax.ext.thrift.TaskCost;
import com.tianlangstudio.data.datax.ext.thrift.TaskResult;
import com.tianlangstudio.data.datax.ext.thrift.ThriftServer;

import java.util.Map;

public class ThriftServerHandler implements ThriftServer.Iface, IJobHandler{
    private LocalServerHandler localServerHandler;

    @Override
    public String submitJob(String jobConfPath)  {

        return localServerHandler.submitJob(jobConfPath);

    }

    @Override
    public String submitJobWithParams(final String jobConfPath, final Map<String, String> params)  {
        return localServerHandler.submitJobWithParams(jobConfPath, params);
    }

    public String getJobStatus(String jobId) {

        return localServerHandler.getJobStatus(jobId);
    }
    public TaskResult getJobResult(String jobId) {
        return localServerHandler.getJobResult(jobId);
    }
    public TaskCost getJobCost(String jobId) {
        return localServerHandler.getJobCost(jobId);
    }
    public boolean cancelJob(String jobId) {
           return  localServerHandler.cancelJob(jobId);
    }


    public ThriftServerHandler(int concurrence) {
       localServerHandler = new LocalServerHandler(concurrence);
    }
}
