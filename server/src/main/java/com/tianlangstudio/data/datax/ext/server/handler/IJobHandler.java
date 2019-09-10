package com.tianlangstudio.data.datax.ext.server.handler;

import com.tianlangstudio.data.datax.ext.multithread.Worker;
import com.tianlangstudio.data.datax.ext.thrift.TaskCost;
import com.tianlangstudio.data.datax.ext.thrift.TaskResult;
import org.apache.thrift.TException;

import java.util.Map;

/**
 * Created by zhuhq on 17-4-13.
 */
public interface IJobHandler {

     String submitJob(String jobConfPath);


     String submitJobWithParams(final String jobConfPath, final Map<String, String> params);

     String getJobStatus(String jobId);
     TaskResult getJobResult(String jobId);
     TaskCost getJobCost(String jobId);
     boolean cancelJob(String jobId);

}
