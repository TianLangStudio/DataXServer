package esun.fbi.datax.ext.server;


import java.util.Map;

import esun.fbi.datax.ext.multithread.Worker;
import esun.fbi.datax.ext.thrift.TaskCost;
import esun.fbi.datax.ext.thrift.TaskResult;
import esun.fbi.datax.ext.thrift.ThriftServer;
import org.apache.thrift.TException;

public class ThriftServerHandler implements ThriftServer.Iface {
    private Worker worker;
    private String server;
    private int port;
    @Override
    public String submitJob(String jobConfPath) throws TException {
        return worker.submitJob(jobConfPath);
    }

    @Override
    public String submitJobWithParams(final String jobConfPath, final Map<String, String> params) throws TException {
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


    public ThriftServerHandler(int concurrence,String server,int port) {
        worker = new Worker();
        worker.init(concurrence,server,port);
        this.server = server;
        this.port = port;
    }
}
