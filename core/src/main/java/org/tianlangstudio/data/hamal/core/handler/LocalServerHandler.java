package org.tianlangstudio.data.hamal.core.handler;


import org.tianlangstudio.data.hamal.core.runon.multithread.Worker;
import org.tianlangstuido.data.hamal.common.TaskCost;
import org.tianlangstuido.data.hamal.common.TaskResult;

import java.util.Map;

public class LocalServerHandler implements ITaskHandler {
    private Worker worker;
    @Override
    public String submitTask(String taskConfPath){
        return worker.submitTask(taskConfPath);
    }

    @Override
    public String submitTaskWithParams(final String taskConfPath, final Map<String, String> params){
        return worker.submitTask(taskConfPath,params);
    }

    public String getTaskStatus(String taskId) {
        return worker.getTaskStatus(taskId);
    }
    public TaskResult getTaskResult(String taskId) {
        return worker.getTaskResult(taskId);
    }
    public TaskCost getTaskCost(String taskId) {
        return worker.getTaskCost(taskId);
    }
    public boolean cancelTask(String taskId) {
           return  worker.cancelTask(taskId);
    }


    public LocalServerHandler(int concurrence) {
        worker = new Worker();
        worker.init(concurrence);
    }
}
