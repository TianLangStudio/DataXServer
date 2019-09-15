package org.tianlangstudio.data.hamal.server.thrift;
import org.tianlangstudio.data.hamal.core.handler.ITaskHandler;
import org.tianlangstudio.data.hamal.core.handler.LocalServerHandler;
import org.tianlangstuido.data.hamal.common.Consts;
import org.tianlangstuido.data.hamal.common.TaskCost;
import org.tianlangstuido.data.hamal.common.TaskResult;

import java.util.Map;

public class ThriftServerHandler implements ThriftServer.Iface, ITaskHandler {
    private ITaskHandler taskHandler;
    @Override
    public String submitTask(String taskConfPath)  {

        return taskHandler.submitTask(taskConfPath);

    }

    @Override
    public String submitTaskWithParams(final String taskConfPath, final Map<String, String> params)  {
        return taskHandler.submitTaskWithParams(taskConfPath, params);
    }

    public String getTaskStatus(String taskId) {

        return taskHandler.getTaskStatus(taskId);
    }

    @Override
    public TaskResult getTaskResult(String taskId) {
        return taskHandler.getTaskResult(taskId);
    }

    @Override
    public TaskCost getTaskCost(String taskId) {
        return taskHandler.getTaskCost(taskId);
    }

    public ThriftTaskResult getThriftTaskResult(String taskId) {
        return ThriftServerUtil.taskResult2Thrift(
                taskHandler.getTaskResult(taskId)
        );
    }
    public ThriftTaskCost getThriftTaskCost(String taskId) {
        return ThriftServerUtil.taskCost2Thrift(
                taskHandler.getTaskCost(taskId)
        );
    }
    public boolean cancelTask(String taskId) {
           return  taskHandler.cancelTask(taskId);
    }

    public ThriftServerHandler(int concurrence) {
        this.taskHandler = new LocalServerHandler(concurrence);
    }

    public ThriftServerHandler() {
        this(Consts.DEFAULT_CONCURRENCE);
    }
    public ThriftServerHandler(ITaskHandler taskHandler) {
        this.taskHandler = taskHandler;
    }
}
