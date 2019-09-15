package org.tianlangstudio.data.hamal.core.handler;

import org.tianlangstuido.data.hamal.common.TaskCost;
import org.tianlangstuido.data.hamal.common.TaskResult;

import java.util.Map;

/**
 * Created by zhuhq on 17-4-13.
 */
public interface ITaskHandler {

     String submitTask(String taskConfPath);


     String submitTaskWithParams(final String taskConfPath, final Map<String, String> params);

     String getTaskStatus(String taskId);
     TaskResult getTaskResult(String taskId);
     TaskCost getTaskCost(String taskId);
     boolean cancelTask(String taskId);

}
