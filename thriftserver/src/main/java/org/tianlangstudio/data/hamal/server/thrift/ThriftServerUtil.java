package org.tianlangstudio.data.hamal.server.thrift;

import org.tianlangstudio.data.hamal.common.TaskCost;
import org.tianlangstudio.data.hamal.common.TaskResult;

import java.util.Date;

public class ThriftServerUtil {
    public static ThriftTaskResult taskResult2Thrift(TaskResult taskResult) {
        ThriftTaskResult thriftTaskResult = new ThriftTaskResult();
        thriftTaskResult.success = taskResult.isSuccess();
        thriftTaskResult.msg = taskResult.getMsg();
        return thriftTaskResult;
    }

    public static ThriftTaskCost taskCost2Thrift(TaskCost taskCost) {
        ThriftTaskCost thriftTaskCost = new ThriftTaskCost();
        Date beginTime = taskCost.getBeginDateTime();
        if(beginTime != null) {
            thriftTaskCost.setBeginTime(beginTime.getTime());
        }
        Date endTime = taskCost.getEndDateTime();
        if(endTime != null) {
            thriftTaskCost.setEndTime(endTime.getTime());
        }
        thriftTaskCost.setCost(taskCost.getCostMs());
        return thriftTaskCost;
    }
}
