package org.tianlangstuido.data.hamal.common;

import org.tianlangstuido.data.hamal.common.exp.ExceptionUtil;

import static org.tianlangstuido.data.hamal.common.Consts.MSG_SUCCESS;

public class TaskResult {
    private boolean success = true;
    private String msg = "success";
    public TaskResult() {
        success = true;
        msg = MSG_SUCCESS;
    }
    public TaskResult(String errorMsg) {
        this.success = false;
        this.msg = errorMsg;
    }
    public TaskResult(Throwable throwable) {
        this(ExceptionUtil.trace(throwable));
    }
    public void setErrorMsg(String errorMsg) {
        this.success = false;
        this.msg = errorMsg;
    }
    public boolean isSuccess() {
        return success;
    }
    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}



