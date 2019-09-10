package com.tianlangstudio.data.datax.exception;

/**
 * Created by zhuhq on 2016/4/27.
 */
public class DataXException extends RuntimeException {
    private DataXException() {}
    public DataXException(Throwable ex) {
        super(ex);
    }
    public DataXException(String msg, Throwable ex) {
        super(msg,ex);
    }

    public DataXException(String msg) {
        super(msg);
    }
}
