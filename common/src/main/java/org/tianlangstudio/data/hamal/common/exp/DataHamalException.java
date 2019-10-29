package org.tianlangstudio.data.hamal.common.exp;

/**
 * Created by zhuhq on 2016/4/27.
 */
public class DataHamalException extends RuntimeException {
    private DataHamalException() {}
    public DataHamalException(Throwable ex) {
        super(ex);
    }
    public DataHamalException(String msg, Throwable ex) {
        super(msg,ex);
    }

    public DataHamalException(String msg) {
        super(msg);
    }
}
