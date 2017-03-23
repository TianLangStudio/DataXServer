package esun.fbi.datax.exception;

/**
 * Created by zhuhq on 2016/4/27.
 */
public class DataxException extends RuntimeException {
    private DataxException() {}
    public DataxException(Exception ex) {
        super(ex);
    }
    public DataxException(String msg,Exception ex) {
        super(msg,ex);
    }

    public DataxException(String msg) {
        super(msg);
    }
}
