package org.tianlangstudio.data.hamal.common.exp;

import org.apache.commons.lang3.exception.ExceptionUtils;

public class ExceptionUtil {
    public static String trace(Throwable throwable) {
        return ExceptionUtils.getStackTrace(throwable);
    }
}
