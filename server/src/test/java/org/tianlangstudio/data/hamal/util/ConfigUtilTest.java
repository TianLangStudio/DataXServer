package org.tianlangstudio.data.hamal.util;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.tianlangstudio.data.datax.util.ConfigUtil;
import org.tianlangstudio.data.hamal.core.ConfigUtil;

/**
 * Created by zhuhq on 2015/12/14.
 */
public class ConfigUtilTest {
    @Test
    public void testReplacePlaceholder() throws Exception{
        String strWithPlaceholder = "tebleName:${mysql.table}";
        Map<String,String> params = new HashMap<String, String>();

        params.put("mysql.table","dw_mbr_userinfo_20151114");

        String str = ConfigUtil.replacePlaceholder(strWithPlaceholder, params);

        Assert.assertEquals("tebleName:dw_mbr_userinfo_20151114", str);


    }
}
