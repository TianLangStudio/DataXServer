package esun.fbi.datax;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import esun.fbi.datax.util.ConfigUtil;

/**
 * Created by zhuhq on 2015/12/14.
 */
public class TestConfigUtil {
    @Test
    public void replacePlaceholderTest() throws Exception{
        String jobDesc = FileUtils
                .readFileToString(new File("D:\\work\\datax\\jobs\\postgrereader_to_mysqlwriter_zhuhq.xml"), "UTF-8");
        System.out.println(jobDesc);
        Map<String,String> params = new HashMap<String, String>();
        params.put("mysql.table","dw_mbr_userinfo_20151114");
        String job = ConfigUtil.replacePlaceholder(jobDesc,params);
        System.out.println(job);
    }
}
