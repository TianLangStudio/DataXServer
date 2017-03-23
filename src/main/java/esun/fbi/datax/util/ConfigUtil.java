package esun.fbi.datax.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.engine.conf.JobConf;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.util.PropertiesUtil;

import esun.fbi.datax.ext.multithread.Task;

/**
 * Created by zhuhq on 2015/12/14.
 */
public class ConfigUtil {
    private static final Logger logger = Logger.getLogger(ConfigUtil.class);
    //匹配占位符正则
    public static final Pattern PLACEHOLDER_PATTERN = Pattern
            .compile("(\\$)\\{([\\w\\.]+)}");
    //替换占位符
    public static String replacePlaceholder(String confStr,Map<String,String> holderValueMap) {
        if(StringUtils.isBlank(confStr) || holderValueMap == null) {
            return  confStr;
        }
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(confStr);

        while (matcher.find()) {
            String allText = matcher.group();
            String value = holderValueMap.get(matcher.group(2));
            if(value == null) {
                value = "";
            }
            //logger.info("allText:" + allText + " value:" + value);
            confStr = StringUtils.replace(confStr, allText, value);
            matcher = PLACEHOLDER_PATTERN.matcher(confStr);
        }
        return confStr;
    }

    public static String readJobDescIfInFileAndReplaceHolder(String jobDesc,Map<String,String> holderValueMap) {
        logger.info("submit job begin");
        if(jobDesc == null) {
            throw new IllegalArgumentException("job desc is required");
        }

        if(!jobDesc.contains("<")) {//jobDesc是一个文件路径

            try {
                logger.info("reader job desc content from file begin");
                jobDesc = FileUtils
                        .readFileToString(new File(jobDesc), "UTF-8");
                logger.info("reader job desc content from file end");
            }catch (Exception e) {
                logger.error(String.format("DataX read config file %s failed .",
                        jobDesc),e);
                throw new DataExchangeException(e.getCause());
            }
        }
        logger.info("replace config content placeholder begin");
        jobDesc = ConfigUtil.replacePlaceholder(jobDesc,holderValueMap);
        logger.info("replace config content placeholder end");
        return jobDesc;
    }
}
