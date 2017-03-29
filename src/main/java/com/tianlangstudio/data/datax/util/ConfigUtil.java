package com.tianlangstudio.data.datax.util;

import java.io.File;
import java.util.Map;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tianlangstudio.data.datax.exception.DataXException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhuhq on 2015/12/14.
 */
public class ConfigUtil {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtil.class);
    //匹配占位符正则
    public static final Pattern PLACEHOLDER_PATTERN = Pattern
            .compile("(\\$)\\{([\\w.]+)}");
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
        jobDesc = readJobDescIfInFile(jobDesc);
        logger.info("replace config content placeholder begin");
        jobDesc = ConfigUtil.replacePlaceholder(jobDesc,holderValueMap);
        logger.info("replace config content placeholder end");
        return jobDesc;
    }
    public static String readJobDescIfInFile(String jobDesc) {
        String jobContent = "";
        if(!jobDesc.contains("{")) {//jobDesc是一个文件路径

            try {
                logger.info("reader job desc content from file begin");
                jobContent = FileUtils
                        .readFileToString(new File(jobDesc), "UTF-8");
                logger.info("reader job desc content from file end");
            }catch (Exception e) {
                logger.error(String.format("DataX read config file %s failed .",
                        jobDesc),e);
                throw new DataXException(e.getCause());
            }
        }
        return jobContent;
    }
}
