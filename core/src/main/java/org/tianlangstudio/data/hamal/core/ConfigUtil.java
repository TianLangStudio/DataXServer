package org.tianlangstudio.data.hamal.core;

import java.io.File;
import java.util.Map;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tianlangstuido.data.hamal.common.Consts;
import org.tianlangstuido.data.hamal.common.exp.DataHamalException;
import org.tianlangstuido.data.hamal.core.Constants;
import scala.collection.immutable.Stream;

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

    public static String readTaskDescIfInFileAndReplaceHolder(String taskDesc,Map<String,String> holderValueMap) {
        if(!validTaskDesc(taskDesc)) {
            throw new IllegalArgumentException(Consts.MSG_JOBDES_IS_REQUIRED);
        }
        taskDesc = readTaskDescIfInFile(taskDesc);
        logger.info("replace config content placeholder begin");
        taskDesc = ConfigUtil.replacePlaceholder(taskDesc,holderValueMap);
        logger.info("replace config content placeholder end");
        return taskDesc;
    }
    public static String readTaskDescIfInFile(String taskDesc) {
        String taskContent = taskDesc;//如果是task配置内容直接返回
        if(!validTaskDesc(taskDesc)) {
            throw new DataHamalException(Consts.MSG_JOBDES_IS_REQUIRED);
        }
        if(!taskDesc.contains("{")) {//taskDesc是一个文件路径 读取文件内容

            try {
                logger.info("reader task desc content from file begin");
                taskContent = FileUtils
                        .readFileToString(new File(taskDesc), "UTF-8");
                logger.info("reader task desc content from file end");
            }catch (Exception e) {
                logger.error(String.format("DataX read config file %s failed .",
                        taskDesc),e);
                throw new DataHamalException(e);
            }
        }
        return taskContent;
    }

    public static boolean validTaskDesc(String taskDesc) {
        return StringUtils.isNotBlank(taskDesc) && taskDesc.length() > 3;
    }
}
