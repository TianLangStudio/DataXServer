package org.tianlangstudio.data.hamal.core;

import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhuhq on 17-3-23.
 */
public class Engine extends com.alibaba.datax.core.Engine {
    private final static Logger logger = LoggerFactory.getLogger(Engine.class);
    public void start(String jobContent, String jobId){
        Configuration configuration = Configuration.from(jobContent);
   /*     boolean isStandAloneMode = "standalone".equalsIgnoreCase(RUNTIME_MODE);
        if (!isStandAloneMode && jobId == -1) {
            // 如果不是 standalone 模式，那么 jobId 一定不能为-1
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "非 standalone 模式必须在 URL 中提供有效的 jobId.");
        }*/
        System.out.println("jobid***********");
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, Long.parseLong(jobId));

        configuration.merge(
                Configuration.from(new File(CoreConstant.DATAX_CONF_PATH)),
                false);
        // todo config优化，只捕获需要的plugin
        String readerPluginName = configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        String writerPluginName = configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);

        String preHandlerName = configuration.getString(
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);

        String postHandlerName = configuration.getString(
                CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME);

        Set<String> pluginList = new HashSet<String>();
        pluginList.add(readerPluginName);
        pluginList.add(writerPluginName);

        if(StringUtils.isNotEmpty(preHandlerName)) {
            pluginList.add(preHandlerName);
        }
        if(StringUtils.isNotEmpty(postHandlerName)) {
            pluginList.add(postHandlerName);
        }
        try {
            configuration.merge(ConfigParser.parsePluginConfig(new ArrayList<String>(pluginList)), false);
        }catch (Exception e){
            //吞掉异常，保持log干净。这里message足够。
            logger.warn(String.format("插件[%s,%s]加载失败，1s后重试... Exception:%s ", readerPluginName, writerPluginName, e.getMessage()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                //
            }
            configuration.merge(ConfigParser.parsePluginConfig(new ArrayList<String>(pluginList)), false);
        }

        //打印vmInfo
        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
            logger.info(vmInfo.toString());
        }

        logger.info("\n" + com.alibaba.datax.core.Engine.filterJobConfiguration(configuration) + "\n");

        logger.debug(configuration.toJSON());

        ConfigurationValidate.doValidate(configuration);

        start(configuration);
    }
}
