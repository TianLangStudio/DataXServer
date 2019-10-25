package org.tianlangstudio.data.hamal.yarn.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tianlangstudio.data.hamal.yarn.Client;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhuhq on 17-4-1.
 */
public class ClientTest {
    private static final Logger logger = LoggerFactory.getLogger(ClientTest.class);
    static MiniYARNCluster miniCluster;
    @BeforeClass
    public static void startYarn() throws Exception{

        Map<String, String> env = System.getenv();
        for(Map.Entry<String, String> entry : env.entrySet()) {
            logger.info("key:{}, value:{}", entry.getKey(), entry.getValue());
        }
        logger.info("classpath:{}",System.getProperty("java.class.path"));
        logger.info("JAVA_HOME:{}",env.get("JAVA_HOME"));

        Properties sysProperties = System.getProperties();
        for(String key : sysProperties.stringPropertyNames()) {
            logger.info("sys key:{}, value:{}", key, sysProperties.getProperty(key));
        }
        YarnConfiguration conf = new YarnConfiguration();
        conf.set("yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage",
                "100.0");
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 256);
//        conf.setClass(YarnConfiguration.RM_SCHEDULER,
//                FifoScheduler.class, ResourcesScheduler.class);
        miniCluster = new MiniYARNCluster("ClientTest", 1, 1, 1);
        miniCluster.init(conf);
        miniCluster.start();

        Configuration config = miniCluster.getConfig();
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        String rmAddress = "";
        while ( (rmAddress = config.get(YarnConfiguration.RM_ADDRESS)).split(":")[1] == "0") {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("Timed out waiting for RM to come up.");
            }
            logger.debug("RM address still not set in configuration, waiting...");
            TimeUnit.MILLISECONDS.sleep(100);
        }
        logger.info("RM start on port:{}", rmAddress);
        String rmSchedulerAddress = config.get(YarnConfiguration.RM_SCHEDULER_ADDRESS);
        System.setProperty(YarnConfiguration.RM_SCHEDULER_ADDRESS,rmSchedulerAddress);
    }

    @Test
    public void testClient() throws Exception {
        YarnConfiguration conf = new YarnConfiguration(miniCluster.getConfig());
        new Client(conf).run(null);
    }

    @AfterClass
    public static void stopYarn() {
        miniCluster.stop();
    }

}
