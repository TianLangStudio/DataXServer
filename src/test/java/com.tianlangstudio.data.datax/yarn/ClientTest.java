package com.tianlangstudio.data.datax.yarn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by zhuhq on 17-4-1.
 */
public class ClientTest {
    static MiniYARNCluster miniCluster;
    @BeforeClass
    public static void startYarn() {
        YarnConfiguration conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
//        conf.setClass(YarnConfiguration.RM_SCHEDULER,
//                FifoScheduler.class, ResourcesScheduler.class);
        miniCluster = new MiniYARNCluster("ClientTest", 2, 1, 1);
        miniCluster.init(conf);
        miniCluster.start();
    }

    @Test
    public void testClient() throws Exception {
        YarnConfiguration conf = new YarnConfiguration(miniCluster.getConfig());
        new Client(conf).run(new Path("./"));
    }

    @AfterClass
    public static void stopYarn() {
        miniCluster.stop();
    }

}
