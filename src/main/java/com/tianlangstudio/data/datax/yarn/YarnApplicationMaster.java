package com.tianlangstudio.data.datax.yarn;

import java.util.List;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import com.tianlangstudio.data.datax.main.ThriftServerMain;

/**
 * Created by zhuhq on 2016/4/26.
 */
public class YarnApplicationMaster implements AMRMClientAsync.CallbackHandler {

    @Override public void onContainersCompleted(final List<ContainerStatus> list) {

    }

    @Override public void onContainersAllocated(final List<Container> list) {

    }

    @Override public void onShutdownRequest() {

    }

    @Override public void onNodesUpdated(final List<NodeReport> list) {

    }

    @Override public float getProgress() {
        return 0;
    }

    @Override public void onError(final Throwable throwable) {

    }
    private static Configuration yarnConfiguration;
    private static Config dataxThriftServerConfig;
    private static  Resource executorResource;
    private static  Priority executorPriority;
    public static void main(String args[]) {
        dataxThriftServerConfig = ConfigFactory.load();
        yarnConfiguration = new YarnConfiguration();
        executorResource = Records.newRecord(Resource.class);
        executorPriority = Records.newRecord(Priority.class);

    }
    public void runMainLoop() throws Exception {
        AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(2000,this);
        rmClient.init(yarnConfiguration);
        rmClient.start();

        //Register with ResourceManager
        System.out.println("[AM] registerApplicationMaster begin");
        rmClient.registerApplicationMaster("",0,"");
        System.out.println("[AM] registerApplicationMaster end");

        executorPriority.setPriority(0);

        executorResource.setMemory(1024);
        executorResource.setVirtualCores(2);



    }

    public void startThriftServer() {
        Config serverConfig = dataxThriftServerConfig.getConfig("server");
        String ip = serverConfig.getString("ip");
        int port = serverConfig.getInt("port");
        int concurrence = serverConfig.getInt("concurrence");
        new ThriftServerMain().start(concurrence,ip,port);

    }
}
