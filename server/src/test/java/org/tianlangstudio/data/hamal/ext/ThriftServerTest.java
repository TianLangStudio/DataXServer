package org.tianlangstudio.data.hamal.ext;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.tianlangstudio.data.datax.ext.thrift.TaskCost;
import com.tianlangstudio.data.datax.ext.thrift.TaskResult;
import com.tianlangstudio.data.datax.ext.thrift.ThriftServer;
import com.tianlangstudio.data.datax.main.ThriftServerMain;

import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhuhq on 2015/12/2.
 */
public class ThriftServerTest {
    private final static Logger logger = LoggerFactory.getLogger(ThriftServerTest.class);



    @BeforeClass
    public static void startServer() throws Exception {

        String dataxHome = ConfigFactory.load().getString("datax.home");
        System.setProperty("datax.home", dataxHome);
        logger.info("datax.home:{}",System.getProperty("datax.home"));
        new Thread(new Runnable() {
            @Override
            public void run() {
                ThriftServerMain.start(4,"127.0.0.1",9777);
            }
        }).start();
        Thread.sleep(3000);

    }


    @AfterClass
    public static void stopServer() {
        ThriftServerMain.stop();
    }

    @Test
    public void testSubmitJob() throws Exception {


        TTransport transport = new TSocket("127.0.0.1",9777);
        TProtocol protocol = new TBinaryProtocol(transport);
        ThriftServer.Client client = new ThriftServer.Client(protocol);
        transport.open();
        String taskId1 = client.submitJob("/data1/code/github/DataX/target/datax/datax/stream2stream.json");
        //String taskId2 = client.submitJob("/home/datax/datax/jobs/postgrereader_to_oraclewriter_zhuhq.xml");
        System.out.println(taskId1);
        //System.out.println(taskId2);
        transport.close();

    }

    @Test
    public void testSubmitJobWithParams() throws Exception {


        TTransport transport = new TSocket("192.168.41.225",9777);
        TProtocol protocol = new TBinaryProtocol(transport);
        ThriftServer.Client client = new ThriftServer.Client(protocol);
        transport.open();
        String jobDesc = FileUtils
                .readFileToString(new File("D:\\work\\datax\\jobs\\postgrereader_to_mysqlwriter_zhuhq.xml"), "UTF-8");
        System.out.println(jobDesc);
        Map<String,String> params = new HashMap<String, String>();
        params.put("mysql.table","dw_mbr_userinfo_20151114");
        String taskId1 = client.submitJobWithParams(jobDesc,params);
        System.out.println(taskId1);
        transport.close();
    }
    @Test
    public void testGetJobResult() throws Exception{
        TTransport transport = new TSocket("127.0.0.1",9777);
        TProtocol protocol = new TBinaryProtocol(transport);
        ThriftServer.Client client = new ThriftServer.Client(protocol);
        transport.open();
        TaskResult taskResult1 = client.getJobResult("4956993821621");
        //TaskResult taskResult2 = client.getJobResult("127.0.0.1:9777/2d2b61vxwdo");
        System.out.println(taskResult1==null?"":taskResult1.success + " " + taskResult1.msg);
        //System.out.println(taskResult2==null?"":taskResult2.getMsg());
        transport.close();
    }

    @Test
    public void testGetJobCost() throws Exception{
        TTransport transport = new TSocket("192.168.41.225",9777);
        TProtocol protocol = new TBinaryProtocol(transport);
        ThriftServer.Client client = new ThriftServer.Client(protocol);
        transport.open();
        TaskCost cost1 = client.getJobCost("127.0.0.1:9777/2d2h3e85x3q");
        TaskCost cost2 = client.getJobCost("127.0.0.1:9777/2d2h3e9er3t");
        System.out.println(cost1);
        System.out.println(cost2);
        transport.close();
    }
}
