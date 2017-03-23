package esun.fbi.datax.main;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import esun.fbi.datax.exception.DataxException;
import esun.fbi.datax.ext.server.ThriftServerHandler;
import esun.fbi.datax.ext.thrift.ThriftServer;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;


import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhuhq on 2015/11/30.
 */
public class ThriftServerMain {
    private static final Logger logger = Logger.getLogger(ThriftServerMain.class);
    public static void start(int concurrence,String server,int port){
        start(concurrence,server,port,new ThriftServerHandler(concurrence,server, port));
    }
    public static void start(int concurrence,String host,int port,ThriftServer.Iface handler) {
        try {
            System.out.println("start begin");
            TServerTransport serverTransport = new TServerSocket(port);
            TBinaryProtocol.Factory proFactory = new TBinaryProtocol.Factory();
            TProcessor processor = new ThriftServer.Processor(handler);
            TServer server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).protocolFactory(proFactory).processor(processor)
            );
            System.out.println("start thrift server begin");
            server.serve();
            System.out.println("start thrift server on port:" + port);
        }catch (Exception ex) {
            System.out.println("start server error:" + ex.getMessage());
            logger.error("error:",ex);
            ex.printStackTrace();
            throw new DataxException(ex);
            //System.exit(1);
        }
    }
    public static  void main(String args[]) throws Exception{
        Config config  = ConfigFactory.load();
        Config serverConfig = config.getConfig("server");
        String ip = serverConfig.getString("ip");
        int port = serverConfig.getInt("port");
        int concurrence = serverConfig.getInt("concurrence");
        start(concurrence,ip,port);
    }
}
