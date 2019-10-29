package org.tianlangstudio.data.hamal.server.thrift;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tianlangstudio.data.hamal.common.exp.DataHamalException;

/**
 * Created by zhuhq on 2015/11/30.
 */
public class ThriftServerApp {
    private static final Logger logger = LoggerFactory.getLogger(ThriftServerApp.class);
    private static TServer server;

    public static void start(int concurrence, String server,int port){
        start(server,port,new ThriftServerHandler(concurrence));
    }
    public static void start(String host,int port,ThriftServer.Iface handler) {
        if(server != null && server.isServing()) {
            return;
        }
        try {
            logger.info("=======start begin=======");
            TServerTransport serverTransport = new TServerSocket(port);
            TBinaryProtocol.Factory proFactory = new TBinaryProtocol.Factory();
            TProcessor processor = new ThriftServer.Processor(handler);
            server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).protocolFactory(proFactory).processor(processor)
            );
            logger.info("=======start thrift server on port:{} ======", port);
            server.serve();

        }catch (Exception ex) {
            logger.error("start server error:",ex);
            throw new DataHamalException(ex);
        }
        logger.info("******start success******");
    }

    public static void stop() {
        if(server != null && server.isServing()) {
            server.stop();
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
