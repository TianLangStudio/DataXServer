package service.demo;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by zhuhq on 2015/11/30.
 */
public class HelloServiceImpl implements Hello.Iface {
    @Override
    public String helloString(String word) throws TException {
        return "hello " + word;
    }
    public static void main(String args[]) throws TTransportException {
        TServerTransport serverTransport = new TServerSocket(9777);
        TBinaryProtocol.Factory proFactory = new TBinaryProtocol.Factory();
        TProcessor processor = new Hello.Processor<HelloServiceImpl>(new HelloServiceImpl());
        TServer server = new TThreadPoolServer(
                new TThreadPoolServer.Args(serverTransport).protocolFactory(proFactory).processor(processor)
        );
        System.out.println("Start server on port 9777");
        server.serve();
    }
}
