package service.demo;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import service.demo.Hello;

/**
 * Created by zhuhq on 2015/11/30.
 */
public class HelloClient {
    public static void main(String args[]) throws TException {
        TTransport transport = new TSocket("127.0.0.1",9777);
        TProtocol protocol = new TBinaryProtocol(transport);
        Hello.Client client = new Hello.Client(protocol);
        transport.open();
        System.out.print(client.helloString("word"));
        transport.close();

    }
}
