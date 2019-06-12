import java.net.InetAddress;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.server.THsHaServer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportFactory;

public class BENode {
    static Logger log;

    public static void main(String [] args) throws Exception {
	if (args.length != 3) {
	    System.err.println("Usage: java BENode FE_host FE_port BE_port");
	    System.exit(-1);
	}

	// initialize log4j
	BasicConfigurator.configure();
	log = Logger.getLogger(BENode.class.getName());

	String hostFE = args[0];
	String hostBE = InetAddress.getLocalHost().getHostName();
	
	int portFE = Integer.parseInt(args[1]);
	int portBE = Integer.parseInt(args[2]);
	log.info("Launching BE node on port " + portBE + " at host " + getHostName());
	
	// Connect to FE node
	TSocket sock = new TSocket(hostFE, portFE);
	TTransport transport = new TFramedTransport(sock);
	TProtocol protocol = new TBinaryProtocol(transport);
	BcryptService.Client client = new BcryptService.Client(protocol);
	try {
		transport.open();
		client.storeBeNode(hostBE, portBE);
		transport.close();
	} catch (Exception e) {
		e.printStackTrace();
	}
	// launch Thrift server
	BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(true));
	TServerSocket socket = new TServerSocket(portBE);
	TSimpleServer.Args sargs = new TSimpleServer.Args(socket);
	sargs.protocolFactory(new TBinaryProtocol.Factory());
	sargs.transportFactory(new TFramedTransport.Factory());
	sargs.processorFactory(new TProcessorFactory(processor));
	//sargs.maxWorkerThreads(64);
	TSimpleServer server = new TSimpleServer(sargs);
	server.serve();
	
//	BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(true));
//	TNonblockingServerSocket socket = new TNonblockingServerSocket(portFE);
////	TServerSocket socket = new TServerSocket(portBE);
//	THsHaServer.Args sargs = new THsHaServer.Args(socket);
//	sargs.protocolFactory(new TBinaryProtocol.Factory());
//	sargs.transportFactory(new TFramedTransport.Factory());
//	sargs.processorFactory(new TProcessorFactory(processor));
//	//sargs.maxWorkerThreads(64);
//	THsHaServer server = new THsHaServer(sargs);
//	server.serve();
	
    }

    static String getHostName()
    {
	try {
	    return InetAddress.getLocalHost().getHostName();
	} catch (Exception e) {
	    return "localhost";
	}
    }
}
