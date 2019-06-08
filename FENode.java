import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.server.THsHaServer;


public class FENode {
    static Logger log;

    public static void main(String [] args) throws Exception {
	if (args.length != 1) {
	    System.err.println("Usage: java FENode FE_port");
	    System.exit(-1);
	}

	// initialize log4j
	BasicConfigurator.configure();
	log = Logger.getLogger(FENode.class.getName());

	int portFE = Integer.parseInt(args[0]);
	log.info("Launching FE node on port " + portFE);

	// launch Thrift server
	BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler());
	TNonblockingServerSocket socket = new TNonblockingServerSocket(portFE);
	THsHaServer.Args sargs = new THsHaServer.Args(socket);
	sargs.protocolFactory(new TBinaryProtocol.Factory());
	sargs.transportFactory(new TFramedTransport.Factory());
	sargs.processorFactory(new TProcessorFactory(processor));
    sargs.maxWorkerThreads(20); //???
	THsHaServer server = new THsHaServer(sargs);
	server.serve();
    }
}
