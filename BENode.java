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
import org.apache.thrift.server.TThreadPoolServer;

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
	
	Thread thread = new Thread(new ConnectionThread(hostFE, hostBE, portFE, portBE));		
    thread.start();
	
	// Connect to FE node
//	TSocket sock = new TSocket(hostFE, portFE);
//	TTransport transport = new TFramedTransport(sock);
//	TProtocol protocol = new TBinaryProtocol(transport);
//	BcryptService.Client client = new BcryptService.Client(protocol);
//	try {
//		transport.open();
//		client.storeBeNode(hostBE, portBE);
//		transport.close();
//	} catch (Exception e) {
//		e.printStackTrace();
//	}
	// launch Thrift server
//	BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(true));
//	TServerSocket socket = new TServerSocket(portBE);
//	TSimpleServer.Args sargs = new TSimpleServer.Args(socket);
//	sargs.protocolFactory(new TBinaryProtocol.Factory());
//	sargs.transportFactory(new TFramedTransport.Factory());
//	sargs.processorFactory(new TProcessorFactory(processor));
//	//sargs.maxWorkerThreads(64);
//	TSimpleServer server = new TSimpleServer(sargs);
//	server.serve();
	
//	BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(true));
//	TServerSocket socket = new TServerSocket(portBE);
//    TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
//    sargs.protocolFactory(new TBinaryProtocol.Factory());
//    sargs.transportFactory(new TFramedTransport.Factory());
//    sargs.processorFactory(new TProcessorFactory(processor));
//    TThreadPoolServer server = new TThreadPoolServer(sargs);
//    server.serve();
	
	BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(true));
	TNonblockingServerSocket socket = new TNonblockingServerSocket(portFE);
//	TServerSocket socket = new TServerSocket(portBE);
	THsHaServer.Args sargs = new THsHaServer.Args(socket);
	sargs.protocolFactory(new TBinaryProtocol.Factory());
	sargs.transportFactory(new TFramedTransport.Factory());
	sargs.processorFactory(new TProcessorFactory(processor));
	//sargs.maxWorkerThreads(64);
	THsHaServer server = new THsHaServer(sargs);
	server.serve();
	
    }

    static String getHostName()
    {
	try {
	    return InetAddress.getLocalHost().getHostName();
	} catch (Exception e) {
	    return "localhost";
	}
    }
    
    static class ConnectionThread implements Runnable {
    	private static String hostFE;
    	private static String hostBE;
    	private static int portFE;
    	private static int portBE;
    	private static BcryptService.Client client;

    	public ConnectionThread (String hostFE, String hostBE, int portFE, int portBE) {
    		this.hostFE = hostFE;
    		this.hostBE = hostBE;
    		this.portFE = portFE;
    		this.portBE = portBE;
    	}

	    public void run(){
			// contact to the FE note on startup
	       	try {
				TSocket sock = new TSocket(hostFE, portFE);
			    TTransport transport = new TFramedTransport(sock);    
			    TProtocol protocol = new TBinaryProtocol(transport);  
			    client = new BcryptService.Client(protocol);

			    transport.open();			    
//		    	client.newConnection(hostBE, (short)portBE);
			    client.storeBeNode(hostBE, portBE);

			    while(true) {
			    	heartbeat();
				}
			}
			catch (Exception x) {
			    x.printStackTrace();
			} 
	    }

	    private void heartbeat() {
	    	try {
				// Let the thread sleep for a while.
				Thread.sleep(5 * 1000);
		    	client.ping();
	    	}
	    	catch (Exception e)
	    	{
	   			establishNewConnection();
	    	}
	    }

	    private void establishNewConnection() {
	    	// contact to the FE note on startup
	       	try {
				Thread.sleep(1 * 1000);

				TSocket sock = new TSocket(hostFE, portFE);
			    TTransport transport = new TFramedTransport(sock);    
			    TProtocol protocol = new TBinaryProtocol(transport);  
			    client = new BcryptService.Client(protocol);

			    transport.open();			    
//		    	client.newConnection(hostBE, (short)portBE);
			    client.storeBeNode(hostBE, portBE);
			}
			catch (Exception x) {
			    establishNewConnection();
			} 
	    }
	}
}
