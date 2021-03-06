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

        // parse CLI arguments    
	   String hostFE = args[0];
	   String hostBE = getHostName();
	   int portFE = Integer.parseInt(args[1]);
	   int portBE = Integer.parseInt(args[2]);
	   log.info("Launching BE node on port " + portBE + " at host " + getHostName());
        
        // start the health check thread
        Thread healthCheckThread = new Thread(new HealthCheckRunnable(hostFE, hostBE, portFE, portBE));		
        healthCheckThread.start();	
        
        // serve BE node on startup
        BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(true));
        TNonblockingServerSocket socket = new TNonblockingServerSocket(portBE);
        THsHaServer.Args sargs = new THsHaServer.Args(socket);
	    sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
	    sargs.processorFactory(new TProcessorFactory(processor));
	    THsHaServer server = new THsHaServer(sargs);
	    server.serve();
    }
    
    static class HealthCheckRunnable implements Runnable {
    	private static String hostFE;
    	private static String hostBE;
    	private static int portFE;
    	private static int portBE;
    	private static BcryptService.Client client;

    	public HealthCheckRunnable (String hostFE, String hostBE, int portFE, int portBE) {
    		this.hostFE = hostFE;
    		this.hostBE = hostBE;
    		this.portFE = portFE;
    		this.portBE = portBE;
    	}

	    public void run(){
                // establish connection to FE node 
	            TSocket sock = new TSocket(hostFE, portFE);
                TTransport transport = new TFramedTransport(sock);    
                TProtocol protocol = new TBinaryProtocol(transport);  
                BcryptService.Client client = new BcryptService.Client(protocol);
        
                // write BE node into the array of available nodes    
                try {
                    transport.open();			    
                    client.storeBeNode(hostBE, portBE);
                }catch(Exception e) {
                	log.warn("Cannot open the transport. FE does not exist.");
                }
                
                // run the actual health check indefinitely
			    while(true) {
			    	try {
				        Thread.sleep(2000);
//			            sock = new TSocket(hostFE, portFE);
//		                transport = new TFramedTransport(sock);    
//		                protocol = new TBinaryProtocol(transport);  
//		                client = new BcryptService.Client(protocol);
				        if (!transport.isOpen())
				        	transport.open();
		    	        client.pingFrom(hostBE, portBE);
	    	        }
	    	        catch (Exception e){
	    	        	 e.printStackTrace();
                         log.warn("Exception caught during health check. The connect b/w BE and FE is likely corrupted.");
                         retry();       
	    	        }
				}
	    }
        
        public void retry(){
			try {
                log.info("Trying to re-connect to FE.");
                Thread.sleep(2000); 
                // establish connection to FE node 
	            TSocket sock = new TSocket(hostFE, portFE);
                TTransport transport = new TFramedTransport(sock);    
                TProtocol protocol = new TBinaryProtocol(transport);  
                BcryptService.Client client = new BcryptService.Client(protocol);	
                transport.open();
                client.storeBeNode(hostBE, portBE);
                transport.close(); 
	    	}
	        catch (Exception e){
                log.warn("Exception caught during retry.");
                retry();
	       }
				
        }
	}
    
    static String getHostName(){
	   try {
	       return InetAddress.getLocalHost().getHostName();
	   } catch (Exception e) {
           return "localhost";
	   }
    }
}
