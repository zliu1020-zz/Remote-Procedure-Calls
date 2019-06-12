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


public class Node {
	
	String hostname;
	int port;
	BcryptService.Client client;
	TTransport transport;
	boolean isBusy;
	
	public Node(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;

		TSocket sock = new TSocket(hostname, port);
		transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		client = new BcryptService.Client(protocol);
		isBusy = false;
	}

	public TTransport getTransport() {
		return this.transport;
	}

	public BcryptService.Client getClient() {
		return this.client;
	}

	public void setIsBusy(boolean busy) {
		this.isBusy = busy;
	}

	public boolean getIsBusy() {
		return this.isBusy;
	}

}
