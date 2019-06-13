import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
    static Logger log;

	public boolean isBeNode;
	private final ExecutorService executorService = Executors.newFixedThreadPool(4);
	private List<Node> nodeList = Collections.synchronizedList(new ArrayList<Node>());
	private Integer nodeIndex = -1;

	public BcryptServiceHandler(boolean isBeNode) {
		this.isBeNode = isBeNode;
	    BasicConfigurator.configure();
	    log = Logger.getLogger(BcryptServiceHandler.class.getName());
	}
    
    public List<String> concurrentHashing(List<String> password, short logRounds) throws Exception{
        String[] res = new String[password.size()];
        int size = password.size();
        int numThreads = Math.min(size, 4);
        int chunkSize = size / numThreads;
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            int start = i * chunkSize;
            int end = i == numThreads - 1 ? size : (i + 1) * chunkSize;
            executorService.execute(new HashPasswordRunnable(password, logRounds, res, start, end, latch));
        }
        
        latch.await();
        return Arrays.asList(res);
    }
    
    public List<Boolean> concurrentChecking(List<String> password, List<String> hash) throws Exception{
        Boolean[] res = new Boolean[password.size()];  
        int size = password.size();
        int numThreads = Math.min(size, 4);
        int chunkSize = size / numThreads;
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            int start = i * chunkSize;
            int end = i == numThreads - 1 ? size : (i + 1) * chunkSize;
            executorService.execute(new CheckPasswordRunnable(password, hash, res, start, end, latch));
        }
        latch.await();
        return Arrays.asList(res);
    }
    

	public List<String> hashPassword(List<String> password, short logRounds)
			throws IllegalArgument, org.apache.thrift.TException {
        // Error handling        
        if(password == null || password.size() == 0){
            throw new IllegalArgument("Password list cannot be null nor empty.");
        }  
    
        if(logRounds < 4 || logRounds > 16){
            throw new IllegalArgument("Logrounds cannot be null nor out of range. Expected range is 4 - 16.");
        }  
		if (isBeNode) {
			try {
				return concurrentHashing(password, logRounds);
			} catch (Exception e) {
				throw new IllegalArgument(e.getMessage());
			}
		} else {
            // Round Robin - get the next available and update index
			if (isAnyBENodeAvailable()) {
				Node be = nodeList.get(nodeIndex);
                Node n = new Node(be.hostname, be.port);
                try {			
				    log.info("Hash Password - FE assigns work to BE Node:" + be.hostname + " " + be.port);
                    if (!n.getTransport().isOpen()) {
                        n.getTransport().open();
                    }
				    return n.getClient().hashPassword(password, logRounds);
				} catch (org.apache.thrift.transport.TTransportException e) {
				    log.warn("Hash Password - Exception from BE Node:" + be.hostname + " " + be.port + ". Removing the corrupted BE node.");
				    nodeList.remove(be);
                    synchronized(nodeIndex){
                        nodeIndex--;    
                    }
						
				} finally {
	               if (n != null && n.getTransport() != null && n.getTransport().isOpen()) {
	                   n.getTransport().close();
	               }
	            }
			}
			
			// FE should handle the work
			log.info("Hash Password - FE is taking over the work.");
			try {
				return concurrentHashing(password, logRounds);
			} catch (Exception e) {
				throw new IllegalArgument(e.getMessage());
			}	
		}
	}
	
	private boolean isAnyBENodeAvailable() {
		if (nodeList.size() == 0) {
			return false;
		} else {
            synchronized(nodeIndex){
                nodeIndex  = (nodeIndex + 1) % nodeList.size();   
            }
			return true;
		}
	}
	
	public List<Boolean> checkPassword(List<String> password, List<String> hash)
	throws IllegalArgument, org.apache.thrift.TException {
        if(password == null || password.size() == 0){
            throw new IllegalArgument("Password list cannot be null nor empty.");
        }  
        
        if(hash == null || hash.size() == 0){
            throw new IllegalArgument("Hash list cannot be null nor empty.");
        }  

		if (isBeNode) {
			try {
				return concurrentChecking(password, hash);
			} catch (Exception e) {
				throw new IllegalArgument(e.getMessage());
			}
		} else {
			if (isAnyBENodeAvailable()) {
				Node be = nodeList.get(nodeIndex);
                Node n = new Node(be.hostname, be.port);
                try {
				    if (!n.getTransport().isOpen()) {
				        n.getTransport().open();
				    }
				    log.info("Check Password - FE assigns work to BE Node:" + be.hostname + " " + be.port);
				    return n.getClient().checkPassword(password, hash);
				} catch (org.apache.thrift.transport.TTransportException e) {
				    log.warn("Check Password - Exception from BE Node:" + be.hostname + " " + be.port + ". Removing the corrupted BE node.");
				    nodeList.remove(be);
				} finally {
	               if (n != null && n.getTransport() != null && n.getTransport().isOpen()) {
	                   n.getTransport().close();
	               }
	           }
			}
			
			// FE should handle the work
			log.info("Check Password - FE is taking over the work.");
			try {
				return concurrentChecking(password, hash);
			} catch (Exception e) {
				throw new IllegalArgument(e.getMessage());
			}
	   }
    }
	
	public void pingFrom(String host, int port) {
		log.info("Received health check from BE node located at host = " + host + " port = " + port);
	}

	public void storeBeNode(String hostname, int port) throws IllegalArgument, org.apache.thrift.TException {
		try {
			Node node = new Node(hostname, port);
			if (!nodeList.contains(node)) {
				nodeList.add(node);
			}
		} catch (Exception e) {
            log.error("Failed to insert BE node. Msg: " + e.getMessage());
			e.printStackTrace();
		}
	}

	class HashPasswordRunnable implements Runnable {
		private List<String> password;
		private short logRounds;
		private String[] res;
		private int start;
		private int end;
		private CountDownLatch latch;

		public HashPasswordRunnable(List<String> passwords, short logRounds, String[] res, int start, int end,
				CountDownLatch latch) {
			this.logRounds = logRounds;
			this.password = passwords;
			this.res = res;
			this.start = start;
			this.end = end;
			this.latch = latch;
		}

		@Override
		public void run() {
			try {
			    for (int i = start; i < end; i++) {
				    res[i] = BCrypt.hashpw(password.get(i), BCrypt.gensalt(logRounds));
			    }
		    } catch (Exception e) {
                 log.error("Failed to hash password. Msg: " + e.getMessage());
			     e.printStackTrace();
		    }
			latch.countDown();
		}
	}
	
    class CheckPasswordRunnable implements Runnable {
        private List<String> password;
        private List<String> hash;
        private Boolean[] res;
        int start;
        int end;
        CountDownLatch latch;


        public CheckPasswordRunnable(List<String> passwords, List<String> hashes, Boolean[] res, int start, int end, CountDownLatch latch) {
            this.password = passwords;
            this.hash = hashes;
            this.res = res;
            this.start = start;
            this.end = end;
            this.latch = latch;
        }

        @Override
        public void run() {
            for (int i = start; i < end; i++) {
                try{
                    res[i] = BCrypt.checkpw(password.get(i), hash.get(i));
                } catch (Exception e){
                    log.warn("Failed to check password against hash. Will record the result as FALSE. Msg: " + e.getMessage());
                    res[i] = false;
                }
            }   
            latch.countDown();
        }
    }

}
