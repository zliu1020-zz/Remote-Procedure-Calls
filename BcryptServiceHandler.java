import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {

	public boolean isBeNode;
	private final ExecutorService service = Executors.newFixedThreadPool(4);
	private List<Node> nodeList = Collections.synchronizedList(new ArrayList<Node>());
	private Integer nodeIndex = -1;

	public BcryptServiceHandler(boolean isBeNode) {
		this.isBeNode = isBeNode;
	}

	public List<String> hashPassword(List<String> password, short logRounds)
			throws IllegalArgument, org.apache.thrift.TException {
                
        if(password == null || password.size() == 0){
            throw new IllegalArgument("Password list cannot be null nor empty.");
        }  
    
        if(logRounds < 4 || logRounds > 16){
            throw new IllegalArgument("Logrounds cannot be null nor out of range. Expected range is 4 - 16.");
        }  
                
		String[] res = new String[password.size()];
		if (isBeNode) {
			try {
				int size = password.size();
				int numThreads = Math.min(size, 4);
				int chunkSize = size / numThreads;
				CountDownLatch latch = new CountDownLatch(numThreads);
				if (size > 1) {
					for (int i = 0; i < numThreads; i++) {
						int startInd = i * chunkSize;
						int endInd = i == numThreads - 1 ? size : (i + 1) * chunkSize;
						service.execute(new MultiThreadHash(password, logRounds, res, startInd, endInd, latch));
					}
					latch.await();
				} else {
					hashPasswordImpl(password, logRounds, res, 0, password.size());
				}

				List<String> ret = Arrays.asList(res);
				return ret;
			} catch (Exception e) {
                //TODO: handle BE node failure if it happends during encrpytion/decrpytion 
				throw new IllegalArgument(e.getMessage());
			}
		} else {
			if (updateCurrentBeNodeIndex()) {
				Node be = nodeList.get(nodeIndex);
				if (be == null) {
					System.out.println("help");
					return null;
				}
				while (be != null) {
					try {			
						Node tmp = new Node(be.hostname, be.port);
						if (!tmp.getTransport().isOpen()) {
							tmp.getTransport().open();
						}
						System.out.println("Current BE Node:" + tmp.hostname + tmp.port);
						List<String> ret = tmp.getClient().hashPassword(password, logRounds);
						return ret;
					} catch (org.apache.thrift.transport.TTransportException e) {
						System.out.println("Hash Transport Exception port#" + be.port);
						nodeList.remove(be);
                        synchronized(nodeIndex){
                            nodeIndex--;    
                        }
						break;
						
					} finally {
	                    if (be.getTransport() != null && be.getTransport().isOpen()) {
	                    	be.getTransport().close();
	                    }
	                }
				}
			}
			
			// FE should handle the work
			System.out.println("Hash FE handles the work");
			try {
				int size = password.size();
				int numThreads = Math.min(size, 4);
				int chunkSize = size / numThreads;
				CountDownLatch latch = new CountDownLatch(numThreads);
				if (size > 1) {
					for (int i = 0; i < numThreads; i++) {
						int startInd = i * chunkSize;
						int endInd = i == numThreads - 1 ? size : (i + 1) * chunkSize;
						service.execute(new MultiThreadHash(password, logRounds, res, startInd, endInd, latch));
					}
					latch.await();
				} else {
					hashPasswordImpl(password, logRounds, res, 0, password.size());
				}

				List<String> ret = Arrays.asList(res);
				return ret;
			} catch (Exception e) {
				throw new IllegalArgument(e.getMessage());
			}	
		}
	}
	
	private boolean updateCurrentBeNodeIndex() {
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
        
		Boolean[] res = new Boolean[password.size()];
		if (isBeNode) {
			try {
				int size = password.size();
				int numThreads = Math.min(size, 4);
				int chunkSize = size / numThreads;
				CountDownLatch latch = new CountDownLatch(numThreads);
				if (size > 1) {
					for (int i = 0; i < numThreads; i++) {
						int startInd = i * chunkSize;
						int endInd = i == numThreads - 1 ? size : (i + 1) * chunkSize;
						service.execute(new MultiThreadCheck(password, hash, res, startInd, endInd, latch));
					}
					latch.await();
				} else {
					checkPasswordImpl(password, hash, res, 0, password.size());
				}

				List<Boolean> ret = Arrays.asList(res);
				return ret;
			} catch (Exception e) {
				throw new IllegalArgument(e.getMessage());
			}
		} else {
			if (updateCurrentBeNodeIndex()) {
				Node be = nodeList.get(nodeIndex);
				if (be == null) {
					System.out.println("help");
					return null;
				}
				while (be != null) {
					try {
						Node tmp = new Node(be.hostname, be.port);
						if (!tmp.getTransport().isOpen()) {
							tmp.getTransport().open();
						}
						System.out.println("Current BE Node:" + tmp.hostname + tmp.port);
						List<Boolean> ret = tmp.getClient().checkPassword(password, hash);
						return ret;
					} catch (org.apache.thrift.transport.TTransportException e) {
						System.out.println("Check Transport Exception port# " + be.port);
						nodeList.remove(be);
						break;
					}  finally {
	                    if (be.getTransport() != null && be.getTransport().isOpen()) {
	                    	be.getTransport().close();
	                    }
	                }
				}
			}
			
			// FE should handle the work
			System.out.println("Check FE handles the work");
			try {
				int size = password.size();
				int numThreads = Math.min(size, 4);
				int chunkSize = size / numThreads;
				CountDownLatch latch = new CountDownLatch(numThreads);
				if (size > 1) {
					for (int i = 0; i < numThreads; i++) {
						int startInd = i * chunkSize;
						int endInd = i == numThreads - 1 ? size : (i + 1) * chunkSize;
						service.execute(new MultiThreadCheck(password, hash, res, startInd, endInd, latch));
					}
					latch.await();
				} else {
					checkPasswordImpl(password, hash, res, 0, password.size());
				}

				List<Boolean> ret = Arrays.asList(res);
				return ret;
			} catch (Exception e) {
				throw new IllegalArgument(e.getMessage());
			}
	}
    }
	
	public void pingFrom(String host, int port) {
		System.out.println("Received health check from BE node located at host = " + host + " port = " + port);
	}
	
    private void checkPasswordImpl(List<String> passwords, List<String> hashes, Boolean[] res, int start, int end) {
        String password;
        String hash;
        for (int i = start; i < end; i++) {
            password = passwords.get(i);
            hash = hashes.get(i);
            try{
                res[i] = (BCrypt.checkpw(password, hash));
            } catch (Exception e){
                res[i] = false;
            }

        }
    }

	public void storeBeNode(String hostname, int port) throws IllegalArgument, org.apache.thrift.TException {
		try {
			Node node = new Node(hostname, port);
			if (!nodeList.contains(node)) {
				nodeList.add(node);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgument(e.getMessage());
		}
	}

	private void hashPasswordImpl(List<String> passwords, short logRounds, String[] res, int start, int end){
		try {
			for (int i = start; i < end; i++) {
				res[i] = BCrypt.hashpw(passwords.get(i), BCrypt.gensalt(logRounds));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	class MultiThreadHash implements Runnable {
		private List<String> password;
		private short logRounds;
		private String[] res;
		private int start;
		private int end;
		private CountDownLatch latch;

		public MultiThreadHash(List<String> passwords, short logRounds, String[] res, int start, int end,
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
			hashPasswordImpl(password, logRounds, res, start, end);
			latch.countDown();
		}
	}
	
    class MultiThreadCheck implements Runnable {
        private List<String> password;
        private List<String> hash;
        private Boolean[] res;
        int start;
        int end;
        CountDownLatch latch;


        public MultiThreadCheck(List<String> passwords, List<String> hashes, Boolean[] res, int start, int end, CountDownLatch latch) {
            this.password = passwords;
            this.hash = hashes;
            this.res = res;
            this.start = start;
            this.end = end;
            this.latch = latch;
        }

        @Override
        public void run() {
            checkPasswordImpl(password, hash, res, start, end);
            latch.countDown();
        }
    }

}
