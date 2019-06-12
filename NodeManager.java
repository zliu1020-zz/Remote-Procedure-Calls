import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
public class NodeManager {

	// key is hostname+port, value is node object
//	private static ConcurrentHashMap<String, Node> nodeMap = new ConcurrentHashMap<>();
//	private static ArrayList<Node> nodeList = (ArrayList<Node>) Collections.synchronizedList(new ArrayList<Node>());
	private static ArrayList<Node> container = new ArrayList<Node>();
	private static List<Node> nodeList = Collections.synchronizedList(container);
	
	public static void addNode(String hostname, int port) {
		Node node = new Node(hostname, port);
		String nodeId = hostname + port;
//		nodeMap.put(nodeId, node);
		nodeList.add(node);
	}
	
	public static synchronized Node getBeNode() {
		Node ret = null;
		// does not store any nodes
//		if (nodeMap.size() == 0)
//			return null;
		if (nodeList.size() == 0)
			return null;
		
//		RoundRobin it = new RoundRobin<Node>(nodeList);
//		Node node = (Node) it.iterator().next();
		
//		System.out.println("getBeNode port: " + node.port);
		return null;

		// find an idle BE node
//		for (Node node : nodeMap.values()) {
//			if (!node.getIsBusy()) {
//				node.setIsBusy(true);
//				ret = node;
//				break;
//			}
//		}

		// if every BE node is busy 
		// try to find the least load BE node
		///TODO:

		// Node ret = null;
		// if (nodeMap == null ) {
		// 	return null;
		// } else {
		// 	for (Node node: nodeMap.values()) {
		// 		ret = node;
		// 	}
		// 	return ret;
		// }
//		return ret;
	}
	
//	public static boolean containsNode(String nodeId) {
//		return nodeMap.containsKey(nodeId);
//	}
	
	public static int getMapSize() {
//		return nodeMap.size();
		return nodeList.size();
	}

}
