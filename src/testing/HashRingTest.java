package testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import app_kvServer.IKVServer.CacheStrategy;
import junit.framework.TestCase;
import ecs.ECSNode;
import ecs.HashRing;
import ecs.ECSNode.NodeStatus;

public class HashRingTest extends TestCase {
    private HashRing hashRingClass = new HashRing();
    private static final String CONFIG_PATH = "ecs.config";
    private HashMap<String, NodeStatus> serverStatusInfo = new HashMap<String, NodeStatus>();
    private int numServersToAdd;

    public void setUp() {
        try {
            // Read configuration file
            BufferedReader reader = new BufferedReader(new FileReader(CONFIG_PATH));
            String l;

            while ((l = reader.readLine()) != null) {
                String[] config = l.split("\\s+", 3);
                serverStatusInfo.put(String.format("%s:%s:%s", config[0], config[1], config[2]), NodeStatus.OFFLINE);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Put server info in a list
        Set<String> serverInfoSet = serverStatusInfo.keySet();
        List<String> serverInfoList = new ArrayList<String>(serverInfoSet);

        numServersToAdd = serverStatusInfo.size() - 1;
        hashRingClass.initHashRing(serverInfoList.subList(0, numServersToAdd), CacheStrategy.None, 0);
    }

    @Test
    /**
     * Check that size of hash ring matches number of possible servers.
     */
    public void testHashRingSize() {
        HashMap<BigInteger, ECSNode> hashRing = hashRingClass.getHashRing();
        assertTrue(hashRing.size() == numServersToAdd);
    }

    @Test
    /**
     * Check that each node's placement in the hash ring makes sense.
     */
    public void testHashRingOrder() {
        boolean orderValid = true;
        HashMap<BigInteger, ECSNode> hashRing = hashRingClass.getHashRing();
        Set<BigInteger> nodeIDsSet = hashRing.keySet();
        // Use TreeSet to order IDs
        TreeSet<BigInteger> nodeIDsList = new TreeSet<BigInteger>(nodeIDsSet);
        Iterator nodeIDs = nodeIDsList.iterator();
        int size = nodeIDsList.size();
        int accum = 0;

        while (nodeIDs.hasNext()) {
            BigInteger currNodeID = (BigInteger) nodeIDs.next();
            ECSNode currNode = hashRing.get(currNodeID);
            BigInteger prevNodeID = currNode.getPrevNodeID();
            BigInteger nextNodeID = currNode.getNextNodeID();

            // Check prevNodeID < currNodeID < nextNodeID
            int isPrevNodeSmaller = currNodeID.compareTo(prevNodeID);
            int isNextNodeLarger = nextNodeID.compareTo(currNodeID);

            if (isPrevNodeSmaller == 1 && isNextNodeLarger == 1) {
                orderValid = true;
            } else if (isPrevNodeSmaller == -1 && isNextNodeLarger == 1 && accum == 0) {
                // First node's prev node will be larger
                orderValid = true;
            } else if (isPrevNodeSmaller == 1 && isNextNodeLarger == -1 && accum == size - 1) {
                // Last node's next node will be smaller
                orderValid = true;
            } else {
                orderValid = false;
                System.out.println("Error: prev, curr, and next node IDs are not ordered properly");
                System.out.println(String.format("prevNodeID: %d", prevNodeID));
                System.out.println(String.format("currNodeID: %d", currNodeID));
                System.out.println(String.format("nextNodeID: %d", nextNodeID));
                break;
            }

            accum += 1;
        }

        assertTrue(orderValid == true);
    }

    @Test
    /**
     * Check that each node's hash range matches its previous and next nodes.
     */
    public void testHashRange() {
        HashMap<BigInteger, ECSNode> hashRing = hashRingClass.getHashRing();
        Set<BigInteger> nodeIDsSet = hashRing.keySet();
        boolean hashRangeValid = true;

        for (BigInteger currNodeID : nodeIDsSet) {
            ECSNode currNode = hashRing.get(currNodeID);
            BigInteger nextNodeID = currNode.getNextNodeID();
            BigInteger[] currNodeHashRange = currNode.getNodeHashRange();
            BigInteger hashStart = currNodeHashRange[0];
            BigInteger hashStop = currNodeHashRange[1];
            int hashStartCompare = hashStart.compareTo(currNodeID);
            int hashStopCompare = hashStop.compareTo(nextNodeID);

            // Check hashStart == currNodeID, hashStop == nextNodeID
            if (hashStartCompare == 0 && hashStopCompare == 0) {
                hashRangeValid = true;
            } else {
                hashRangeValid = false;
                System.out.println("Error: invalid hash range");
                System.out.println(String.format("currNodeID: %d", currNodeID));
                System.out.println(String.format("nextNodeID: %d", nextNodeID));
                System.out.println(String.format("currNodeHashRange: %d-%d", hashStart, hashStop));
                break;
            }
        }

        assertTrue(hashRangeValid == true);
    }

    @Test
    /**
     * Test removing a node from the hash ring.
     */
    public void testNodeRemove() {
        HashMap<BigInteger, ECSNode> hashRing = hashRingClass.getHashRing();
        Random rand = new Random();
        // Choose random node to remove
        int size = hashRing.size();
        assert size != 0;
        int randIdx = rand.nextInt(hashRing.size());
        // Construct string to specify which server to remove
        List<BigInteger> sortedNodeIDs = hashRingClass.getSortedNodeIDs();
        ECSNode nodeToRemove = hashRing.get(sortedNodeIDs.get(randIdx));
        BigInteger nodeToRemoveID = nodeToRemove.getNodeID();
        String name = nodeToRemove.getNodeName();
        String host = nodeToRemove.getNodeHost();
        int port = nodeToRemove.getNodePort();
        String removeString = String.format("%s:%s:%s", name, host, String.valueOf(port));

        // Remove node
        hashRingClass.removeNode(removeString);
        int newSize = hashRing.size();

        // Check that it's not in hash ring anymore
        List<BigInteger> newSortedNodeIDs = hashRingClass.getSortedNodeIDs();
        boolean isRemovedNodeInHashRing = false;
        for (BigInteger currNodeID : newSortedNodeIDs) {
            int nodeIDCompare = nodeToRemoveID.compareTo(currNodeID);

            // nodeToRemoveID == currNodeID
            if (nodeIDCompare == 0) {
                isRemovedNodeInHashRing = true;
                System.out.println(
                        String.format("Node %s was not successfully removed. It appears to still be in the hash ring.",
                                removeString));
                break;
            }
        }

        assertTrue(nodeToRemove != null && isRemovedNodeInHashRing == false && newSize == size - 1);
    }

    @Test
    /**
     * Test adding a node to the hash ring.
     */
    public void testNodeAdd() {
        // Determine possible nodes to add
        Set<String> allServerInfo = serverStatusInfo.keySet();
        List<BigInteger> sortedNodeIDs = hashRingClass.getSortedNodeIDs();
        List<String> serversAvailable = new ArrayList<String>();
        HashMap<BigInteger, ECSNode> hashRing = hashRingClass.getHashRing();

        for (String serverInfo : allServerInfo) {
            String[] serverInfoArray = serverInfo.split(":");
            String host = serverInfoArray[1];
            int port = Integer.valueOf(serverInfoArray[2]);
            String infoToHash = hashRingClass.createStringToHash(host, port);
            BigInteger ID = hashRingClass.hashServerInfo(infoToHash);
            ECSNode currNode = hashRing.get(ID);

            // Server does not exist in hash ring
            if (currNode == null) {
                serversAvailable.add(serverInfo);
            }
        }

        // Choose random server to add
        Random rand = new Random();
        int randIdx = rand.nextInt(serversAvailable.size());
        String serverToAdd = serversAvailable.get(randIdx);
        ECSNode nodeToAdd = hashRingClass.createECSNode(serverToAdd, CacheStrategy.None, 0);
        int oldSize = hashRing.size();
        hashRingClass.addNode(nodeToAdd);
        int newSize = hashRing.size();
        ECSNode addedNode = hashRing.get(nodeToAdd.getNodeID());

        if (addedNode == null) {
            System.out.println("addedNode is null - appears that a node was not added successfully");
        }

        if (newSize != oldSize + 1) {
            System.out.println(String.format("newSize %d != oldSize %d + 1", newSize, oldSize));
        }

        assertTrue(nodeToAdd != null && addedNode != null && newSize == oldSize + 1);
    }
}
