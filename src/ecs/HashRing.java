package ecs;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.CacheStrategy;
import shared.DebugHelper;
import shared.Metadata;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HashRing {
    private static Logger logger = Logger.getRootLogger();

    private HashMap<BigInteger, ECSNode> hashRing = new HashMap<>(); // MD5 hash -> node

    public HashRing() {
        DebugHelper.logFuncEnter(logger);
        DebugHelper.logFuncExit(logger);
    }

    /**
     * Initialize hash ring.
     * 
     * @param serverInfo All server information, each in a <name>:<ip>:<port> format
     */
    public List<ECSNode> initHashRing(List<String> serverInfo, CacheStrategy cacheStrategy, int cacheSize) {
        DebugHelper.logFuncEnter(logger);
        int hashRingSize = serverInfo.size();
        List<ECSNode> nodesAdded = new ArrayList<ECSNode>();

        // Add each node to hash ring
        for (int i = 0; i < hashRingSize; i++) {
            String info = serverInfo.get(i);
            ECSNode node = createECSNode(info, cacheStrategy, cacheSize);
            hashRing.put(node.getNodeID(), node);
            nodesAdded.add(node);
        }

        List<BigInteger> nodeIDsList = getSortedNodeIDs();

        // Calculate hash range for each node
        for (int i = 0; i < hashRingSize; i++) {
            setNodeHashRange(i, nodeIDsList, hashRingSize);
        }

        DebugHelper.logFuncExit(logger);

        return nodesAdded;
    }

    private void setNodeHashRange(int currNodeIdx, List<BigInteger> nodeIDsList, int hashRingSize) {
        DebugHelper.logFuncEnter(logger);
        BigInteger currNodeID = nodeIDsList.get(currNodeIdx);

        // Calculate hash range
        int prevNodeIdx = (currNodeIdx == 0) ? hashRingSize - 1 : currNodeIdx - 1;
        int nextNodeIdx = (currNodeIdx + 1) % hashRingSize;
        BigInteger nextNodeID = nodeIDsList.get(nextNodeIdx);
        BigInteger[] hashRange = { currNodeID, nextNodeID };

        // Set ring position and hash range info
        ECSNode currNode = hashRing.get(currNodeID);
        BigInteger prevNodeID = nodeIDsList.get(prevNodeIdx);
        currNode.setPrevNodeID(prevNodeID);
        currNode.setNextNodeID(nextNodeID);
        currNode.setNodeHashRange(hashRange);
        DebugHelper.logFuncExit(logger);
    }

    public ECSNode createECSNode(String info, CacheStrategy cacheStrategy, int cacheSize) {
        DebugHelper.logFuncEnter(logger);
        String[] infoArray = info.split(":");
        String name = infoArray[0];
        String host = infoArray[1];
        int port = Integer.parseInt(infoArray[2]);
        String infoToHash = createStringToHash(host, port);
        BigInteger nodeID = hashServerInfo(infoToHash);
        logger.info(String.format("Hashed %s --> %d", infoToHash, nodeID));
        logger.debug(String.format("ID: %d, name: %s, host: %s, port: %d, cache: %s, %d", nodeID, name, host, port,
                cacheStrategy.toString(), cacheSize));

        ECSNode node = new ECSNode(nodeID, name, host, port, cacheStrategy, cacheSize);
        DebugHelper.logFuncExit(logger);

        return node;
    }

    public void addNode(ECSNode node) {
        DebugHelper.logFuncEnter(logger);
        BigInteger newNodeID = node.getNodeID();
        List<BigInteger> nodeIDsList = getSortedNodeIDs();
        int hashRingSize = getHashRingSize();
        int newNodeIdx = -1;

        for (int i = 0; i < hashRingSize; i++) {
            BigInteger currNodeID = nodeIDsList.get(i);
            int isNewNodeIDLarger = newNodeID.compareTo(currNodeID);

            if (isNewNodeIDLarger == -1) {
                // newNodeID < currNodeID;
                newNodeIdx = i;
                nodeIDsList.add(newNodeIdx, newNodeID);
                break;
            } else if (isNewNodeIDLarger == 1) {
                // newNodeID > currNodeID;
                continue;
            } else {
                logger.error(String.format("Found two nodes with the same ID: %d, %d", newNodeID, currNodeID));
                break;
            }
        }

        setNodeHashRange(newNodeIdx, nodeIDsList, hashRingSize + 1);

        // Make previous and next nodes point to current node
        hashRing.put(newNodeID, node);
        ECSNode prevNode = hashRing.get(node.getPrevNodeID());
        ECSNode nextNode = hashRing.get(node.getNextNodeID());
        prevNode.updateNodeIfBefore(newNodeID);
        nextNode.updateNodeIfAfter(newNodeID);
        DebugHelper.logFuncExit(logger);
    }

    public void addNodes(ECSNode[] nodeArray) {
        DebugHelper.logFuncEnter(logger);

        for (ECSNode node : nodeArray) {
            addNode(node);
        }

        DebugHelper.logFuncExit(logger);
    }

    public void removeNode(String serverInfo) {
        DebugHelper.logFuncEnter(logger);
        String[] serverInfoArray = serverInfo.split(":");
        String host = serverInfoArray[1];
        int port = Integer.parseInt(serverInfoArray[2]);

        String infoToHash = createStringToHash(host, port);
        BigInteger nodeID = hashServerInfo(infoToHash);
        ECSNode node = hashRing.get(nodeID);
        removeNode(node);
        DebugHelper.logFuncExit(logger);
    }

    public void removeNode(ECSNode node) {
        DebugHelper.logFuncEnter(logger);
        ECSNode prevNode = hashRing.get(node.getPrevNodeID());
        BigInteger prevNodeID = node.getPrevNodeID();
        ECSNode nextNode = hashRing.get(node.getNextNodeID());
        BigInteger nextNodeID = node.getNextNodeID();

        prevNode.setNextNodeID(nextNodeID);
        nextNode.setPrevNodeID(prevNodeID);
        hashRing.remove(node.getNodeID());
        DebugHelper.logFuncExit(logger);
    }

    public BigInteger hashServerInfo(String info) {
        DebugHelper.logFuncEnter(logger);
        BigInteger hashBigInt = null;

        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] hash = md5.digest(info.getBytes());
            hashBigInt = new BigInteger(1, hash);
        } catch (Exception e) {
            logger.error(String.format("Unable to hash server info: %s", info));
            e.printStackTrace();
        }

        DebugHelper.logFuncExit(logger);

        return hashBigInt;
    }

    private String createStringToHash(String host, int port) {
        return host + ":" + port;
    }

    /**
     * Get all node IDs from the hash ring and sort in ascending order.
     * 
     * @return Sorted list of node IDs.
     */
    private List<BigInteger> getSortedNodeIDs() {
        DebugHelper.logFuncEnter(logger);
        Set<BigInteger> nodeIDsSet = this.hashRing.keySet();
        List<BigInteger> nodeIDsList = new ArrayList<BigInteger>(nodeIDsSet);
        Collections.sort(nodeIDsList);
        DebugHelper.logFuncExit(logger);

        return nodeIDsList;
    }

    public HashMap<String, Metadata> getAllMetadata() {
        DebugHelper.logFuncEnter(logger);
        HashMap<String, Metadata> allMetadata = new HashMap<String, Metadata>();

        for (BigInteger serverID : hashRing.keySet()) {
            ECSNode node = hashRing.get(serverID);
            String name = node.getNodeName();
            String host = node.getNodeHost();
            int port = node.getNodePort();
            BigInteger hashStart = node.getNodeHashRange()[0];
            BigInteger hashStop = node.getNodeHashRange()[1];
            Metadata nodeMetadata = new Metadata(host, port, hashStart, hashStop);
            allMetadata.put(name, nodeMetadata);
        }

        DebugHelper.logFuncExit(logger);

        return allMetadata;
    }

    private int getHashRingSize() {
        return hashRing.size();
    }

    public HashMap<BigInteger, ECSNode> getHashRing() {
        return this.hashRing;
    }

    public void printHashRingStatus() {
        DebugHelper.logFuncEnter(logger);
        final String FANCY_TEXT = "=======================================";
        System.out.println(FANCY_TEXT);

        for (Map.Entry<BigInteger, ECSNode> set : hashRing.entrySet()) {
            BigInteger nodeID = set.getKey();
            ECSNode node = set.getValue();

            System.out.println(String.format("ID: %d", node.getNodeID()));
            System.out.println(String.format("Name: %s", node.getNodeName()));
            System.out.println(String.format("Host: %s", node.getNodeHost()));
            System.out.println(String.format("Port: %d", node.getNodePort()));
            BigInteger prevNodeID = node.getPrevNodeID();
            String prevNodeName = hashRing.get(prevNodeID).getNodeName();
            System.out.println(String.format("prevNode: %s, %d", prevNodeName, prevNodeID));
            BigInteger nextNodeID = node.getNextNodeID();
            String nextNodeName = hashRing.get(nextNodeID).getNodeName();
            System.out.println(String.format("nextNode: %s, %d", nextNodeName, nextNodeID));
            System.out.println();
        }

        System.out.println(FANCY_TEXT);
        DebugHelper.logFuncExit(logger);
    }
}
