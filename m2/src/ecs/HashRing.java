package ecs;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvECS.ECSClient;
import shared.DebugHelper;
import shared.Metadata;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class HashRing {
    private static Logger logger = ECSClient.logger;

    private HashMap<BigInteger, ECSNode> hashRing = new HashMap<>(); // MD5 hash -> node

    public HashRing() {
        DebugHelper.logFuncEnter(logger);
        DebugHelper.logFuncExit(logger);
    }

    /**
     * Initialize hash ring.
     * 
     * @param serverInfo All server information, each in a name:ip:port format
     */
    public List<ECSNode> initHashRing(List<String> serverInfo, CacheStrategy cacheStrategy, int cacheSize) {
        DebugHelper.logFuncEnter(logger);
        int hashRingSize = serverInfo.size();
        List<ECSNode> nodesAdded = new ArrayList<ECSNode>();
        logger.debug(String.format("hashRingSize: %d", hashRingSize));

        // Add each node to hash ring
        for (int i = 0; i < hashRingSize; i++) {
            String info = serverInfo.get(i);
            ECSNode node = createECSNode(info, cacheStrategy, cacheSize);
            logger.debug(String.format("Adding to hash ring: %d", node.getNodeID()));
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
        logger.debug(String.format("currNodeIdx: %d", currNodeIdx));
        logger.debug(String.format("hashRingSize: %d", hashRingSize));
        BigInteger currNodeID = nodeIDsList.get(currNodeIdx);
        logger.debug(String.format("currNodeID: %d", currNodeID));

        // Calculate hash range
        int prevNodeIdx = (currNodeIdx == 0) ? hashRingSize - 1 : currNodeIdx - 1;
        int nextNodeIdx = (currNodeIdx + 1) % hashRingSize;
        BigInteger nextNodeID = nodeIDsList.get(nextNodeIdx);
        BigInteger[] hashRange = { currNodeID, nextNodeID };
        logger.debug(String.format("prev/nextNodeIdx: %s, %s", prevNodeIdx, nextNodeIdx));
        logger.debug(String.format("nextNodeID: %s", nextNodeID));
        logger.debug(String.format("hashRange: %d-%d", hashRange[0], hashRange[1]));

        // Set ring position and hash range info
        ECSNode currNode = hashRing.get(currNodeID);
        BigInteger prevNodeID = nodeIDsList.get(prevNodeIdx);
        logger.debug(String.format("currNode: %s", currNode));
        logger.debug(String.format("prevNodeID: %d", prevNodeID));
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
        boolean added = false;

        for (int i = 0; i < hashRingSize; i++) {
            BigInteger currNodeID = nodeIDsList.get(i);
            int isNewNodeIDLarger = newNodeID.compareTo(currNodeID);

            if (isNewNodeIDLarger == -1) {
                // newNodeID < currNodeID;
                newNodeIdx = i;
                nodeIDsList.add(newNodeIdx, newNodeID);
                added = true;
                break;
            } else if (isNewNodeIDLarger == 1) {
                // newNodeID > currNodeID;
                continue;
            } else {
                logger.error(String.format("Found two nodes with the same ID: %d, %d", newNodeID, currNodeID));
                break;
            }
        }

        // Handle case where new node's ID is larger than all existing IDs in hash ring
        if (!added) {
            BigInteger currNodeID = nodeIDsList.get(hashRingSize - 1);
            int isNewNodeIDLarger = newNodeID.compareTo(currNodeID);

            if (isNewNodeIDLarger == 1) {
                newNodeIdx = hashRingSize;
                nodeIDsList.add(newNodeIdx, newNodeID);
            } else {
                logger.error(
                        "Check placement of node IDs in hash ring. Seems like current node's ID is both larger and smaller than existing nodes.");
            }
        }

        hashRing.put(newNodeID, node);
        setNodeHashRange(newNodeIdx, nodeIDsList, hashRingSize + 1);

        // Make previous and next nodes point to current node
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

    public HashMap<String, Metadata> removeNode(String serverInfo) {
        DebugHelper.logFuncEnter(logger);
        String[] serverInfoArray = serverInfo.split(":");
        String host = serverInfoArray[1];
        int port = Integer.parseInt(serverInfoArray[2]);

        String infoToHash = createStringToHash(host, port);
        BigInteger nodeID = hashServerInfo(infoToHash);
        ECSNode node = hashRing.get(nodeID);
        HashMap<String, Metadata> allMetadataOld = removeNode(node);
        DebugHelper.logFuncExit(logger);

        return allMetadataOld;
    }

    public HashMap<String, Metadata> removeNode(ECSNode node) {
        DebugHelper.logFuncEnter(logger);
        ECSNode prevNode = hashRing.get(node.getPrevNodeID());
        BigInteger prevNodeID = node.getPrevNodeID();
        ECSNode nextNode = hashRing.get(node.getNextNodeID());
        BigInteger nextNodeID = node.getNextNodeID();
        logger.debug(String.format("nextNode: %s:%s:%s", nextNode.getNodeName(), nextNode.getNodeHost(),
                nextNode.getNodePort()));
        // Next server is now responsible for current node's hash range
        BigInteger[] nextNodeHashRange = nextNode.getNodeHashRange();
        logger.debug(String.format("nextNode old hash range: %s", Arrays.toString(nextNodeHashRange)));
        nextNodeHashRange[0] = node.getNodeHashRange()[0];
        logger.debug(String.format("nextNode new hash range: %s", Arrays.toString(nextNodeHashRange)));

        prevNode.setNextNodeID(nextNodeID);
        nextNode.setPrevNodeID(prevNodeID);
        nextNode.setNodeHashRange(nextNodeHashRange);
        // To notify current server to transfer all data
        BigInteger[] nodeNewHashRange = { BigInteger.valueOf(0), BigInteger.valueOf(0) };
        node.setNodeHashRange(nodeNewHashRange);
        // Grab metadata before removing node so it knows what to update
        HashMap<String, Metadata> allMetadataOld = getAllMetadata();

        hashRing.remove(node.getNodeID());
        DebugHelper.logFuncExit(logger);
        return allMetadataOld;
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

    public String createStringToHash(String host, int port) {
        return host + ":" + port;
    }

    /**
     * Get all node IDs from the hash ring and sort in ascending order.
     * 
     * @return Sorted list of node IDs.
     */
    public List<BigInteger> getSortedNodeIDs() {
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
            String host = node.getNodeHost();
            int port = node.getNodePort();
            BigInteger hashStart = node.getNodeHashRange()[0];
            BigInteger hashStop = node.getNodeHashRange()[1];
            CacheStrategy cacheStrategy = node.getCacheStrategy();
            int cacheSize = node.getCacheSize();
            BigInteger prevNodeID = node.getPrevNodeID();
            BigInteger nextNodeID = node.getNextNodeID();
            ECSNode prevNode = hashRing.get(prevNodeID);
            ECSNode nextNode = hashRing.get(nextNodeID);
            Metadata nodeMetadata = new Metadata(host, port, hashStart, hashStop, cacheStrategy, cacheSize, prevNode,
                    nextNode);
            String hostAndPort = String.format("%s:%s", host, port);
            allMetadata.put(hostAndPort, nodeMetadata);
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
        // Use TreeSet to order IDs
        TreeSet<BigInteger> nodeIDsSet = new TreeSet<BigInteger>(hashRing.keySet());
        System.out.println(FANCY_TEXT);
        System.out.println();

        for (BigInteger nodeID : nodeIDsSet) {
            ECSNode node = hashRing.get(nodeID);

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
            System.out.println(String.format("hashRange: %s", Arrays.toString(node.getNodeHashRange())));
            System.out.println();
        }

        System.out.println(FANCY_TEXT);
        DebugHelper.logFuncExit(logger);
    }
}
