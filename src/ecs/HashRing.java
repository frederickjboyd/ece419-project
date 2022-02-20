package ecs;

import org.apache.log4j.Logger;

import shared.DebugHelper;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    public void initHashRing(String[] serverInfo) {
        DebugHelper.logFuncEnter(logger);
        int hashRingSize = serverInfo.length;

        for (int i = 0; i < hashRingSize; i++) {
            String info = serverInfo[i];
            ECSNode node = createECSNode(info);
            hashRing.put(node.getNodeID(), node);
        }

        List<BigInteger> nodeIDsList = getSortedNodeIDs();

        for (int i = 0; i < hashRingSize; i++) {
            setNodeHashRange(i, nodeIDsList, hashRingSize);
        }

        DebugHelper.logFuncExit(logger);
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

    public ECSNode createECSNode(String info) {
        DebugHelper.logFuncEnter(logger);
        String[] infoArray = info.split(":");
        String name = infoArray[0];
        String host = infoArray[1];
        int port = Integer.parseInt(infoArray[2]);
        String infoToHash = String.format("%s:%s", host, port);
        BigInteger nodeID = hashServerInfo(infoToHash);
        logger.debug(String.format("ID: %d, name: %s, host: %s, port: %d", nodeID, name, host, port));

        ECSNode node = new ECSNode(nodeID, name, host, port);
        DebugHelper.logFuncExit(logger);

        return node;
    }

    public void addNode(ECSNode node) {
        DebugHelper.logFuncEnter(logger);
        BigInteger newNodeID = node.getNodeID();
        List<BigInteger> nodeIDsList = getSortedNodeIDs();
        int hashRingSize = getHashRingSize();
        int newNodeIdx = -1;

        // TODO: not sure if this logic makes sense
        // Wouldn't any node be inserted immediately (has to be < or >)
        for (int i = 0; i < hashRingSize; i++) {
            BigInteger currNodeID = nodeIDsList.get(i);
            int isNewNodeIDLarger = newNodeID.compareTo(currNodeID);

            if (isNewNodeIDLarger == -1) {
                newNodeIdx = i;
                break;
            } else if (isNewNodeIDLarger == 1) {
                newNodeIdx = i;
                break;
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

    public void removeNode(ECSNode node) {
        DebugHelper.logFuncEnter(logger);
        List<BigInteger> nodeIDsList = getSortedNodeIDs();
        int currNodeIdx = nodeIDsList.indexOf(node.getNodeID());

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

    /**
     * Get all node IDs from the hash ring and sort in ascending order.
     * 
     * @return Sorted list of node IDs.
     */
    private List<BigInteger> getSortedNodeIDs() {
        Set<BigInteger> nodeIDsSet = this.hashRing.keySet();
        List<BigInteger> nodeIDsList = new ArrayList<BigInteger>(nodeIDsSet);
        Collections.sort(nodeIDsList);

        return nodeIDsList;
    }

    private int getHashRingSize() {
        return hashRing.size();
    }

    public HashMap<BigInteger, ECSNode> getHashRing() {
        return this.hashRing;
    }
}