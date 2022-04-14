package ecs;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.CacheStrategy;
import shared.DebugHelper;

import java.math.BigInteger;

public class ECSNode {
    private BigInteger ID; // MD5 hash of <ip>:<port>
    private String name; // Name of server provided in ECS config file
    private String host; // IP
    private int port; // Port
    private CacheStrategy cacheStrategy; // Cache method to use
    private int cacheSize; // Size of cache
    private BigInteger prevNodeID; // MD5 hash of previous node in ring
    private BigInteger nextNodeID; // MD5 hash of next node in ring
    private BigInteger[] hashRange; // Range of hashes the node is responsible for

    private static Logger logger = Logger.getRootLogger();

    private NodeStatus status;

    public enum NodeStatus {
        OFFLINE, // Node in ECS config, but not added
        IDLE, // Node added, but not started
        ONLINE, // Node added and started
        FAILED // Node crashed - should not be used for remainder of session
    }

    public ECSNode(BigInteger ID, String name, String host, int port, CacheStrategy cacheStrategy, int cacheSize) {
        DebugHelper.logFuncEnter(logger);
        this.ID = ID;
        this.name = name;
        this.host = host;
        this.port = port;
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;

        logger.debug("Instantiate ECSNode with:");
        logger.debug(String.format("\tID:\t\t%d", this.ID));
        logger.debug(String.format("\tName:\t\t%s", this.name));
        logger.debug(String.format("\tHost:\t\t%s", this.host));
        logger.debug(String.format("\tPort:\t\t%d", this.port));
        logger.debug(String.format("\tCache:\t\t%s, %d", this.cacheStrategy.toString(), this.cacheSize));
        DebugHelper.logFuncExit(logger);
    }

    /**
     * @return Name of the node (e.g. server1)
     */
    public String getNodeName() {
        return this.name;
    }

    public BigInteger getNodeID() {
        return this.ID;
    }

    /**
     * @return Hostname of the node (e.g. 127.0.0.1)
     */
    public String getNodeHost() {
        return this.host;
    }

    /**
     * @return Port number of the node (e.g. 8080)
     */
    public int getNodePort() {
        return this.port;
    }

    public CacheStrategy getCacheStrategy() {
        return this.cacheStrategy;
    }

    public int getCacheSize() {
        return this.cacheSize;
    }

    public BigInteger getPrevNodeID() {
        return this.prevNodeID;
    }

    public void setPrevNodeID(BigInteger newPrevNodeID) {
        this.prevNodeID = newPrevNodeID;
    }

    public BigInteger getNextNodeID() {
        return this.nextNodeID;
    }

    public void setNextNodeID(BigInteger newNextNodeID) {
        this.nextNodeID = newNextNodeID;
    }

    /**
     * @return Array of two numbers representing the low and high range of the
     *         hashes that the given node is responsible for
     */
    public BigInteger[] getNodeHashRange() {
        return this.hashRange;
    }

    public void setNodeHashRange(BigInteger[] newHashRange) {
        this.hashRange = newHashRange;
    }

    /**
     * If this node is directly before a new node that is being added to the hash
     * ring, update its data
     * 
     * @param newNextNodeID
     */
    public void updateNodeIfBefore(BigInteger newNextNodeID) {
        this.hashRange[1] = newNextNodeID;
        this.nextNodeID = newNextNodeID;
    }

    /**
     * If this node is directly after a new node that is being added to the has
     * ring, update its data
     * 
     * @param newPrevNodeID
     */
    public void updateNodeIfAfter(BigInteger newPrevNodeID) {
        this.prevNodeID = newPrevNodeID;
    }

    public void setNodeStatus(NodeStatus newStatus) {
        this.status = newStatus;
    }

    public NodeStatus getNodeStatus() {
        return this.status;
    }
}
