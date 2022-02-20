package ecs;

import org.apache.log4j.Logger;

import java.math.BigInteger;

import ecs.IECSNode;
import ecs.IECSNode.NodeStatus;

public class ECSNode implements IECSNode {
    private BigInteger ID; // MD5 hash of <ip>:<port>
    private String name; // Name of server provided in ECS config file
    private String host; // IP
    private int port; // Port
    private BigInteger prevNodeID; // MD5 hash of previous node in ring
    private BigInteger nextNodeID; // MD5 hash of next node in ring
    private BigInteger[] hashRange; // Range of hashes the node is responsible for

    private static Logger logger = Logger.getRootLogger();

    private NodeStatus status;

    public ECSNode(BigInteger ID, String name, String host, int port, BigInteger prevNodeID, BigInteger nextNodeID,
            BigInteger[] hashRange) {
        this.ID = ID;
        this.name = name;
        this.host = host;
        this.port = port;
        this.prevNodeID = prevNodeID;
        this.nextNodeID = nextNodeID;
        this.hashRange = hashRange;

        logger.debug("Instantiate ECSNode with:");
        logger.debug(String.format("\tID:\t\t%d", this.ID));
        logger.debug(String.format("\tName:\t\t%s", this.name));
        logger.debug(String.format("\tHost:\t\t%s", this.host));
        logger.debug(String.format("\tPort:\t\t%d", this.port));
        logger.debug(String.format("\tprevNodeID:\t%d", this.prevNodeID));
        logger.debug(String.format("\tnextNodeID:\t%d", this.nextNodeID));
        logger.debug(String.format("\thashRange[0]:\t%d", this.hashRange[0]));
        logger.debug(String.format("\thashRange[1]:\t%d", this.hashRange[1]));
    }

    public ECSNode(BigInteger ID, String name, String host, int port) {
        this.ID = ID;
        this.name = name;
        this.host = host;
        this.port = port;

        logger.debug("Instantiate ECSNode with:");
        logger.debug(String.format("\tID:\t\t%d", this.ID));
        logger.debug(String.format("\tName:\t\t%s", this.name));
        logger.debug(String.format("\tHost:\t\t%s", this.host));
        logger.debug(String.format("\tPort:\t\t%d", this.port));
    }

    public String getNodeName() {
        return this.name;
    }

    // @Override
    public BigInteger getNodeID() {
        return this.ID;
    }

    // @Override
    public String getNodeHost() {
        return this.host;
    }

    // @Override
    public int getNodePort() {
        return this.port;
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

    // @Override
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
