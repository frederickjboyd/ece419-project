package ecs;

import java.math.BigInteger;

public interface IECSNode {
    public enum NodeStatus {
        OFFLINE, // Node in ECS config, but not added
        IDLE, // Node added, but not started
        ONLINE // Node added and started
    }

    /**
     * @return the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return array of two strings representing the low and high range of the
     *         hashes that the given node is responsible for
     */
    public BigInteger[] getNodeHashRange();

}
