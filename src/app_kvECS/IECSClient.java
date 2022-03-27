package app_kvECS;

import java.util.Map;

import app_kvServer.IKVServer.CacheStrategy;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;

import ecs.ECSNode;

public interface IECSClient {
    /**
     * Create a new KVServer with the specified cache size and replacement strategy
     * and add it to the storage service at an arbitrary position.
     * 
     * @return name of new server
     */
    public ECSNode addNode(String cacheStrategy, int cacheSize);

    /**
     * Randomly choose <numberOfNodes> servers from the available machines and start
     * the KVServer by issuing an SSH call to the respective machine.
     * This call launches the storage server with the specified cache size and
     * replacement strategy. For simplicity, locate the KVServer.jar in the
     * same directory as the ECS. All storage servers are initialized with the
     * metadata and any persisted data, and remain in state stopped.
     * 
     * NOTE: Must call setupNodes before the SSH calls to start the servers and must
     * call awaitNodes before returning
     * 
     * @return set of strings containing the names of the nodes
     */
    public Collection<ECSNode> addNodes(int count, String cacheStrategy, int cacheSize);

    /**
     * Sets up `count` servers with the ECS (in this case Zookeeper)
     * 
     * @return array of strings, containing unique names of servers
     */
    public Collection<ECSNode> setupNodes(int count, CacheStrategy cacheStrategy, int cacheSize,
            List<String> serversToSetup);

    /**
     * Wait for all nodes to report status or until timeout expires
     * 
     * @param count   number of nodes to wait for
     * @param timeout the timeout in milliseconds
     * @return true if all nodes reported successfully, false otherwise
     */
    public boolean awaitNodes(int count, int timeout) throws Exception;

    /**
     * Starts the storage service by calling start() on all KVServer instances that
     * participate in the service.
     * 
     * @throws Exception some meaningfull exception on failure
     * @return true on success, false on failure
     */
    public boolean start() throws Exception;

    /**
     * Stops the service; all participating KVServers are stopped for processing
     * client requests but the processes remain running.
     * 
     * @throws Exception some meaningfull exception on failure
     * @return true on success, false on failure
     */
    public boolean stop() throws Exception;

    /**
     * Stops all server instances and exits the remote processes.
     * 
     * @throws Exception some meaningfull exception on failure
     * @return true on success, false on failure
     */
    public boolean shutdown() throws Exception;

    /**
     * Remove a node, re-calculate hash ring values, and update metadata on
     * remaining nodes.
     * 
     * @param nodeNames List of nodes to be removed with each entry in a
     *                  serverName:ip:port format
     * @param isFailure True if this node if being removed after failing, false
     *                  otherwise
     * @return True on success, false otherwise
     */
    public boolean removeNodes(List<String> nodeNames, boolean isFailure);

    /**
     * Get a map of all nodes
     */
    public Map<BigInteger, ECSNode> getNodes();

    /**
     * Get the specific node responsible for the given key
     */
    public ECSNode getNodeByKey(BigInteger Key);
}
