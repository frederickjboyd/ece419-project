package app_kvECS;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;

import app_kvServer.IKVServer.CacheStrategy;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import ecs.ECSNode;
import ecs.ECSNode.NodeStatus;
import ecs.HashRing;
import logger.LogSetup;
import shared.communication.AdminMessage;
import shared.communication.AdminMessage.MessageType;
import shared.DebugHelper;
import shared.Metadata;

/**
 * Main ECS client that user interacts with to manage servers.
 */
public class ECSClient implements IECSClient {
    // Prevent external libraries from spamming console
    public static Logger logger = Logger.getLogger(ECSClient.class);
    private static Level logLevel = Level.ERROR;
    private List<Integer> initialJavaPIDs;

    private boolean running = false;
    private boolean zkServerRunning = false;
    private static final String PROMPT = "ECS> ";
    public HashMap<String, NodeStatus> serverStatusInfo = new HashMap<String, NodeStatus>();
    private HashRing hashRing;
    private List<String> unavailableServers = new ArrayList<String>(); // Any servers that are not OFFLINE
    private HashMap<String, Process> runningServers = new HashMap<String, Process>();

    public static final String ZK_ROOT_PATH = "/zkRoot";
    private static final String ZK_CONF_PATH = "zoo.cfg";
    private static final String SERVER_DIR = "~/ece419-project";
    private static final String SERVER_JAR = "m3-server.jar";
    private static final int ZK_PORT = 2181;
    private static final String ZK_HOST = "localhost";
    private static final int ZK_TIMEOUT = 2000;
    private Thread zkServer;
    private ZooKeeperConnectedWatcher zkWatcher;
    private ZooKeeper zk;

    public ECSClient(String configPath) {
        DebugHelper.logFuncEnter(logger);
        logger.setLevel(logLevel);

        // Track current running Java PIDs
        // Only want to kill new Java processes that have been created after ECSClient
        // launch (i.e. servers)
        initialJavaPIDs = getJavaPIDs();

        try {
            // Read configuration file
            BufferedReader reader = new BufferedReader(new FileReader(configPath));
            String l;
            logger.info(String.format("Reading configuration file at: %s", configPath));

            while ((l = reader.readLine()) != null) {
                String[] config = l.split("\\s+", 3);
                logger.trace(String.format("%s => %s:%s", config[0], config[1], config[2]));
                serverStatusInfo.put(String.format("%s:%s:%s", config[0], config[1], config[2]), NodeStatus.OFFLINE);
            }

            // Initialize hash ring
            hashRing = new HashRing();

            // Initialize ZooKeeper server
            zkServer = new Thread(new ZooKeeperServer());
            zkServer.start();

            // Initialize ZooKeeper client
            CountDownLatch latch = new CountDownLatch(1);
            zkWatcher = new ZooKeeperConnectedWatcher(latch);
            String connectString = String.format("%s:%s", ZK_HOST, ZK_PORT);
            zk = new ZooKeeper(connectString, ZK_TIMEOUT, zkWatcher);
            latch.await(); // Wait for client to initialize

            // Create root storage server
            if (zk.exists(ZK_ROOT_PATH, false) == null) {
                zk.create(ZK_ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info(String.format("Created ZooKeeper root: %s", ZK_ROOT_PATH));
            }
        } catch (Exception e) {
            logger.error("Unable to initialize ECSClient.");
            e.printStackTrace();
        }

        running = true;
        DebugHelper.logFuncExit(logger);
    }

    /**
     * Class to run ZooKeeper in a separate thread.
     */
    private class ZooKeeperServer implements Runnable {
        public ZooKeeperServer() {
            zkServerRunning = true;
        }

        public void run() {
            while (zkServerRunning) {
                try {
                    // Initialize ZooKeeper
                    ServerConfig zkServerCfg = new ServerConfig();
                    zkServerCfg.parse(ZK_CONF_PATH);
                    ZooKeeperServerMain zkServerMain = new ZooKeeperServerMain();
                    zkServerMain.runFromConfig(zkServerCfg);
                } catch (Exception e) {
                    logger.error("Unable to initialize ZooKeeper server.");
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Class that signals when ZooKeeper client has successfully started.
     */
    private class ZooKeeperConnectedWatcher implements Watcher {
        private CountDownLatch latch;

        public ZooKeeperConnectedWatcher(CountDownLatch clientLatch) {
            this.latch = clientLatch;
        }

        public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.SyncConnected) {
                latch.countDown();
            }
        }
    }

    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize, boolean isFailure) {
        DebugHelper.logFuncEnter(logger);
        List<ECSNode> nodesAdded = addNodes(1, cacheStrategy, cacheSize, isFailure);
        DebugHelper.logFuncExit(logger);

        return nodesAdded.get(0);
    }

    public List<ECSNode> addNodes(int count, String cacheStrategyStr, int cacheSize) {
        return addNodes(count, cacheStrategyStr, cacheSize, false);
    }

    @Override
    public List<ECSNode> addNodes(int count, String cacheStrategyStr, int cacheSize, boolean isFailure) {
        DebugHelper.logFuncEnter(logger);

        if (unavailableServers.size() == serverStatusInfo.size()) {
            logger.error("Cannot add any new nodes. All servers are currently in use.");
            return null;
        }

        int numAvailableServers = serverStatusInfo.size() - unavailableServers.size();

        if (count > numAvailableServers) {
            logger.error(String.format("Specified count, %d, is greater than the number of available servers, %d.",
                    count, numAvailableServers));
        }

        if (!isServerCountValid(count)) {
            return null;
        }

        CacheStrategy cacheStrategyEnum = CacheStrategy.valueOf(cacheStrategyStr.toUpperCase());
        List<ECSNode> nodesAdded = new ArrayList<ECSNode>();
        List<String> serverInfoAdded = new ArrayList<String>();
        List<String> availableServers = getAvailableServers();
        List<ECSNode> serversToAdd = new ArrayList<ECSNode>();

        for (int i = 0; i < count; i++) {
            String newServerInfo = availableServers.get(0);
            logger.debug(String.format("Adding server: %s", newServerInfo));
            serverStatusInfo.put(newServerInfo, NodeStatus.IDLE);
            unavailableServers.add(newServerInfo);
            availableServers.remove(newServerInfo);
            serverInfoAdded.add(newServerInfo);

            // Add to hash ring
            if (hashRing.getHashRing().isEmpty()) {
                List<String> newServerInfoList = new ArrayList<String>();
                newServerInfoList.add(newServerInfo);
                // Creates and adds node to hash ring
                nodesAdded.addAll(hashRing.initHashRing(newServerInfoList, cacheStrategyEnum, cacheSize));
            } else {
                ECSNode newNode = hashRing.createECSNode(newServerInfo, cacheStrategyEnum, cacheSize);
                hashRing.addNode(newNode);
                nodesAdded.add(newNode);
            }

            // Start KVServer via SSH
            String cd = String.format(String.format("cd %s;", SERVER_DIR));
            String newServerHostAndPort = getHostAndPort(newServerInfo);
            String sshRunServerJar = String.format("java -jar %s/%s %s %s %s", SERVER_DIR, SERVER_JAR,
                    newServerHostAndPort, ZK_PORT, ZK_HOST);
            String sshNohup = String.format("nohup %s > logs/%s.out &", sshRunServerJar, newServerInfo);
            String sshStart = String.format("ssh -o StrictHostKeyChecking=no -n %s %s %s", ZK_HOST, cd, sshNohup);
            logger.info(String.format("Executing command: %s", sshStart));

            try {
                Process p = Runtime.getRuntime().exec(sshStart);
                // Keep track so we can properly terminate later
                logger.debug(String.format("Adding process %s to runningServers", p));
                runningServers.put(newServerInfo, p);
            } catch (Exception e) {
                logger.error("Unable to start or connect to KVServer.");
                e.printStackTrace();
            }

            awaitTime(25);
        }

        try {
            awaitNodes(count, ZK_TIMEOUT);
        } catch (Exception e) {
            logger.error("awaitNodes failed");
        }

        setupNodes(count, cacheStrategyEnum, cacheSize, serverInfoAdded, isFailure);

        // Set up ZooKeeper watcher to detect when a node goes offline
        for (String serverInfo : serverInfoAdded) {
            handleNodeCrash(serverInfo);
        }

        DebugHelper.logFuncExit(logger);

        return nodesAdded;
    }

    @Override
    public Collection<ECSNode> setupNodes(int count, CacheStrategy cacheStrategy, int cacheSize,
            List<String> serversToSetup, boolean isFailure) {
        DebugHelper.logFuncEnter(logger);

        if (!isServerCountValid(count)) {
            return null;
        }

        for (String serverInfo : serversToSetup) {
            String zkNodePath = buildZkNodePath(serverInfo);
            HashMap<String, Metadata> allMetadata = hashRing.getAllMetadata();
            AdminMessage msg = new AdminMessage(MessageType.INIT, allMetadata);

            try {
                logger.debug(String.format("Setting up node at path: %s", zkNodePath));

                while (zk.exists(zkNodePath, false) == null) {
                    awaitNodes(count, ZK_TIMEOUT);
                }

                logger.trace(String.format("Setting data to node %s:", zkNodePath));
                logger.trace(String.format("zk: %s", zk));
                if (zk.exists(zkNodePath, false) != null) {
                    zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
                }
            } catch (Exception e) {
                logger.error("Unable to send admin message to node.");
                e.printStackTrace();
            }
        }

        // Update metadata on pre-existing servers
        List<String> serversToUpdate = new ArrayList<String>(unavailableServers);
        logger.info(String.format("unavailableServers: %s", serversToUpdate));
        serversToUpdate.removeAll(serversToSetup);

        if (isFailure) {
            awaitTime(40000);
        }

        updateNodeMetadata(serversToUpdate);

        DebugHelper.logFuncExit(logger);

        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        DebugHelper.logFuncEnter(logger);
        boolean result = false;
        CountDownLatch latch = new CountDownLatch(timeout);

        try {
            result = latch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Unable to wait for all nodes.");
            e.printStackTrace();
        }

        DebugHelper.logFuncExit(logger);
        return result;
    }

    public void awaitTime(int timeout) {
        DebugHelper.logFuncEnter(logger);
        CountDownLatch latch = new CountDownLatch(timeout);

        try {
            latch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Error during await");
        }

        DebugHelper.logFuncExit(logger);
    }

    private void handleNodeCrash(String serverInfo) {
        DebugHelper.logFuncEnter(logger);
        String zkNodePath = buildZkNodePath(serverInfo);

        try {
            if (running) {
                zk.exists(zkNodePath, new ZooKeeperNodeDeletedWatcher(serverInfo));
            }
        } catch (Exception e) {
            logger.error("Unable to configure node crash detection");
            e.printStackTrace();
        }

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Class that signals when a node has failed.
     */
    private class ZooKeeperNodeDeletedWatcher implements Watcher {
        String serverInfo;

        /**
         * Constructor.
         * 
         * @param serverInfo serverName:ip:port
         */
        public ZooKeeperNodeDeletedWatcher(String serverInfo) {
            this.serverInfo = serverInfo;
            logger.debug(String.format("Creating new watcher for %s", serverInfo));
        }

        public void process(WatchedEvent event) {
            try {
                if (event.getType() == EventType.NodeDeleted
                        && serverStatusInfo.get(serverInfo) != NodeStatus.OFFLINE) {
                    System.out.println();
                    logger.error(String.format("Server %s failed!", serverInfo));
                    // Remove failed node
                    ECSNode failedNode = hashRing.getNodeByServerInfo(serverInfo);
                    CacheStrategy oldCacheStrategy = failedNode.getCacheStrategy();
                    int oldCacheSize = failedNode.getCacheSize();
                    unavailableServers.remove(serverInfo);
                    NodeStatus oldStatus = serverStatusInfo.get(serverInfo);
                    List<String> failedNodeList = new ArrayList<String>();
                    logger.debug(String.format("Old cache: %s, %d", oldCacheStrategy.toString(), oldCacheSize));
                    logger.debug(String.format("Old status: %s", oldStatus.toString()));
                    failedNodeList.add(serverInfo);
                    removeNodes(failedNodeList, true);
                    // Start up replacement
                    addNode(oldCacheStrategy.toString(), oldCacheSize, true);

                    if (oldStatus == NodeStatus.ONLINE) {
                        start();
                    }
                } else {
                    handleNodeCrash(serverInfo);
                }

                System.out.print(PROMPT);
            } catch (Exception e) {
                logger.error("Unable to handle ZooKeeper event");
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean start() throws Exception {
        DebugHelper.logFuncEnter(logger);
        AdminMessage msg = new AdminMessage(MessageType.START);

        for (String serverInfo : unavailableServers) {
            serverStatusInfo.put(serverInfo, NodeStatus.ONLINE);
            String zkNodePath = buildZkNodePath(serverInfo);

            try {
                while (zk.exists(zkNodePath, false) == null) {
                    awaitNodes(1, ZK_TIMEOUT);
                }

                logger.debug(String.format("Setting data at %s: %s", zkNodePath, msg));
                if (zk.exists(zkNodePath, false) != null) {
                    zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
                }
                logger.debug("Done setting data");
            } catch (Exception e) {
                String errorMsg = String.format("Unable to start server: %s", serverInfo);
                logger.error(errorMsg);
                e.printStackTrace();
                throw new Exception(errorMsg);
            }
        }

        DebugHelper.logFuncExit(logger);

        return true;
    }

    @Override
    public boolean stop() throws Exception {
        DebugHelper.logFuncEnter(logger);
        AdminMessage msg = new AdminMessage(MessageType.STOP);

        for (String serverInfo : unavailableServers) {
            try {
                serverStatusInfo.put(serverInfo, NodeStatus.IDLE);
                String zkNodePath = buildZkNodePath(serverInfo);
                if (zk.exists(zkNodePath, false) != null) {
                    zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
                }
            } catch (Exception e) {
                String errorMsg = String.format("Unable to stop server: %s", serverInfo);
                logger.error(errorMsg);
                e.printStackTrace();
                throw new Exception(errorMsg);
            }
        }

        DebugHelper.logFuncExit(logger);

        return true;
    }

    @Override
    public boolean shutdown() throws Exception {
        DebugHelper.logFuncEnter(logger);
        AdminMessage msg = new AdminMessage(MessageType.SHUTDOWN);

        for (String serverInfo : unavailableServers) {
            try {
                serverStatusInfo.put(serverInfo, NodeStatus.OFFLINE);
                hashRing.removeNode(serverInfo);
                String zkNodePath = buildZkNodePath(serverInfo);
                if (zk.exists(zkNodePath, false) != null) {
                    zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
                }
                Process p = runningServers.remove(serverInfo);
                logger.debug(String.format("Destroying process %s", p));
                p.destroy();
            } catch (Exception e) {
                String errorMsg = String.format("Unable to shutdown server: %s", serverInfo);
                logger.error(errorMsg);
                e.printStackTrace();
                throw new Exception(errorMsg);
            }
        }

        unavailableServers.clear();
        DebugHelper.logFuncExit(logger);

        return true;
    }

    @Override
    public boolean removeNodes(List<String> nodeNames, boolean isFailure) {
        DebugHelper.logFuncEnter(logger);
        logger.info(String.format("Removing nodes: %s", nodeNames));
        logger.info(String.format("isFailure: %s", isFailure));

        for (String serverInfo : nodeNames) {
            try {
                NodeStatus newStatus = isFailure ? NodeStatus.FAILED : NodeStatus.OFFLINE;
                serverStatusInfo.put(serverInfo, newStatus);
                // Metadata still with node that is to be removed
                HashMap<String, Metadata> allMetadataOld = hashRing.removeNode(serverInfo);

                // Can't send message to failed node
                if (!isFailure) {
                    // Sent updated hash range to server
                    AdminMessage msg = new AdminMessage(MessageType.UPDATE, allMetadataOld);
                    String zkNodePath = buildZkNodePath(serverInfo);

                    if (zk.exists(zkNodePath, false) != null) {
                        zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
                    }
                }

                unavailableServers.remove(serverInfo);
            } catch (Exception e) {
                logger.error(String.format("Unable to remove server: %s", serverInfo));
                e.printStackTrace();
                return false;
            }
        }

        // Update metadata on all remaining nodes
        updateNodeMetadata(unavailableServers);
        DebugHelper.logFuncExit(logger);

        return false;
    }

    /**
     * Notify servers of updated metadata.
     * 
     * @param nodesToUpdate Servers to update in format serverName:ip:port
     */
    private void updateNodeMetadata(List<String> serversToUpdate) {
        DebugHelper.logFuncEnter(logger);
        logger.info(String.format("Updating metadata on nodes: %s", serversToUpdate));
        HashMap<String, Metadata> allMetadata = hashRing.getAllMetadata();
        AdminMessage msg = new AdminMessage(MessageType.UPDATE, allMetadata);

        for (String serverInfo : serversToUpdate) {
            logger.debug(String.format("Sending UPDATE to node %s", serverInfo));
            String zkNodePath = buildZkNodePath(serverInfo);

            try {
                if (zk.exists(zkNodePath, false) != null) {
                    zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
                }
            } catch (Exception e) {
                logger.error(String.format("Unable to send message via ZooKeeper to update metadata on node %s",
                        zkNodePath));
                e.printStackTrace();
            }
        }

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Remove all *.out and *.log files from the log folder.
     */
    public void cleanLogs() {
        File dir = new File("logs");
        File fList[] = dir.listFiles();

        for (int i = 0; i < fList.length; i++) {
            String f = fList[i].toString();

            if (f.endsWith(".out") || f.endsWith(".log")) {
                boolean succ = new File(f).delete();

                if (!succ) {
                    logger.error(String.format("Unable to delete file %s", f));
                }
            }
        }
    }

    /**
     * Remove all *.properties files from the data folder.
     */
    public void cleanData() {
        File dir = new File("data");
        File fList[] = dir.listFiles();

        for (int i = 0; i < fList.length; i++) {
            String f = fList[i].toString();

            if (f.endsWith(".out") || f.endsWith(".properties")) {
                boolean succ = new File(f).delete();

                if (!succ) {
                    logger.error(String.format("Unable to delete file %s", f));
                }
            }
        }
    }

    /**
     * Stop/shutdown all nodes, threads, and ZooKeeper.
     */
    public void quit() {
        DebugHelper.logFuncEnter(logger);

        try {
            running = false;
            zkServerRunning = false;
            stop();
            shutdown();
            zk.close();
            zkServer.stop();
        } catch (Exception e) {
            logger.error("Unable to quit ECSClient");
            e.printStackTrace();
        }

        // Clean up ZooKeeper files
        // Get directory
        Properties prop = new Properties();
        try (FileInputStream fis = new FileInputStream(ZK_CONF_PATH)) {
            prop.load(fis);
        } catch (Exception e) {
            logger.error("Unable to clean up ZooKeeper files");
            e.printStackTrace();
        }

        // Delete contents
        String zkPath = prop.getProperty("dataDir");
        File zkDir = new File(zkPath);
        deleteDirectory(zkDir);

        // Manually kill all new "java" processes
        List<Integer> javaPIDs = getJavaPIDs();

        // Construct kill command
        StringBuilder killCmd = new StringBuilder();
        killCmd.append("kill ");

        for (Integer pid : javaPIDs) {
            if (!initialJavaPIDs.contains(pid)) {
                killCmd.append(pid);
                killCmd.append(" ");
            }
        }

        killCmd.append("&");

        // Execute
        try {
            logger.info("Cleaning up Java programs");
            logger.info(killCmd.toString());
            Process p = Runtime.getRuntime().exec(killCmd.toString());
        } catch (Exception e) {
            logger.error("Unable to clean up Java programs");
            e.printStackTrace();
        }

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Recursively delete all contents of a directory.
     * 
     * @param f
     */
    private void deleteDirectory(File f) {
        for (File subfile : f.listFiles()) {
            if (subfile.isDirectory()) {
                deleteDirectory(subfile);
            }

            subfile.delete();
        }
    }

    /**
     * Helper function to kill all Java processes, regardless of when they were
     * created.
     */
    private void killAllJavaPIDs() {
        List<Integer> javaPIDs = getJavaPIDs();
        StringBuilder killCmd = new StringBuilder();
        killCmd.append("kill ");

        for (Integer pid : javaPIDs) {
            killCmd.append(pid);
            killCmd.append(" ");
        }

        try {
            logger.info("Cleaning up all Java programs");
            logger.info(killCmd.toString());
            Process p = Runtime.getRuntime().exec(killCmd.toString());
        } catch (Exception e) {
            logger.error("Unable to clean up Java programs");
            e.printStackTrace();
        }
    }

    /**
     * Get list of current Java PIDs a user is running.
     * 
     * @return
     */
    public List<Integer> getJavaPIDs() {
        // Get username
        String homeDir = System.getProperty("user.home");
        String[] homeDirArray = homeDir.split("/");
        String username = homeDirArray[homeDirArray.length - 1];
        // Get all user-specific processes
        String cmd = String.format("ps -u %s", username);
        List<Integer> javaPrograms = null;

        try {
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader stdIn = new BufferedReader(new InputStreamReader(p.getInputStream()));
            javaPrograms = parseProcessList(stdIn);
        } catch (Exception e) {
            logger.error(String.format("Unable to execute or read output of %s", cmd));
            e.printStackTrace();
        }

        logger.debug(String.format("Current running Java processes: %s", javaPrograms));

        return javaPrograms;
    }

    /**
     * Parse list of processes a user is currently running and get the PIDs of all
     * Java programs.
     * 
     * @param stdIn
     * @return
     */
    private List<Integer> parseProcessList(BufferedReader stdIn) {
        List<Integer> javaPrograms = new ArrayList<Integer>();
        String line;

        try {
            while ((line = stdIn.readLine()) != null) {
                String[] psArray = line.split("\\s+");

                int pid = -1;
                String cmd;
                try {
                    if (psArray.length == 5) {
                        pid = Integer.parseInt(psArray[1]);
                        cmd = psArray[4];
                    } else {
                        pid = Integer.parseInt(psArray[0]);
                        cmd = psArray[3];
                    }
                } catch (Exception e) {
                    continue;
                }

                if (cmd.equals("java")) {
                    javaPrograms.add(pid);
                }
            }
        } catch (Exception e) {
            logger.error("Unable to parse list of processes");
            e.printStackTrace();
        }

        return javaPrograms;
    }

    @Override
    public HashMap<BigInteger, ECSNode> getNodes() {
        DebugHelper.logFuncEnter(logger);
        DebugHelper.logFuncExit(logger);
        return hashRing.getHashRing();
    }

    @Override
    public ECSNode getNodeByKey(BigInteger key) {
        DebugHelper.logFuncEnter(logger);
        HashMap<BigInteger, ECSNode> nodeMap = getNodes();
        DebugHelper.logFuncExit(logger);
        return nodeMap.get(key);
    }

    /**
     * Iterate through all possible servers and only return those that are offline.
     * 
     * @return List of servers that can be added
     */
    public List<String> getAvailableServers() {
        DebugHelper.logFuncEnter(logger);
        List<String> availableServers = new ArrayList<String>();

        for (Map.Entry<String, NodeStatus> set : serverStatusInfo.entrySet()) {
            String info = set.getKey();
            NodeStatus status = set.getValue();

            if (status == NodeStatus.OFFLINE) {
                availableServers.add(info);
            }
        }

        Collections.sort(availableServers);
        DebugHelper.logFuncExit(logger);

        return availableServers;
    }

    /**
     * Ensure number of servers user wishes to modify is valid.
     * 
     * @param count
     * @return
     */
    private boolean isServerCountValid(int count) {
        DebugHelper.logFuncEnter(logger);
        boolean isValid = true;

        if (count < 0 || count > serverStatusInfo.size()) {
            isValid = false;
            logger.error(String.format("Invalid count specified: %d", count));
        }

        DebugHelper.logFuncExit(logger);

        return isValid;
    }

    /**
     * Extract just the server host and port from server info.
     * 
     * @param serverInfo <serverName>:<ip>:<port>
     * @return ip:port
     */
    private String getHostAndPort(String serverInfo) {
        DebugHelper.logFuncEnter(logger);
        String[] serverInfoArray = serverInfo.split(":");
        String host = serverInfoArray[1];
        String port = serverInfoArray[2];
        String serverHostAndPort = String.format("%s:%s", host, port);
        DebugHelper.logFuncExit(logger);
        return serverHostAndPort;
    }

    /**
     * Build a ZooKeeper path for a node. Ensures compatibility between ECSClient
     * and other components.
     * 
     * @param serverInfo serverName:ip:port
     * @return
     */
    private String buildZkNodePath(String serverInfo) {
        DebugHelper.logFuncEnter(logger);
        String hostAndPort = getHostAndPort(serverInfo);
        String zkNodePath = String.format("%s/%s", ZK_ROOT_PATH, hostAndPort);
        logger.trace(String.format("Constructed zkNodePath: %s", zkNodePath));
        DebugHelper.logFuncExit(logger);
        return zkNodePath;
    }

    /**
     * Get system's local IP.
     */
    public String getLocalIP() {
        String ip = null;

        try {
            InetAddress addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress();
            logger.debug(String.format("Local IP: %s", ip));
        } catch (Exception e) {
            logger.error("Unable to get local IP");
            e.printStackTrace();
        }

        return ip;
    }

    private void handleCommand(String cmdLine) throws Exception {
        DebugHelper.logFuncEnter(logger);
        String[] tokens = cmdLine.split("\\s+");

        switch (tokens[0]) {
            case "addnodes":
                logger.info("Handling addnodes...");

                if (tokens.length != 4) {
                    System.out.println(
                            "Please specify the right number of arguments for this command: addNodes <count> <cacheStrategy> <cacheSize>");
                    break;
                }

                try {
                    int count = Integer.parseInt(tokens[1]);
                    String cacheStrategy = tokens[2];
                    int cacheSize = Integer.parseInt(tokens[3]);
                    addNodes(count, cacheStrategy, cacheSize, false);
                } catch (Exception e) {
                    throw new Exception("Unable to parse input.");
                }
                break;

            case "addnode":
                logger.info("Handling addnode...");

                if (tokens.length != 3) {
                    System.out.println(
                            "Please specify the right number of arguments for this command: addNodes <cacheStrategy> <cacheSize>");
                }

                try {
                    String cacheStrategy = tokens[1];
                    int cacheSize = Integer.parseInt(tokens[2]);
                    addNode(cacheStrategy, cacheSize, false);
                } catch (Exception e) {
                    throw new Exception("Unable to parse input.");
                }
                break;

            case "start":
                logger.info("Handling start...");
                start();
                break;

            case "stop":
                logger.info("Handling stop...");
                stop();
                break;

            case "shutdown":
                logger.info("Handling shutdown...");
                shutdown();
                break;

            case "removenode":
                logger.info("Handling removenode...");
                List<String> serversToRemove = new ArrayList<String>();

                for (int i = 1; i < tokens.length; i++) {
                    serversToRemove.add(tokens[i]);
                }

                removeNodes(serversToRemove, false);
                break;

            case "cleanlogs":
                logger.info("Handling cleanlogs...");
                cleanLogs();
                break;

            case "cleandata":
                logger.info("Handling cleandata...");
                cleanData();
                break;

            case "cleanall":
                logger.info("Handling cleanall...");
                cleanLogs();
                cleanData();
                break;

            case "killjava":
                logger.info("Handling killjava");
                killAllJavaPIDs();
                break;

            case "status":
                logger.info("Handling status...");
                hashRing.printHashRingStatus();
                break;

            case "logLevel":
                if (tokens.length == 2) {
                    String level = setLevel(tokens[1]);
                    if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                        printError("No valid log level!");
                        printPossibleLogLevels();
                    } else {
                        System.out.println(PROMPT +
                                "Log level changed to level " + level);
                    }
                } else {
                    printError("Invalid number of parameters!");
                }
                break;

            case "help":
                printHelp();
                break;

            case "quit":
                logger.info("Handling quit...");
                quit();
                break;

            default:
                System.out.println(String.format("Error! Unknown command: %s", cmdLine));
                printHelp();
                break;
        }

        DebugHelper.logFuncExit(logger);
    }

    private String setLevel(String levelString) {
        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT + "Possible log levels are:");
        System.out.println(PROMPT + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("addnodes <num> <cacheStrategy> <cacheSize>");
        sb.append("\t Choose <num> nodes from available machines and start them \n");

        sb.append(PROMPT).append("addnode <cacheStrategy> <cacheSize>");
        sb.append("\t Create new KVServer and add it to the storage service at an arbitrary position \n");

        sb.append(PROMPT).append("start");
        sb.append(
                "\t\t\t\t\t Starts the storage service by calling start() on all KVServer instances that participate in the service \n");

        sb.append(PROMPT).append("stop");
        sb.append(
                "\t\t\t\t\t Stops the service - all participating KVServers are stopped for processing client requests, but remain running \n");

        sb.append(PROMPT).append("shutdown");
        sb.append("\t\t\t\t\t Stops all server instances and exits the remote processes \n");

        sb.append(PROMPT).append("removenode <server1>:<ip>:<port> ...");
        sb.append("\t Remove a server from the storage service at an arbitrary position \n");

        sb.append(PROMPT).append("status");
        sb.append("\t\t\t\t\t Get the current status of all available servers \n");

        sb.append(PROMPT).append("cleanlogs");
        sb.append("\t\t\t\t\t Clean all *.out and *.log files from the logs folder \n");

        sb.append(PROMPT).append("cleandata");
        sb.append("\t\t\t\t\t Clean all *.properties files from the data folder \n");

        sb.append(PROMPT).append("cleanall");
        sb.append("\t\t\t\t\t Run all clean* commands \n");

        sb.append(PROMPT).append("killjava");
        sb.append("\t\t\t\t\t Kill all Java processes \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t\t\t changes the logLevel \n");

        sb.append(PROMPT).append("\t\t\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t\t\t Exits the program \n");
        System.out.println(sb.toString());
    }

    public void run() {
        DebugHelper.logFuncEnter(logger);

        while (running) {
            BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = input.readLine();
                try {
                    this.handleCommand(cmdLine);
                } catch (Exception e) {
                    logger.error(String.format("Unable to handle command: %s", cmdLine));
                    printError("Unknown command!");
                    printHelp();
                }
            } catch (IOException e) {
                running = false;
                printError("CLI does not respond - Application terminated ");
                logger.error("ECSClient: CLI not respond!");
            }
        }

        DebugHelper.logFuncExit(logger);
    }

    public static void main(String[] args) {
        try {
            // Leave this off unless you want to view external library logs (e.g. ZooKeeper)
            new LogSetup("logs/ecs.log", Level.OFF);

            if (args.length != 1) {
                System.out.println("Error! Please specify a configuration file!");
            } else {
                ECSClient ecs = new ECSClient(args[0]);
                ecs.run();
            }
        } catch (Exception e) {
            logger.error("Unable to initialize/run ECS client.");
            e.printStackTrace();
            System.exit(1);
        }

        System.exit(0);
    }
}
