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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
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
    private static Logger logger = Logger.getLogger(ECSClient.class);
    private static Level logLevel = Level.TRACE;

    private boolean running = false;
    private boolean zkServerRunning = false;
    private static final String PROMPT = "ECS> ";
    private HashMap<String, NodeStatus> serverStatusInfo = new HashMap<String, NodeStatus>();
    private HashRing hashRing;
    private ArrayList<String> unavailableServers = new ArrayList<String>(); // Any servers that are not OFFLINE
    private HashMap<String, Process> runningServers = new HashMap<String, Process>();

    public static final String ZK_ROOT_PATH = "/zkRoot";
    private static final String ZK_CONF_PATH = "zoo.cfg";
    private static final String SERVER_DIR = "~/ece419-project";
    private static final String SERVER_JAR = "m2-server.jar";
    private static final int ZK_PORT = 2181;
    private static final String ZK_HOST = "localhost";
    private static final int ZK_TIMEOUT = 2000;
    private Thread zkServer;
    private ZooKeeperWatcher zkWatcher;
    private ZooKeeper zk;

    public ECSClient(String configPath) {
        DebugHelper.logFuncEnter(logger);

        try {
            logger.setLevel(logLevel);
            // Read configuration file
            BufferedReader reader = new BufferedReader(new FileReader(configPath));
            String l;
            logger.info(String.format("Reading configuration file at: %s", configPath));

            while ((l = reader.readLine()) != null) {
                String[] config = l.split("\\s+", 3);
                logger.info(String.format("%s => %s:%s", config[0], config[1], config[2]));
                serverStatusInfo.put(String.format("%s:%s:%s", config[0], config[1], config[2]),
                        NodeStatus.OFFLINE);
            }

            // Initialize hash ring
            hashRing = new HashRing();

            // Initialize ZooKeeper server
            zkServer = new Thread(new ZooKeeperServer());
            zkServer.start();

            // Initialize ZooKeeper client
            CountDownLatch latch = new CountDownLatch(1);
            zkWatcher = new ZooKeeperWatcher(latch);
            String connectString = String.format("%s:%s", ZK_HOST, ZK_PORT);
            zk = new ZooKeeper(connectString, ZK_TIMEOUT, zkWatcher);
            latch.await(); // Wait for client to initialize

            // Create storage server root
            if (zk.exists(ZK_ROOT_PATH, false) == null) {
                zk.create(ZK_ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
    private class ZooKeeperWatcher implements Watcher {
        private CountDownLatch latch;

        public ZooKeeperWatcher(CountDownLatch clientLatch) {
            this.latch = clientLatch;
        }

        public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.SyncConnected) {
                latch.countDown();
            }
        }
    }

    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize) {
        DebugHelper.logFuncEnter(logger);
        List<ECSNode> nodesAdded = addNodes(1, cacheStrategy, cacheSize);
        DebugHelper.logFuncExit(logger);

        return nodesAdded.get(0);
    }

    @Override
    public List<ECSNode> addNodes(int count, String cacheStrategyStr, int cacheSize) {
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
        Random rand = new Random();
        List<String> availableServers = getAvailableServers();
        List<ECSNode> serversToAdd = new ArrayList<ECSNode>();

        for (int i = 0; i < count; i++) {
            // Randomly select a new, available server to add
            int randIdx = rand.nextInt(availableServers.size());
            String newServerInfo = availableServers.get(randIdx);
            serverStatusInfo.put(newServerInfo, NodeStatus.IDLE);
            unavailableServers.add(newServerInfo);
            availableServers.remove(newServerInfo);

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
        }

        setupNodes(count, cacheStrategyEnum, cacheSize);
        DebugHelper.logFuncExit(logger);

        return nodesAdded;
    }

    @Override
    public Collection<ECSNode> setupNodes(int count, CacheStrategy cacheStrategy, int cacheSize) {
        DebugHelper.logFuncEnter(logger);

        if (!isServerCountValid(count)) {
            return null;
        }

        for (String serverInfo : unavailableServers) {
            String zkNodePath = buildZkNodePath(serverInfo);
            HashMap<String, Metadata> allMetadata = hashRing.getAllMetadata();
            AdminMessage msg = new AdminMessage(MessageType.INIT, allMetadata, null);

            try {
                logger.debug(String.format("Setting up node at path: %s", zkNodePath));

                while (zk.exists(zkNodePath, false) == null) {
                    awaitNodes(count, ZK_TIMEOUT);
                }

                logger.trace(String.format("Setting data to node %s:", zkNodePath));
                logger.trace(String.format("zk: %s", zk));
                zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
            } catch (Exception e) {
                logger.error("Unable to send admin message to node.");
                e.printStackTrace();
            }
        }

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

    @Override
    public boolean start() throws Exception {
        DebugHelper.logFuncEnter(logger);
        AdminMessage msg = new AdminMessage(MessageType.START, null, null);

        for (String serverInfo : unavailableServers) {
            serverStatusInfo.put(serverInfo, NodeStatus.ONLINE);
            String zkNodePath = buildZkNodePath(serverInfo);

            try {
                while (zk.exists(zkNodePath, false) == null) {
                    awaitNodes(1, ZK_TIMEOUT);
                }

                logger.debug(String.format("Setting data at %s: %s", zkNodePath, msg));
                zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
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
        AdminMessage msg = new AdminMessage(MessageType.STOP, null, null);

        for (String serverInfo : unavailableServers) {
            try {
                serverStatusInfo.put(serverInfo, NodeStatus.IDLE);
                String zkNodePath = buildZkNodePath(serverInfo);
                zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
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
        AdminMessage msg = new AdminMessage(MessageType.SHUTDOWN, null, null);

        for (String serverInfo : unavailableServers) {
            try {
                serverStatusInfo.put(serverInfo, NodeStatus.OFFLINE);
                hashRing.removeNode(serverInfo);
                String zkNodePath = buildZkNodePath(serverInfo);
                zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
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
    public boolean removeNodes(Collection<String> nodeNames) {
        DebugHelper.logFuncEnter(logger);

        for (String serverInfo : nodeNames) {
            try {
                serverStatusInfo.put(serverInfo, NodeStatus.OFFLINE);
                hashRing.removeNode(serverInfo);
                unavailableServers.remove(serverInfo);

                AdminMessage msg = new AdminMessage(MessageType.SHUTDOWN, null, null);
                String zkNodePath = buildZkNodePath(serverInfo);

                if (zk.exists(zkNodePath, false) != null) {
                    zk.setData(zkNodePath, msg.toBytes(), zk.exists(zkNodePath, false).getVersion());
                }
            } catch (Exception e) {
                logger.error(String.format("Unable to remove server: %s", serverInfo));
                e.printStackTrace();
                return false;
            }
        }

        DebugHelper.logFuncExit(logger);

        return false;
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
    private List<String> getAvailableServers() {
        DebugHelper.logFuncEnter(logger);
        List<String> availableServers = new ArrayList<String>();

        for (Map.Entry<String, NodeStatus> set : serverStatusInfo.entrySet()) {
            String info = set.getKey();
            NodeStatus status = set.getValue();

            if (status == NodeStatus.OFFLINE) {
                availableServers.add(info);
            }
        }

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
        logger.debug(String.format("Constructed zkNodePath: %s", zkNodePath));
        DebugHelper.logFuncExit(logger);
        return zkNodePath;
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
                    addNodes(count, cacheStrategy, cacheSize);
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
                    addNode(cacheStrategy, cacheSize);
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

                removeNodes(serversToRemove);
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
                stop();
                shutdown();
                zk.close();
                running = false;
                zkServerRunning = false;
                zkServer.stop();
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
        sb.append("\t Randomly choose <num> nodes from available machines and start them \n");

        sb.append(PROMPT).append("addnode <cacheStrategy> <cacheSize>");
        sb.append("\t\t Create new KVServer and add it to the storage service at an arbitrary position \n");

        sb.append(PROMPT).append("start");
        sb.append(
                "\t\t Starts the storage service by calling start() on all KVServer instances that participate in the service \n");

        sb.append(PROMPT).append("stop");
        sb.append(
                "\t\t Stops the service - all participating KVServers are stopped for processing client requests, but remain running \n");

        sb.append(PROMPT).append("shutdown");
        sb.append("\t\t Stops all server instances and exits the remote processes \n");

        sb.append(PROMPT).append("removenode <index>");
        sb.append("\t Remove a server from the storage service at an arbitrary position \n");

        sb.append(PROMPT).append("status");
        sb.append("\t\t Get the current status of all available servers \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t changes the logLevel \n");

        sb.append(PROMPT).append("\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t Exits the program \n");
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
