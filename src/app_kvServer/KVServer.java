package app_kvServer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;
import shared.communication.KVCommunicationServer;
import persistent_storage.PersistentStorage;

import org.apache.zookeeper.*;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import app_kvECS.ECSClient;
import ecs.ECSNode;

import org.apache.zookeeper.Watcher.Event.KeeperState;
import java.math.BigInteger;
import java.util.concurrent.CountDownLatch;
import shared.communication.AdminMessage;
import shared.communication.KVMessage;
import shared.DebugHelper;
import shared.Metadata;
import shared.communication.AdminMessage.MessageType;

import app_kvServer.kvCache.kvCacheOperator;

// Runnable for threading
public class KVServer implements IKVServer, Runnable {
    private static Logger logger = Logger.getRootLogger();

    // M2 Cache implementation
    // Set to unitialized values for now
    private int cacheSize;
    private kvCacheOperator cache = null;
    private String strategy = null;

    private int port;
    private ServerSocket serverSocket;
    private boolean running;

    private PersistentStorage storage;
    private ArrayList<Thread> threadList;
    private Thread newThread;

    public static String dataDirectory = "./data";
    public static String databaseName = "database.properties";

    // Milestone 2 Modifications
    // Zookeeper vars
    private ZooKeeper zoo;
    private String zooHost;
    private String zooPathRoot = ECSClient.ZK_ROOT_PATH;
    private String zooPathServer;
    private int zooPort;

    // Distributed system vars
    // Is server running as distributed system?
    private String name;
    private String hashedName;
    private boolean distributedMode;
    // Write lock
    private boolean locked;
    private ServerStatus status;

    // Latch to wait for completed action
    final CountDownLatch syncLatch = new CountDownLatch(1);

    private Map<String, Metadata> allMetadata;
    private Metadata localMetadata;

    // REMOVE NODE: Mark server as to be deleted.
    // Shutdown server once all data is transferred to successor node
    private boolean toBeDeleted = false;

    // Milestone 3 Modifications
    // For replication

    // TODO - replace paths with ECSClient paths
    private String zooPathRootPrev = ECSClient.ZK_ROOT_PATH_PREV;
    private String zooPathRootNext = ECSClient.ZK_ROOT_PATH_NEXT;
    private String zooPathServerPrev;
	private String zooPathServerNext;

    /**
     * M1: Start KV Server at given port. Server NOT distributed.
     * 
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO",
     *                  "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {
        // Store list of client threads
        this.threadList = new ArrayList<Thread>();
        this.port = port;
        this.serverSocket = null;

        // M2 Cache implementation
        this.cacheSize = cacheSize;
        this.strategy = strategy;
        this.cache = new kvCacheOperator(cacheSize, strategy);

        this.name = getHostname() + ":" + getPort();

        // Not running as distributed system
        this.distributedMode = false;

        // Check if file directory exists
        File testFile = new File(dataDirectory);
        if (!testFile.exists()) {
            this.storage = new PersistentStorage(name);
        }
        // if exists, load into persistentStorage
        else {
            this.storage = new PersistentStorage(databaseName);
        }

        // // Start main thread
        // newThread = new Thread(this);
        // newThread.start();
    }

    /**
     * M2: Initialize KVServer in distributed mode (Zookeeper)
     * 
     * @param name    Name of the KVServer (ipaddress:port)
     * @param zooPort ZK port
     * @param zooHost ZK host
     */
    public KVServer(String name, int zooPort, String zooHost) {
        // Running as distributed system
        this.distributedMode = true;
        // TODO Check: Start as stopped status
        this.status = ServerStatus.STOP;
        // Set server name
        this.name = name;
        // Write lock is enabled at beginning, since server is stopped
        this.locked = true;
        // Store list of client threads
        this.threadList = new ArrayList<Thread>();
        // Split server name to get port (provided in ipaddr:port format)
        this.port = Integer.parseInt(name.split(":")[1]);

        // Check if file directory exists
        File testFile = new File(dataDirectory);
        if (!testFile.exists()) {
            this.storage = new PersistentStorage(name);
        }
        // if exists, load into persistentStorage
        else {
            this.storage = new PersistentStorage(name, databaseName);
        }

        // Hashed server name
        this.hashedName = storage.MD5Hash(name).toString();
        // Global metadata
        this.allMetadata = new HashMap<>();
        // Local server-specific metadata
        this.localMetadata = null;

        // Configure zookeeper
        this.zooPathServer = zooPathRoot + "/" + name;
        this.zooHost = zooHost;
        this.zooPort = zooPort;

        // Milestone 3
        this.zooPathServerNext = zooPathRootNext + "/" + name;
		this.zooPathServerPrev = zooPathRootPrev + "/" + name;

        // Initialize new zookeeper client
        try {
            this.zoo = new ZooKeeper(zooHost + ":" + zooPort, 20000, new Watcher() {
                public void process(WatchedEvent we) {
                    if (we.getState() == KeeperState.SyncConnected) {
                        // Countdown latch if we succesfully connected
                        syncLatch.countDown();
                    }
                }
            });

            logger.info("Succesfully initialized new ZooKeeper client on serverside! Zoo host: " + zooHost
                    + " Zoo port: " + zooPort);
            // Blocks until current count reaches zero
            syncLatch.await();
        } catch (IOException | InterruptedException e) {
            logger.error("Failed to initialize ZooKeeper client: " + e);
        }

        // Handle metadata
        handleMetadata();

        // // Start main thread
        // newThread = new Thread(this);
        // newThread.start();

        // this.run();
    }

    /**
     * Version 2 - not working!
     * Helper function to handle ZK metadata and send to adminMessageHelper
     */
    public void handleMetadataImproved() {
        DebugHelper.logFuncEnter(logger);
        try {
            byte[] adminMessageBytes = zoo.getData(zooPathServer, new Watcher() {
                @Override
                public void process(WatchedEvent we) {
                    if (we.getType() == Event.EventType.None) {
                        switch (we.getState()) {
                            case Expired:
                                syncLatch.countDown();
                                break;
                        }
                    } else {
                        try {
                            handleMetadataImproved();
                        } catch (Exception ex) {
                            System.out.println(ex.getMessage());
                        }
                    }
                }
            }, null);

            String adminMessageString = new String(adminMessageBytes, StandardCharsets.UTF_8);
            handleAdminMessageHelper(adminMessageString);

            // Create new ZNode - see https://www.baeldung.com/java-zookeeper
            if (zoo != null) {
                try {
                    // The call to ZooKeeper.exists() checks for the existence of the znode
                    if (zoo.exists(zooPathServer, false) == null) {
                        // Path, data, access control list (perms), znode type (ephemeral = delete upon
                        // client DC)
                        byte[] data = this.name.getBytes();

                        zoo.create(zooPathServer, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        logger.info("Succesfully created ZNode on serverside at zooPathServer: " + zooPathServer);
                    }
                } catch (KeeperException | InterruptedException e) {
                    logger.error("Failed to create ZK ZNode: ", e);
                }
            }

            syncLatch.await();
        } catch (KeeperException e1) {
            logger.error(e1);
        } catch (InterruptedException e2) {
            logger.error(e2);
        }
    }

    /**
     * Version 1
     * Helper function to handle ZK metadata and send to adminMessageHelper
     */
    public void handleMetadata() {
        DebugHelper.logFuncEnter(logger);
        // Create new ZNode - see https://www.baeldung.com/java-zookeeper
        try {
            // The call to ZooKeeper.exists() checks for the existence of the znode
            if (zoo.exists(zooPathServer, false) == null) {
                // Path, data, access control list (perms), znode type (ephemeral = delete upon
                // client DC)
                zoo.create(zooPathServer, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                logger.info("Succesfully created Root ZNode on serverside at zooPathServer: " + zooPathServer);
            }
            // // Milestone 3
            // // Create ZNode instances for replication (prev + next servers)
            // if (zoo.exists(zooPathServerPrev, false) == null) {
            //     // Path, data, access control list (perms), znode type (ephemeral = delete upon
            //     // client DC)
            //     zoo.create(zooPathServerPrev, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            //     logger.info("Succesfully created Previous ZNode on serverside at zooPathServerPrev: " + zooPathServerPrev);
            // }
            // if (zoo.exists(zooPathServerNext, false) == null) {
            //     // Path, data, access control list (perms), znode type (ephemeral = delete upon
            //     // client DC)
            //     zoo.create(zooPathServerNext, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            //     logger.info("Succesfully created Next ZNode on serverside at zooPathServerNext: " + zooPathServerNext);
            // }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to create ZK ZNode: ", e);
        }

        // Root Node Event Watcher
        try {
            // Given path, do we need to watch node, stat of node
            byte[] adminMessageBytes = zoo.getData(zooPathServer, new Watcher() {
                // See https://zookeeper.apache.org/doc/r3.1.2/javaExample.html
                public void process(WatchedEvent we) {
                    if (running == false) {
                        return;
                    } else {
                        try {
                            byte[] adminMessageBytes = zoo.getData(zooPathServer, this, null);
                            String adminMessageString = new String(adminMessageBytes, StandardCharsets.UTF_8);
                            logger.info("Incoming WATCHER admin message string for root server: " + adminMessageString);
                            handleAdminMessageHelper(adminMessageString);
                        } catch (KeeperException | InterruptedException e) {
                            logger.error("Failed to process admin message: ", e);
                        }
                    }
                }
            }, null);

            // // Process the admin Message
            // String adminMessageString = new String(adminMessageBytes,
            // StandardCharsets.UTF_8);
            // logger.info("This is the incoming OUTER admin message string: " +
            // adminMessageString);
            // handleAdminMessageHelper(adminMessageString);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to process ZK metadata of root node: ", e);
        }

        // Next node event watcher
        try {
            byte[] adminMessageBytes = zoo.getData(zooPathServerNext, new Watcher() {
                public void process(WatchedEvent we) {
                    if (running == false) {
                        return;
                    } else {
                        try {
                            byte[] adminMessageBytes = zoo.getData(zooPathServerNext, this, null);
                            String adminMessageString = new String(adminMessageBytes, StandardCharsets.UTF_8);
                            logger.info("Incoming WATCHER admin message string for next server: " + adminMessageString);
                            handleAdminMessageHelper(adminMessageString);
                        } catch (KeeperException | InterruptedException e) {
                            logger.error("Failed to process admin message: ", e);
                        }
                    }
                }
            }, null);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to process ZK metadata of next node : ", e);
        }

        // Previous node event watcher
        try {
            byte[] adminMessageBytes = zoo.getData(zooPathServerPrev, new Watcher() {
                public void process(WatchedEvent we) {
                    if (running == false) {
                        return;
                    } else {
                        try {
                            byte[] adminMessageBytes = zoo.getData(zooPathServerPrev, this, null);
                            String adminMessageString = new String(adminMessageBytes, StandardCharsets.UTF_8);
                            logger.info("Incoming WATCHER admin message string for prev server: " + adminMessageString);
                            handleAdminMessageHelper(adminMessageString);
                        } catch (KeeperException | InterruptedException e) {
                            logger.error("Failed to process admin message: ", e);
                        }
                    }
                }
            }, null);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to process ZK metadata of prev node : ", e);
        }
    }

    /**
     * Helper function to get ECS Node from admin message
     * 
     * @param adminMessageBytes Input bytes of admin message
     * @return
     */
    public ECSNode getECSNode(byte[] adminMessageBytes) {
        DebugHelper.logFuncEnter(logger);

        // Process ECSNode
        ByteArrayInputStream byteInputTest = null;
        ObjectInputStream objectInputTest = null;
        Object ECSObject = null;

        try {
            byteInputTest = new ByteArrayInputStream(adminMessageBytes);
            objectInputTest = new ObjectInputStream(byteInputTest);
            ECSObject = objectInputTest.readObject();
        } catch (IOException ioe) {
            logger.error(ioe);
        } catch (ClassNotFoundException cnfe) {
            logger.error(cnfe);
        } finally {
            try {
                if (byteInputTest != null) {
                    byteInputTest.close();
                }
                if (objectInputTest != null) {
                    objectInputTest.close();
                }
            } catch (IOException ioe) {
                logger.error(ioe);
            }
        }

        ECSNode node = (ECSNode) ECSObject;
        return node;
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public String getHostname() {
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error("The IP address of server host cannot be resolved. \n", e);
        }
        return hostname;
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        // Implemented under M2
        switch (this.strategy) {
            case "LRU":
                return IKVServer.CacheStrategy.LRU;
            case "LFU":
                return IKVServer.CacheStrategy.LFU;
            case "FIFO":
                return IKVServer.CacheStrategy.FIFO;
            default:
                return IKVServer.CacheStrategy.None;
        }
    }

    @Override
    public int getCacheSize() {
        // TODO Auto-generated method stub
        return this.cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        // need method in persistent storage class
        return storage.existsCheck(key);
    }

    /**
     * Check if key has value in cache
     */
    @Override
    public boolean inCache(String key) {
        if (cache.cacheActiveStatus() == true) {
            return cache.inCache(key);
        } else {
            return false;
        }
    }

    @Override
    public String getKV(String key) throws Exception {
        String value = null;
        // Check cache first
        if (cache.cacheActiveStatus() == true) {
            value = cache.getCache(key);
            // Value was in cache
            if (value != null) {
                return value;
            }
        }
        // Value was not in cache, look on disk
        value = storage.get(key);
        if (value == null) {
            logger.error("Key: " + key + " cannot be found on storage!");
            throw new Exception("Failed to find key in storage!");
        } else {
            // Write to cache
            if (cache.cacheActiveStatus()) {
                cache.putCache(key, value);
            }
            return value;
        }
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        // System.out.println("RECEIVED A PUT"+value);
        // If value was blank, delete
        if (value.equals("") || value == null) {
            if (inStorage(key)) {
                // System.out.println("****A blank value was PUT, delete key: "+key);
                // Delete key if no value was provided in put
                storage.delete(key);

                // Remove from cache as well
                if (cache.cacheActiveStatus()) {
                    cache.delete(key);
                }

            } else {
                logger.error("Tried to delete non-existent key: " + key);
                throw new Exception("Tried to delete non-existent key!");
            }

        } else if (!storage.put(key, value)) {
            logger.error("Failed to PUT (" + key + ',' + value + ") into map!");
            throw new Exception("Failed to put KV pair in storage!");
        }
        // Write to cache
        if (cache.cacheActiveStatus()) {
            cache.putCache(key, value);
        }
    }

    @Override
    public void clearCache() {
        if (cache.cacheActiveStatus()) {
            cache.clearCache();
        }
    }

    @Override
    public void clearStorage() {
        storage.wipeStorage();
        clearCache();
    }

    @Override
    public void run() {
        // boolean running status
        running = initializeServer();

        if (serverSocket != null) {
            while (running) {
                try {
                    Socket client = serverSocket.accept();

                    KVCommunicationServer connection = new KVCommunicationServer(client, this);

                    newThread = new Thread(connection);
                    newThread.start();
                    // Append new client thread to global thread list
                    threadList.add(newThread);

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    // running = false;
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");
    }

    /** Server initialiation helper. Initializes socket on given port. */
    private boolean initializeServer() {
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
                    + serverSocket.getLocalPort());
            return true;

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    @Override
    public void kill() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            // Stop running threads gracefully
            for (int i = 0; i < threadList.size(); i++) {
                threadList.get(i).interrupt();
            }
            logger.info("Goodbye, server is closing");
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    // ********************** Milestone 2 Modifications **********************

    /**
     * Returns boolean for server mode (distributed or not)
     * 
     * @return True if distributed, False if non-distributed
     */
    // public boolean distributed(){
    // return distributedMode;
    // }

    /**
     * Helper function to get current status of the server
     */
    @Override
    public ServerStatus getStatus() {
        logger.info("*** Returning current server status: " + status.name());
        return status;
    }

    /**
     * Helper function to get current status of the lock
     */
    @Override
    public boolean getLock() {
        return locked;
    }

    @Override
    public void start() {
        status = ServerStatus.START;
        // Unlock server for writing
        locked = false;
        logger.info("Started the KVServer, all client requests and all ECS requests are processed.");
    }

    @Override
    public void stop() {
        status = ServerStatus.STOP;
        // Reject client requests
        locked = true;
        logger.info("Stopped the KVServer, all client requests are rejected and only ECS requests are processed.");
    }

    @Override
    public void shutDown() {
        logger.info("Shutting down the KVServer through ECS...");
        close();
    }

    @Override
    public void lockWrite() {
        locked = true;
        logger.info("ACQUIRE WRITE LOCK: Future write requests blocked for now!");
    }

    @Override
    public void unLockWrite() {
        locked = false;
        logger.info("RELEASE WRITE LOCK: Future write requests allowed for now!");
    }

    @Override
    public boolean getLockWrite() {
        return locked;
    }

    /**
     * Initialize KV Server with initial metadata
     * 
     * @param adminMeString Admin message from communications
     */
    public void initKVServer(String adminMessageString) {
        // Process incoming admin message
        AdminMessage incomingMessage = new AdminMessage(adminMessageString);

        // TODO - check that getMsgTypeString is available
        // String incomingMessageType = incomingMessage.getMsgTypeString()
        // Update metadata map
        this.allMetadata = incomingMessage.getMsgMetadata();

        // for (String key: allMetadata.keySet()){
        // Metadata metadata = allMetadata.get(key);
        // }

        // Update local metadata for this server
        // Used to be MD5 hash of ip:port, now just ip:port
        this.localMetadata = allMetadata.get(name);

        // Set status to STOPPED - prevent client requests for now
        this.status = ServerStatus.STOP;
    }

    /**
     * Update metadata, move entries as required
     * 
     * @param adminMessageString Admin message string from communications
     */
    @Override
    public void update(String adminMessageString) {
        // Process incoming admin message
        AdminMessage incomingMessage = new AdminMessage(adminMessageString);
        // Update metadata map
        this.allMetadata = incomingMessage.getMsgMetadata();
        // Update local metadata for this server
        // Used to be MD5 hash of ip:port, now just ip:port
        this.localMetadata = allMetadata.get(name);

        // ************ Move data to correct server ************
        BigInteger begin = localMetadata.getHashStart();
        BigInteger end = localMetadata.getHashStop();

        logger.info("UPDATED hash ranges! New Begin: " + begin.toString() + " New End: " + end.toString());

        // Acquire write lock - prevent further writing to this server since data is
        // stale
        lockWrite();

        // REMOVE NODE - Check if the hash start and stop are 0,0
        // If so, this is a REMOVE update and all entries should be transferred away
        // Server should be shutdown after receiving confirmation of transfer
        if ((begin.compareTo(BigInteger.ZERO) == 0) && (end.compareTo(BigInteger.ZERO) == 0)) {
            logger.info("**** REMOVE SERVER (0,0 hash range): KVServer marked as to be deleted!");
            this.toBeDeleted = true;

            // Get all entries to be transferred
            Map<String, String> moveAllEntries = storage.returnAllEntries();
            String unreachableEntriesString = null;
            for (Map.Entry<String, String> entry : moveAllEntries.entrySet()) {
                unreachableEntriesString += (entry.getKey() + '[' + entry.getValue() + ']');
            }
            logger.info("Removing node! Move all entries: " + unreachableEntriesString);

            // If no unreachable entries, no need to transfer entries to successor
            if (moveAllEntries == null || moveAllEntries.isEmpty()) {
                logger.info("No unreachable entries in this TO BE REMOVED node!");
                unLockWrite();
                shutDown();
            }

            // Some entries need to be moved. Send message to successor node.
            else {
                // Get the next node
                ECSNode nextNode = localMetadata.getNextNode();

                // Get metadata of destination server
                Metadata transferServerMetadata = allMetadata
                        .get(nextNode.getNodeHost() + ":" + nextNode.getNodePort());
                // Build destination server name
                String transferServerName = zooPathRoot + "/" + transferServerMetadata.getHost() + ":"
                        + transferServerMetadata.getPort();
                try {
                    // Send admin message to destination
                    // Message Type, metadata, data, to_server, from_server (allows recipient to
                    // send confirmation back later)
                    sendMessage(MessageType.TRANSFER_DATA, null, moveAllEntries, transferServerName, zooPathServer);
                    logger.info("Sent a TRANSFER_DATA request to: " + transferServerName + " from " + zooPathServer);
                } catch (InterruptedException | KeeperException e) {
                    logger.error("Failed to send admin message with unreachable entries: ", e);
                }
            }
        }

        // UPDATE WITHOUT REMOVING NODE
        // Check if there are any unreachable entries and move to appropriate server
        else {
            // Get unreachable entries based on current hash range
            Map<String, String> unreachableEntries = storage.hashUnreachable(begin, end);
            String unreachableEntriesString = null;
            for (Map.Entry<String, String> entry : unreachableEntries.entrySet()) {
                unreachableEntriesString += (entry.getKey() + '[' + entry.getValue() + ']');
            }
            logger.info("Updating metdata, not removing node. Move entries: " + unreachableEntriesString);

            // If no unreachable entries, no need to transfer entries to successor
            if (unreachableEntries == null || unreachableEntries.isEmpty()) {
                logger.info("No unreachable entries after hash range update..");
                unLockWrite();
            }
            // If there are unreachable entries, send them to the next node
            else {
                logger.info("Some unreachable entries found after hash range update..moving..");

                // Get the next node
                ECSNode nextNode = localMetadata.getNextNode();

                // Get metadata of destination server
                Metadata transferServerMetadata = allMetadata
                        .get(nextNode.getNodeHost() + ":" + nextNode.getNodePort());
                // Build destination server name
                String transferServerName = zooPathRoot + "/" + transferServerMetadata.getHost() + ":"
                        + transferServerMetadata.getPort();
                try {
                    // Send admin message to destination
                    // Message Type, metadata, data, to_server, from_server (allows recipient to
                    // send confirmation back later)
                    sendMessage(MessageType.TRANSFER_DATA, null, unreachableEntries, transferServerName, zooPathServer);
                    logger.info("Sent a TRANSFER_DATA request to: " + transferServerName + " from " + zooPathServer);
                } catch (InterruptedException | KeeperException e) {
                    logger.error("Failed to send admin message with unreachable entries: ", e);
                }
                // Don't release write lock until TRANSFER_DATA_COMPLETE comes back in
                // unLockWrite();
            }
        }

    }


    /**
     * Replicate a single given KV Pair on previous, next server if available
     * 
     * @param adminMessageString Admin message string from communications
     */
    @Override
    public void replicateSingleEntry(String key, String value) {
		Map<String, String> reachableEntries = new HashMap<>();
		reachableEntries.put(key, value);

        String reachableEntriesString = null;
        for (Map.Entry<String, String> entry : reachableEntries.entrySet()) {
            reachableEntriesString += (entry.getKey() + '[' + entry.getValue() + ']');
        }
        logger.info("Replicating single entry: " + reachableEntriesString);

        // Acquire write lock - prevent further writing to this server for now
        lockWrite();

        // If no reachable entries, no need to transfer entries to successor
        if (reachableEntries == null || reachableEntries.isEmpty()) {
            logger.info("No reachable entries to replicate. Done...");
            unLockWrite();
        }
        // If there are reachable entries, send them to the next, prev node
        else {
            logger.info("Single entry found...replicating!");

            // If there are at least 2 servers, send to prev
            if (allMetadata.size() >= 2){
                // Get the prev node
                ECSNode prevNode = localMetadata.getPrevNode();
                // Get metadata of destination server
                Metadata transferServerMetadata = allMetadata
                        .get(prevNode.getNodeHost() + ":" + prevNode.getNodePort());
                // Build prev server name
                String transferServerName = zooPathRootPrev + "/" + transferServerMetadata.getHost() + ":"
                        + transferServerMetadata.getPort();
                try {
                    // Send admin message to destination
                    // Message Type, metadata, data, to_server, from_server (allows recipient to
                    // send confirmation back later)
                    sendMessage(MessageType.REPLICATE_DATA, null, reachableEntries, transferServerName, zooPathServer);
                    logger.info("*** Replicating single entry to prev! Sent a REPLICATE_DATA request to: " + transferServerName + " from " + zooPathServer);
                } catch (InterruptedException | KeeperException e) {
                    logger.error("Failed to replicate single entry to prev: ", e);
                }
            }

            // If there are at least 3 servers, send to next
            if (allMetadata.size() >= 3){
                // Get the next node
                ECSNode nextNode = localMetadata.getNextNode();
                // Get metadata of destination server
                Metadata transferServerMetadata = allMetadata
                        .get(nextNode.getNodeHost() + ":" + nextNode.getNodePort());
                // Build next server name
                String transferServerName = zooPathRootNext + "/" + transferServerMetadata.getHost() + ":"
                        + transferServerMetadata.getPort();
                try {
                    // Send admin message to destination
                    // Message Type, metadata, data, to_server, from_server (allows recipient to
                    // send confirmation back later)
                    sendMessage(MessageType.REPLICATE_DATA, null, reachableEntries, transferServerName, zooPathServer);
                    logger.info("*** Replicating single entry to next! Sent a REPLICATE_DATA request to: " + transferServerName + " from " + zooPathServer);
                } catch (InterruptedException | KeeperException e) {
                    logger.error("Failed to replicate single entry to next: ", e);
                }
            }
            // TODO - check if we need to wait for confirmation that replication
            // is complete, before unlocking
            unLockWrite();
        }
    }


    /**
     * Replicate KV Pairs on previous, next server if available
     * 
     * @param adminMessageString Admin message string from communications
     */
    @Override
    public void replicate() {
        // ************ Move data to correct server ************
        BigInteger begin = localMetadata.getHashStart();
        BigInteger end = localMetadata.getHashStop();

        // Acquire write lock - prevent further writing to this server for now
        lockWrite();

        // Replicate without removing root node
        // Get reachable entries based on current hash range
        Map<String, String> reachableEntries = storage.hashReachable(begin, end);
        String reachableEntriesString = null;
        for (Map.Entry<String, String> entry : reachableEntries.entrySet()) {
            reachableEntriesString += (entry.getKey() + '[' + entry.getValue() + ']');
        }
        logger.info("Replicating entries: " + reachableEntriesString);

        // If no reachable entries, no need to transfer entries to successor
        if (reachableEntries == null || reachableEntries.isEmpty()) {
            logger.info("No reachable entries to replicate. Done...");
            unLockWrite();
        }
        // If there are reachable entries, send them to the next, prev node
        else {
            logger.info("Some reachable entries found...replicating!");

            // If there are at least 2 servers, send to prev
            if (allMetadata.size() >= 2){
                // Get the prev node
                ECSNode prevNode = localMetadata.getPrevNode();
                // Get metadata of destination server
                Metadata transferServerMetadata = allMetadata
                        .get(prevNode.getNodeHost() + ":" + prevNode.getNodePort());
                // Build prev server name
                String transferServerName = zooPathRootPrev + "/" + transferServerMetadata.getHost() + ":"
                        + transferServerMetadata.getPort();
                try {
                    // Send admin message to destination
                    // Message Type, metadata, data, to_server, from_server (allows recipient to
                    // send confirmation back later)
                    sendMessage(MessageType.REPLICATE_DATA, null, reachableEntries, transferServerName, zooPathServer);
                    logger.info("*** Replicating to prev! Sent a REPLICATE_DATA request to: " + transferServerName + " from " + zooPathServer);
                } catch (InterruptedException | KeeperException e) {
                    logger.error("Failed to replicate to prev: ", e);
                }
            }

            // If there are at least 3 servers, send to next
            if (allMetadata.size() >= 3){
                // Get the next node
                ECSNode nextNode = localMetadata.getNextNode();
                // Get metadata of destination server
                Metadata transferServerMetadata = allMetadata
                        .get(nextNode.getNodeHost() + ":" + nextNode.getNodePort());
                // Build next server name
                String transferServerName = zooPathRootNext + "/" + transferServerMetadata.getHost() + ":"
                        + transferServerMetadata.getPort();
                try {
                    // Send admin message to destination
                    // Message Type, metadata, data, to_server, from_server (allows recipient to
                    // send confirmation back later)
                    sendMessage(MessageType.REPLICATE_DATA, null, reachableEntries, transferServerName, zooPathServer);
                    logger.info("*** Replicating to next! Sent a REPLICATE_DATA request to: " + transferServerName + " from " + zooPathServer);
                } catch (InterruptedException | KeeperException e) {
                    logger.error("Failed to replicate to next: ", e);
                }
            }
            // TODO - check if we need to wait for confirmation that replication
            // is complete, before unlocking
            unLockWrite();
        }
    }


    /**
     * Send new admin message to destination servers
     * 
     * @param type       Message type
     * @param metadata   Metadata map to be sent
     * @param data       New KV entries to be transfered
     * @param toServer   Name of destination server (full name: (root/host:port))
     * @param fromServer Name of sender server (root/host:port)
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void sendMessage(MessageType type, Map<String, Metadata> metadata, Map<String, String> data,
            String toServer, String fromServer) throws KeeperException, InterruptedException {
        AdminMessage toSend = new AdminMessage(type, metadata, data, fromServer);
        logger.info("Admin Message sent with SENDING SERVER FIELD: " + toSend.getSendingServer());
        zoo.setData(toServer, toSend.toBytes(), zoo.exists(toServer, false).getVersion());
        logger.info("Sent KV Transfer Message to: " + toServer);
    }

    /**
     * Target server responded with a confirmation that KV pairs have been
     * transferred.
     * Proceed to delete the unreachable KV pairs from this current server
     * 
     * @param adminMessageString Incoming admin message string
     */
    @Override
    public void confirmDataTransfer(String adminMessageString) {
        BigInteger begin = localMetadata.getHashStart();
        BigInteger end = localMetadata.getHashStop();

        // Get unreachable entries based on current hash range
        Map<String, String> unreachableEntries = storage.hashUnreachable(begin, end);

        // Iterate through unreachable entries and remove from storage
        Iterator itr = unreachableEntries.entrySet().iterator();

        while (itr.hasNext()) {
            Map.Entry keyVal = (Map.Entry) itr.next();
            String key = (String) keyVal.getKey();
            if (!storage.keyValid(begin, end, storage.MD5Hash(key))) {
                // Remove unreachable KV Pairs from disk
                // Cached version
                try {
                    putKV(key, "");
                } catch (Exception e) {
                    logger.error("Failed to remove unreachable KV pair from disk after confirm data transfer: " + e);
                }
            } else {
                logger.error("Failed to remove unreachable KV pair from disk - reachable conflict!");
            }
        }

        logger.info("Outgoing data transfer completed!");

        // If this server is being removed, shut it down for good
        if (this.toBeDeleted) {
            logger.info("*** Shutting down server after confirming outgoing data transfer complete!");
            clearStorage();
            shutDown();
        }

        // Release the write lock since data is now up to date
        unLockWrite();

        // Milestone 3: Add a replication call after metadata updated and entries transferred
        replicate();
    }

    /**
     * Receive new incoming KV Pairs and store into persistent storage
     * 
     * @param adminMessageString Incoming admin message string
     */
    @Override
    public void processDataTransfer(String adminMessageString) {
        // Process incoming admin message string
        AdminMessage incomingMessage = new AdminMessage(adminMessageString);
        // MessageType incomingMessageType = incomingMessage.getMsgType();

        logger.info("Trying to process incoming data transfer from: " + incomingMessage.getSendingServer());

        // Acquire write lock
        lockWrite();
        Map<String, String> incomingData = incomingMessage.getMsgKeyValue();
        Iterator<Map.Entry<String, String>> itr = incomingData.entrySet().iterator();
        // Loop through KV entries in incoming data
        while (itr.hasNext()) {
            Map.Entry<String, String> entry = itr.next();
            // TODO - Check this logic
            if (entry.getValue().toString().equals("")) {
                // No cache (old version)
                // storage.delete(entry.getKey());
                // Cached version
                // Cached version
                try {
                    putKV(entry.getKey().toString(), "");
                } catch (Exception e) {
                    logger.error("Failed to PUT DELETE incoming data transfer from distributed server: " + e);
                }
            }
            // Write new entries to disk
            else {
                // No cache (old version)
                // storage.put(entry.getKey().toString(), entry.getValue().toString());
                // Cached version
                try {
                    putKV(entry.getKey().toString(), entry.getValue().toString());
                } catch (Exception e) {
                    logger.error("Failed to PUT incoming data transfer from distributed server: " + e);
                }
            }
        }

        // Send confirmation message (data transfer complete) back to sender server
        // Build destination server name
        // String senderServerName = zooPathRoot + "/" +
        // incomingMessage.getSendingServer();
        String originServerName = incomingMessage.getSendingServer();
        logger.info("Received and finished an incoming data transfer from: " + originServerName);

        try {
            // Send admin message to sender server
            // Message type, metadata, data, to_server, from_server
            sendMessage(MessageType.TRANSFER_DATA_COMPLETE, null, null, originServerName, zooPathServer);
            logger.info("Sent a TRANSFER_DATA_COMPLETE to: " + originServerName + " from " + zooPathServer);
        } catch (InterruptedException | KeeperException e) {
            logger.error("Failed to send TRANSFER_DATA_COMPLETE admin message to sender server: ", e);
        }

        // Release write lock
        unLockWrite();
    }



    /**
     * Receive replicant KV Pairs and store into persistent storage
     * 
     * @param adminMessageString Incoming admin message string
     */
    @Override
    public void processReplicas(String adminMessageString) {
        // Process incoming admin message string
        AdminMessage incomingMessage = new AdminMessage(adminMessageString);
        // MessageType incomingMessageType = incomingMessage.getMsgType();

        logger.info("Trying to process incoming replica transfer from: " + incomingMessage.getSendingServer());

        // Acquire write lock
        lockWrite();
        Map<String, String> incomingData = incomingMessage.getMsgKeyValue();

        String replicaString = null;
        for (Map.Entry<String, String> entry : incomingData.entrySet()) {
            replicaString += (entry.getKey() + '[' + entry.getValue() + ']');
        }
        logger.info("Received entries to replicate: " + replicaString);

        Iterator<Map.Entry<String, String>> itr = incomingData.entrySet().iterator();
        // Loop through KV entries in incoming data
        while (itr.hasNext()) {
            Map.Entry<String, String> entry = itr.next();
            // TODO - Check this logic
            if (entry.getValue().toString().equals("")) {
                try {
                    putKV(entry.getKey().toString(), "");
                } catch (Exception e) {
                    logger.error("Failed to PUT DELETE incoming replica from distributed server: " + e);
                }
            }
            // Write new entries to disk
            else {
                try {
                    putKV(entry.getKey().toString(), entry.getValue().toString());
                } catch (Exception e) {
                    logger.error("Failed to PUT incoming replica from distributed server: " + e);
                }
            }
        }
        // // Send confirmation message (data transfer complete) back to sender server
        // String originServerName = incomingMessage.getSendingServer();
        // logger.info("Received and finished an incoming data transfer from: " + originServerName);

        // try {
        //     // Send admin message to sender server
        //     // Message type, metadata, data, to_server, from_server
        //     sendMessage(MessageType.TRANSFER_DATA_COMPLETE, null, null, originServerName, zooPathServer);
        //     logger.info("Sent a TRANSFER_DATA_COMPLETE to: " + originServerName + " from " + zooPathServer);
        // } catch (InterruptedException | KeeperException e) {
        //     logger.error("Failed to send TRANSFER_DATA_COMPLETE admin message to sender server: ", e);
        // }
        // Release write lock
        unLockWrite();
    }


    public boolean distributed() {
        return distributedMode;
    }

    /**
     * Return local metadata variable
     */
    @Override
    public Metadata getLocalMetadata() {
        return localMetadata;
    }

    /**
     * Return global metadata map
     */
    @Override
    public Map<String, Metadata> getAllMetadata() {
        return allMetadata;
    }

    /**
     * Helper function for handling incoming admin message (route to appropriate
     * case)
     * 
     * @param adminMessageString Incoming admin message
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void handleAdminMessageHelper(String adminMessageString) throws KeeperException, InterruptedException {
        DebugHelper.logFuncEnter(logger);
        // Do Nothing if blank message
        if (adminMessageString == null || adminMessageString.equals("")) {
            logger.error("handleAdminMessageHelper can do nothing - adminMessageString is null! Returning.");
            return;
        } else {
            logger.info("adminMessageString is not null, trying to handle admin message now...");
            AdminMessage incomingMessage = new AdminMessage(adminMessageString);

            // M2 Cache implementation - grab cache info from admin message
            // If first time running handleAdminMessageHelper, cache hasn't been initialized
            // yet..
            if (this.strategy == null) {
                logger.info("Trying to get cache info from metadata!");
                Map<String, Metadata> cacheMetadataAll = incomingMessage.getMsgMetadata();
                Metadata cacheMetdataLocal = cacheMetadataAll.get(name);
                this.cacheSize = cacheMetdataLocal.getCacheSize();
                // TODO Check if this works to convert enum to string
                this.strategy = cacheMetdataLocal.getCacheStrategy().name();
                this.cache = new kvCacheOperator(cacheSize, strategy);
                logger.info("Finished getting cache size, strategy from metadata! Strat: " + this.strategy);
            }

            // Now we check what type of message we got
            MessageType incomingMessageType = incomingMessage.getMsgType();

            // TODO - may need to block incoming requests, check this!
            if (incomingMessageType == MessageType.INIT) {
                logger.info("Got admin message INIT!");
                initKVServer(adminMessageString);
            } else if (incomingMessageType == MessageType.START) {
                logger.info("Got admin message START!");
                start();
            } else if (incomingMessageType == MessageType.STOP) {
                logger.info("Got admin message STOP!");
                stop();
            } else if (incomingMessageType == MessageType.SHUTDOWN) {
                logger.info("Got admin message SHUTDOWN!");
                shutDown();
            } else if (incomingMessageType == MessageType.TRANSFER_DATA) {
                // Handle incoming metadata transfer from another server
                logger.info("Got admin message TRANSFER_DATA (receiving an incoming data transfer)!");
                processDataTransfer(adminMessageString);
            } else if (incomingMessageType == MessageType.TRANSFER_DATA_COMPLETE) {
                // Handle incoming metadata transfer from another server
                logger.info("Got admin message TRANSFER_DATA_COMPLETE (data transfer completed)!");
                confirmDataTransfer(adminMessageString);
            } else if (incomingMessageType == MessageType.UPDATE) {
                // Update metadata repository for this server, shift entries to another server
                // if needed
                logger.info("Got admin message UPDATE (update metadata)!");
                update(adminMessageString);
            } else if (incomingMessageType == MessageType.REPLICATE_START) {
                // Ask root server to replicate its KV pairs to prev, next servers
                logger.info("Got admin message REPLICATE_START!");
                replicate();
            } else if (incomingMessageType == MessageType.REPLICATE_DATA) {
                // Receieve replicant KV pairs, save to disk
                logger.info("Got admin message REPLICATE_DATA (receiving incoming replica(s)!)");
                processReplicas(adminMessageString);
            }
            // else if (incomingMessageType == MessageType.LOCKWRITE){
            // lockWrite();
            // }
            // else if (incomingMessageType == MessageType.UNLOCKWRITE){
            // unLockWrite();
            // }
            // // Transfer a subset of data to another server
            // else if (incomingMessageType == MessageType.MOVE_DATA){
            // moveData(adminMessageString);
            // }
        }
    }

    /**
     * Main entry point for the echo server application.
     * 
     * @param args contains the port number at args[0],
     *             cacheSize at args[1],
     *             strategy at args[2]
     */
    public static void main(String[] args) throws IOException {
        try {
            // TODO turn off logging temporarily for server
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out
                        .println("Usage: M1: Server <port> <cachesize> <cachetype>!\n M2: Server <name> <port> <host>");
            } else {
                // M1 Standard Server
                try {
                    int port = Integer.parseInt(args[0]);
                    int cacheSize = Integer.parseInt(args[1]);
                    String strategy = args[2];
                    KVServer newKV = new KVServer(port, cacheSize, strategy);
                    newKV.run();
                }
                // M2 Distributed Server
                // String name, int zooPort, String zooHost
                catch (NumberFormatException e) {
                    // String serverName = args[0];
                    // String zHost = args[2];
                    // int zPort = Integer.parseInt(args[1]);
                    KVServer newKV = new KVServer(args[0], Integer.parseInt(args[1]), args[2]);
                    newKV.run();
                }

            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument 2: Not a number!");
            System.out.println("Usage: M1: Server <port> <cachesize> <cachetype>!\n M2: Server <name> <port> <host>");
            System.exit(1);
        }
    }
}
