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
        // this.status = ServerStatus.STOP;
        // Set server name
        this.name = name;
        // Write lock disabled
        this.locked = false;
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

        // Initialize new zookeeper client
        try {
            this.zoo = new ZooKeeper(zooHost + ":" + zooPort, 5000, new Watcher() {
                public void process(WatchedEvent we) {
                    if (we.getState() == KeeperState.SyncConnected) {
                        // Countdown latch if we succesfully connected
                        syncLatch.countDown();
                    }
                }
            });

            logger.info("Succesfully initialized new ZooKeeper client on serverside! Zoo host: " + zooHost + " Zoo port: " + zooPort);
            // Blocks until current count reaches zero
            syncLatch.await();
        } catch (IOException | InterruptedException e) {
            logger.error("Failed to initialize ZooKeeper client: " + e);
        }

        // Create new ZNode - see https://www.baeldung.com/java-zookeeper
        try {
            // The call to ZooKeeper.exists() checks for the existence of the znode
            if (zoo.exists(zooPathServer, false) == null) {
                // Path, data, access control list (perms), znode type (ephemeral = delete upon
                // client DC)
                zoo.create(zooPathServer, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                logger.info("Succesfully created ZNode on serverside at zooPathServer: " + zooPathServer);

            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to create ZK ZNode: ", e);
        }

        // Handle metadata
        handleMetadata();

        // try {
        //     // Given path, do we need to watch node, stat of node
        //     byte[] adminMessageBytes = zoo.getData(zooPathServer, new Watcher() {
        //         // See https://zookeeper.apache.org/doc/r3.1.2/javaExample.html
        //         public void process(WatchedEvent we) {
        //             if (running == false) {
        //                 return;
        //             } else {
        //                 try {
        //                     String adminMessageString = new String(zoo.getData(zooPathServer, this, null), StandardCharsets.UTF_8);
        //                     handleAdminMessageHelper(adminMessageString);
        //                 } catch (KeeperException | InterruptedException e) {
        //                     logger.error("Failed to process admin message: ", e);
        //                 }
        //             }
        //         }
        //     }, null);

        //     ECSNode node = getECSNode(adminMessageBytes);
        //     // M2 Cache implementation - grab cache info from ECSNode
        //     this.cacheSize = node.getCacheSize();
        //     // TODO Check if this works to convert enum to string
        //     this.strategy = node.getCacheStrategy().name();
        //     this.cache = new kvCacheOperator(cacheSize, strategy);

        //     // Process the admin Message
        //     String adminMessageString = new String(adminMessageBytes, StandardCharsets.UTF_8);
        //     handleAdminMessageHelper(adminMessageString);
        // } catch (KeeperException | InterruptedException e) {
        //     logger.error("Failed to process ZK metadata: ", e);
        // }

        // // Start main thread
        // newThread = new Thread(this);
        // newThread.start();
    }

    /**
     * Helper function to handle ZK metadata and send to adminMessageHelper
     */
    public void handleMetadata() {
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
                            // Try again
                            handleMetadata();
                        } catch (Exception e) {
                            logger.error("Failed to process admin message bytes: ", e);
                        }
                    }
                }
            }, null);
            
            logger.info("Finished getting adminMessage in handleMetadata()!");

            // ECSNode node = getECSNode(adminMessageBytes);

            // // M2 Cache implementation - grab cache info from ECSNode
            // this.cacheSize = node.getCacheSize();
            // // TODO Check if this works to convert enum to string
            // this.strategy = node.getCacheStrategy().name();
            // this.cache = new kvCacheOperator(cacheSize, strategy);

            // logger.info("Finished getting cache info from metadata!");

            String adminMessageString = new String(adminMessageBytes, StandardCharsets.UTF_8);
            handleAdminMessageHelper(adminMessageString);

            syncLatch.await();

        } catch (KeeperException e1) {
            logger.error(e1);
        } catch (InterruptedException e2) {
            logger.error(e2);
        }
    }


    /**
     * Helper function to get ECS Node from admin message
     * @param adminMessageBytes Input bytes of admin message
     * @return
     */
    public ECSNode getECSNode(byte [] adminMessageBytes){
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

        ECSNode node = (ECSNode)ECSObject;
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
        }
        else{
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
            if (value != null){
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
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    // ********************** Milestone 2 Modifications **********************

    /**
     *  Returns boolean for server mode (distributed or not)
     * @return True if distributed, False if non-distributed
     */
    // public boolean distributed(){
	// 	return distributedMode;
	// }

    /**
     * Helper function to get current status of the server
     */
    @Override
    public ServerStatus getStatus() {
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
        logger.info("Started the KVServer, all client requests and all ECS requests are processed.");
    }

    @Override
    public void stop() {
        status = ServerStatus.STOP;
        logger.info("Stopped the KVServer, all client requests are rejected and only ECS requests are processed.");
    }

    @Override
    public void shutDown() {
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
    public boolean getLockWrite(){ 
		return locked;
	}
    

    // /**
    //  * Transfer a subset (range) of the KVServer’s data to another KVServer 
    //  * (reallocation before removing this server or adding a new KVServer to the ring); 
    //  * send a notification to the ECS, if data transfer is completed.
    //  * 
    //  * @param adminMessageString Admin message string from communications
    //  */
    // @Override
    // public void moveData(String adminMessageString) {
    //     // Process incoming admin message
    //     AdminMessage incomingMessage = new AdminMessage(adminMessageString);
    //     Map<String, Metadata> incomingMetadataMap = incomingMessage.getMsgMetadata();
    //     Metadata incomingMetadata = incomingMetadataMap.get(hashedName);

    //     // ************ Move data to target server ************

    //     // Original start
    //     BigInteger originalBegin = localMetadata.getHashStart();
    //     // Original Stop
    //     BigInteger originalEnd = localMetadata.getHashStop();

	// 	Map<String, String> invalidKVPairs = null;
        
    //     if (localMetadata != null && !localMetadata.stop.equals(incomingMetadata.stop)){
	// 		BigInteger stop = serverMetadata.getHashStop;
	// 		BigInteger newStop = serverMetadatasMap.get(stop.toString()).getHashStop;
	// 		invalidKVPairs = hashReachable(stop, newStop);
	// 	}

    
    //     // Acquire write lock
    //     lockWrite();

    //     // Get unreachable entries based on current hash range
    //     Map<String, String> unreachableEntries = storage.hashUnreachable(begin, end);
    //     // Iterate through unreachable entries
    //     Iterator itr = unreachableEntries.entrySet().iterator();


    //     // Get metadata of destination server
    //     Metadata transferServerMetadata = allMetadata.get(end.toString());
    //     // Build destination server name
    //     String transferServerName = zooPathRoot + "/" + transferServerMetadata.getHost() + ":"
    //             + transferServerMetadata.getPort();
    //     try {
    //         // Send admin message to destination
    //         // Need to confirm enums in MessageType, if TRANSFER_DATA available
    //         sendMessage(MessageType.TRANSFER_DATA, null, unreachableEntries, transferServerName);
    //     } catch (InterruptedException | KeeperException e) {
    //         logger.error("Failed to send admin message with unreachable entries: ", e);
    //     }

    //     // Remove unreachable KV Pairs from this server
    //     while (itr.hasNext()) {
    //         Map.Entry keyVal = (Map.Entry) itr.next();
    //         String key = (String) keyVal.getKey();
    //         if (!storage.keyValid(storage.MD5Hash(key), begin, end)) {
    //             storage.delete(key);
    //         } else {
    //             logger.error("Failed to remove unreachable KV pair from disk - reachable conflict!");
    //         }
    //     }

    //     // Release write lock
    //     unLockWrite();
    // }



    /**
     * Update metadata, move entries as required
     * 
     * @param adminMessageString Admin message string from communications
     */
    @Override
    public void update(String adminMessageString) {
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

        // ************ Move data to correct server ************
        BigInteger begin = localMetadata.getHashStart();
        BigInteger end = localMetadata.getHashStop();

        // Acquire write lock
        lockWrite();

        // Get unreachable entries based on current hash range
        Map<String, String> unreachableEntries = storage.hashUnreachable(begin, end);

        // Get metadata of destination server
        Metadata transferServerMetadata = allMetadata.get(end.toString());
        // Build destination server name
        String transferServerName = zooPathRoot + "/" + transferServerMetadata.getHost() + ":"
                + transferServerMetadata.getPort();
        try {
            // Send admin message to destination
            // Need to confirm enums in MessageType, if TRANSFER_DATA available
            sendMessage(MessageType.TRANSFER_DATA, null, unreachableEntries, transferServerName);
        } catch (InterruptedException | KeeperException e) {
            logger.error("Failed to send admin message with unreachable entries: ", e);
        }

        // TODO - need to receive confirmation of data transfer complete from ECSNode
        // Iterate through unreachable entries and remove from storage
        Iterator itr = unreachableEntries.entrySet().iterator();

        while (itr.hasNext()) {
            Map.Entry keyVal = (Map.Entry) itr.next();
            String key = (String) keyVal.getKey();
            if (!storage.keyValid(begin, end, storage.MD5Hash(key))) {
                // Remove unreachable KV Pairs from disk
                //storage.delete(key);
                // Cached version
                try{
                    putKV(key, "");
                }
                catch(Exception e){
                    logger.error("Failed to PUT from distributed server UPDATE: " + e);
                }
            } else {
                logger.error("Failed to remove unreachable KV pair from disk - reachable conflict!");
            }
        }

        // Release write lock
        unLockWrite();
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
                //storage.delete(entry.getKey());
                // Cached version
                // Cached version
                try{
                    putKV(entry.getKey().toString(), "");
                }
                catch(Exception e){
                    logger.error("Failed to PUT DELETE incoming data transfer from distributed server: " + e);
                }
            }
            // Write new entries to disk
            else {
                // No cache (old version)
                //storage.put(entry.getKey().toString(), entry.getValue().toString());
                // Cached version
                try{
                    putKV(entry.getKey().toString(), entry.getValue().toString());
                }
                catch(Exception e){
                    logger.error("Failed to PUT incoming data transfer from distributed server: " + e);
                }
            }
        }
        // Release write lock
        unLockWrite();
    }

    public boolean distributed(){
		return distributedMode;
	}
    
    /**
     * Send new admin message to destination servers
     * 
     * @param type        Message type
     * @param metadata    Metadata map to be sent
     * @param data        New KV entries to be transfered
     * @param destination Name of destination server (full name: (root/host/port))
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void sendMessage(MessageType type, Map<String, Metadata> metadata, Map<String, String> data,
            String destination) throws KeeperException, InterruptedException {
        AdminMessage toSend = new AdminMessage(type, metadata, data);
        zoo.setData(destination, toSend.toBytes(), zoo.exists(destination, false).getVersion());
        logger.info("Sent KV Transfer Message to: " + destination);
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
            // If first time running handleAdminMessageHelper, cache hasn't been initialized yet..
            if (this.strategy == null){
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
                update(adminMessageString);
            } else if (incomingMessageType == MessageType.START) {
                logger.info("Got admin message START!");
                start();
            } else if (incomingMessageType == MessageType.STOP) {
                logger.info("Got admin message STOP!");
                stop();
            } else if (incomingMessageType == MessageType.SHUTDOWN) {
                logger.info("Got admin message SHUTDOWN!");
                shutDown();
            }

            // else if (incomingMessageType == MessageType.LOCKWRITE){
            // lockWrite();
            // }
            // else if (incomingMessageType == MessageType.UNLOCKWRITE){
            // unLockWrite();
            // }

            // Incoming data transfer from another server
            else if (incomingMessageType == MessageType.TRANSFER_DATA) {
                logger.info("Got admin message TRANSFER_DATA (receiving an incoming data transfer)!");
                processDataTransfer(adminMessageString);
            }

            // // Transfer a subset of data to another server
            // else if (incomingMessageType == MessageType.MOVE_DATA){
            //     moveData(adminMessageString);
            // }

            // Update metadata repository for this server, shift entries if needed
            else if (incomingMessageType == MessageType.UPDATE) {
                logger.info("Got admin message UPDATE (update metadata)!");
                update(adminMessageString);
            }
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
        System.out.println("KVServer running!");

        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: M1: Server <port> <cachesize> <cachetype>!\n M2: Server <name> <port> <host>");
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
