package app_kvServer;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;
import shared.communication.KVCommunicationServer;
import persistent_storage.PersistentStorage;

// Milestone 2 Imports
import org.apache.zookeeper.*;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import ecs.IECSNode;
import java.math.BigInteger;
// For synchronization
import java.util.concurrent.CountDownLatch;
// TODO
import shared.communication.AdminMessage;
import shared.communication.KVMessage;
import shared.Metadata;
// Admin message type enum 
import shared.communication.AdminMessage.MessageType;



// Runnable for threading
public class KVServer implements IKVServer, Runnable {

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private int cacheSize;
    private String strategy;
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
    private String zooPathRoot = "/StorageServerRoot";
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
        this.cacheSize = cacheSize;

        // Not running as distributed system
        this.distributedMode = false;

        // Check if file directory exists
        File testFile = new File(dataDirectory);
        if (!testFile.exists()) {
            this.storage = new PersistentStorage();
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
     * @param name Name of the KVServer (ipaddress:port)
     * @param zooPort ZK port
     * @param zooHost ZK host
     */
    public KVServer(String name, int zooPort, String zooHost) {
        // Running as distributed system
        this.distributedMode = true;
        // Set server name
        this.name = name;
        this.locked = false;
        // Store list of client threads
        this.threadList = new ArrayList<Thread>();
        // Split server name to get port (provided in ipaddr:port format)
		this.port = Integer.parseInt(name.split(":")[1]);
        this.cacheSize = 0;

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
        try{
            // Latch to wait for completed action
            final CountDownLatch syncLatch = new CountDownLatch(1);
            this.zoo = new ZooKeeper(zooHost + ":" + zooPort, 5000, new Watcher(){
                public void process(WatchedEvent we){
                    if (we.getState() == KeeperState.SyncConnected){
                        // Countdown latch if we succesfully connected
                       syncLatch.countDown();
                    }
                }
            });
            // Blocks until current count reaches zero
            syncLatch.await();
        }
        catch (IOException | InterruptedException e){
            logger.error("Failed to initialize ZooKeeper client: "+ e);
        }

        // Create new ZNode - see https://www.baeldung.com/java-zookeeper
		try {
            // The call to ZooKeeper.exists() checks for the existence of the znode
			if (zoo.exists(zooPathServer, false) == null) {
                // Path, data, access control list (perms), znode type (ephemeral = delete upon client DC)
				zoo.create(zooPathServer, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Failed to create ZK ZNode: ",e);
		}
        // Handle metadata
		try {
            // Given path, do we need to watch node, stat of node
			byte[] adminMessageBytes = zoo.getData(zooPathServer, new Watcher(){
                // See https://zookeeper.apache.org/doc/r3.1.2/javaExample.html
				public void process(WatchedEvent we){
					if (running == false){
                        return;
                    }
                    else{
                        try {
                            String adminMessageString = new String(zoo.getData(zooPathServer, this, null), StandardCharsets.UTF_8);
                            // TODO
                            handleAdminMessageHelper(adminMessageString);
                        } catch (KeeperException | InterruptedException e){
                            logger.error("Failed to process admin message: ",e);
                        }
                    }
				}
			}, null);

            // TODO
			String adminMessageString = new String(adminMessageBytes, StandardCharsets.UTF_8);
			handleAdminMessageHelper(adminMessageString);
		} 
        catch (KeeperException | InterruptedException e) {
			logger.error("Failed to process ZK metadata: ",e);
		}
        // // Start main thread
        // newThread = new Thread(this);
        // newThread.start();
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
        // Skip for now
        return IKVServer.CacheStrategy.None;
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

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public String getKV(String key) throws Exception {
        String value = storage.get(key);
        if (value == null) {
            logger.error("Key: " + key + " cannot be found on storage!");
            throw new Exception("Failed to find key in storage!");
        } else {
            return value;
        }
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        //System.out.println("RECEIVED A PUT"+value);
        // If value was blank, delete
        if (value.equals("") || value == null){
            if (inStorage(key)){
                //System.out.println("****A blank value was PUT, delete key: "+key);
                // Delete key if no value was provided in put
                storage.delete(key);
            }
            else{
                logger.error("Tried to delete non-existent key: "+key);
                throw new Exception("Tried to delete non-existent key!");
            }

        }
        else if (!storage.put(key, value)) {
            logger.error("Failed to PUT (" + key + ',' + value + ") into map!");
            throw new Exception("Failed to put KV pair in storage!");
        }
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() {
        storage.wipeStorage();
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

    /**Server initialiation helper. Initializes socket on given port. */
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
     *  Helper function to get current status of the server
     */
    @Override
    public ServerStatus getStatus(){
        return status;
    }

    /**
     *  Helper function to get current status of the lock
     */
    @Override
    public boolean getLock(){
        return locked;
    }

    @Override
    public void start(){
        status = ServerStatus.START;
        logger.info("Started the KVServer, all client requests and all ECS requests are processed.");
    }

    @Override
    public void stop(){
        status = ServerStatus.STOP;
        logger.info("Stopped the KVServer, all client requests are rejected and only ECS requests are processed.");
    }

    @Override
    public void shutDown(){
        close();
    }

    @Override
    public void lockWrite(){
        locked = true;
        logger.info("ACQUIRE WRITE LOCK: Future write requests blocked for now!");
    }

    @Override
    public void unLockWrite(){
        locked = false;
        logger.info("RELEASE WRITE LOCK: Future write requests allowed for now!");
    }

    
    /**
     * Update metadata, move entries as required
     * @param adminMessageString Admin message string from communications
     */
    @Override
    public void update(String adminMessageString){
        // Process incoming admin message
        AdminMessage incomingMessage = new AdminMessage(adminMessageString);

        // TODO - check that getMsgTypeString is available
        // String incomingMessageType = incomingMessage.getMsgTypeString()
        // Update metadata map
        this.allMetadata = incomingMessage.getMsgMetadata();

        // for (String key: allMetadata.keySet()){
		// 	Metadata metadata = allMetadata.get(key);
		// }

        // Update local metadata for this server
		this.localMetadata = allMetadata.get(hashedName);

        // ************ Move data to correct server ************
        BigInteger begin = localMetadata.hashStart;
        BigInteger end = localMetadata.hashStop;

        // Acquire write lock
        lockWrite();

        // Get unreachable entries based on current hash range
		Map<String, String> unreachableEntries = storage.hashUnreachable(begin, end);
		// Iterate through unreachable entries 
        Iterator itr = unreachableEntries.entrySet().iterator();

		while (itr.hasNext()) {
			Map.Entry keyVal = (Map.Entry)itr.next();
			String key = (String)keyVal.getKey(); 
			if (!storage.hashReachable(storage.MD5Hash(key), begin, end)){
				// Remove unreachable KV Pairs from disk
                storage.delete(key);
            }
			else {
                logger.error("Failed to remove unreachable KV pair from disk - reachable conflict!")
			}
		}

        // Get metadata of destination server
		Metadata transferServerMetadata = allMetadata.get(end.toString());
        // Build destination server name
		String transferServerName = zooPathRoot + "/" + transferServerMetadata.host + ":" + transferServerMetadata.port;
		try {
            // Send admin message to destination
            // Need to confirm enums in MessageType, if TRANSFER_DATA available
			sendMessage(MessageType.TRANSFER_DATA, null, unreachableEntries, transferServerName);
		}
        catch (InterruptedException | KeeperException e){
            logger.error("Failed to send admin message with unreachable entries: ",e);
		}
        // Release write lock
        unLockWrite();
    }


    /**
     * Receive new KV Pairs and store into persistent storage
     * @param adminMessageString Incoming admin message string
     */
    @Override
    public void processDataTransfer(String adminMessageString){
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
            if (entry.getValue().toString().equals("")){
                storage.delete(entry.getKey());
            }
            // Write new entries to disk
            else {
                storage.put(entry.getKey().toString(), entry.getValue().toString());
            }
        }
        // Release write lock
        unLockWrite();
    }


    /**
     * Send new admin message to destination servers
     * @param type Message type
     * @param metadata Metadata map to be sent
     * @param data New KV entries to be transfered
     * @param destination Name of destination server (full name: (root/host/port))
     * @throws KeeperException
     * @throws InterruptedException
     */
	public void sendMessage(MessageType type, Map<String, Metadata> metadata, Map<String, String> data, String destination) throws KeeperException, InterruptedException{
		AdminMessage toSend = new AdminMessage(type, metadata, data);
		zoo.setData(destination, toSend.toBytes(), zoo.exists(destination, false).getVersion());
	}


    /**
     * Return local metadata variable
     */
	@Override
	public Metadata getLocalMetadata(){
		return localMetadata;
	}


    /**
     * Return global metadata map
     */
    @Override
	public Map<String, Metadata> getAllMetadata(){
		return allMetadata;
	}


    /**
     * Helper function for handling incoming admin message (route to appropriate case)
     * @param adminMessageString Incoming admin message
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void handleAdminMessageHelper(String adminMessageString) throws KeeperException, InterruptedException{
        // Do Nothing if blank message
        if (adminMessageString == null || adminMessageString.equals("")){
            return;
        }
        else{
            // TODO
            AdminMessage incomingMessage = new AdminMessage(adminMessageString);
            MessageType incomingMessageType = incomingMessage.getMsgType()

            // TODO - may need to block incoming requests, check this!
            if (incomingMessageType == MessageType.INIT){
                update(adminMessageString);
            }
            else if (incomingMessageType == MessageType.START){
                start();
            }
            else if (incomingMessageType == MessageType.STOP){
                stop();
            }
            else if (incomingMessageType == MessageType.SHUTDOWN){
                shutDown();
            }

            // else if (incomingMessageType == MessageType.LOCKWRITE){
            //     lockWrite();
            // }
            // else if (incomingMessageType == MessageType.UNLOCKWRITE){
            //     unLockWrite();
            // }

            // Incoming data transfer from another server
            else if (incomingMessageType == MessageType.TRANSFER_DATA){
                processDataTransfer();
            }
            // Update metadata repository for this server, shift entries if needed
            else if (incomingMessageType == MessageType.UPDATE){
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
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port> <cachesize> <cachetype>!\n Server <name> <port> <host>");
            } else {
                // M1 Standard Server
                try{
                    int port = Integer.parseInt(args[0]);
                    int cacheSize = Integer.parseInt(args[1]);
                    String strategy = args[2];
                    KVServer newKV = new KVServer(port, cacheSize, strategy);
                    newKV.run();
                }
                // M2 Distributed Server
                // String name, int zooPort, String zooHost
                catch (NumberFormatException e){
                    // String serverName = args[0];
                    // String zHost = args[2];
                    // int zPort = Integer.parseInt(args[1]);
                    KVServer newKV = new KVServer(args[0], Integer.parseInt(args[1]), args[2]);
                    newKv.run();
                }

            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Server <port> <cachesize> <cachetype>!\n Server <name> <port> <host>");
            System.exit(1);
        }
    }
}
