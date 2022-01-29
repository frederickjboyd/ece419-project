package app_kvServer;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import persistent_storage.PersistentStorage;
import shared.communication.KVCommunicationServer;

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

  /**
   * Start KV Server at given port
   *
   * @param port given port for storage server to operate
   * @param cacheSize specifies how many key-value pairs the server is allowed to keep in-memory
   * @param strategy specifies the cache replacement strategy in case the cache is full and there is
   *     a GET- or PUT-request on a key that is currently not contained in the cache. Options are
   *     "FIFO", "LRU", and "LFU".
   */
  public KVServer(int port, int cacheSize, String strategy) {
    // Store list of client threads
    this.threadList = new ArrayList<Thread>();
    this.port = port;
    this.serverSocket = null;
    this.cacheSize = cacheSize;

    // Check if file directory exists
    File testFile = new File(dataDirectory);
    if (!testFile.exists()) {
      this.storage = new PersistentStorage();
    }
    // if exists, load into persistentStorage
    else {
      this.storage = new PersistentStorage(databaseName);
    }

    // Start new client thread
    newThread = new Thread(this);
    newThread.start();
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
    // System.out.println("RECEIVED A PUT"+value);
    // If value was blank, delete
    if (value.equals("")) {
      if (inStorage(key)) {
        // System.out.println("****A blank value was PUT, delete key: "+key);
        // Delete key if no value was provided in put
        storage.delete(key);
      } else {
        logger.error("Tried to delete non-existent key: " + key);
        throw new Exception("Tried to delete non-existent key!");
      }

    } else if (!storage.put(key, value)) {
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

    running = initializeServer();

    if (serverSocket != null) {
      while (running) {
        try {
          Socket client = serverSocket.accept();

          // To be replaced
          KVCommunicationServer connection = new KVCommunicationServer(client, this);

          newThread = new Thread(connection);
          newThread.start();
          // Append new thread to global thread list
          threadList.add(newThread);

          logger.info(
              "Connected to "
                  + client.getInetAddress().getHostName()
                  + " on port "
                  + client.getPort());
        } catch (IOException e) {
          logger.error("Error! " + "Unable to establish connection. \n", e);
        }
      }
    }
    logger.info("Server stopped.");
  }

  private boolean initializeServer() {
    logger.info("Initialize server ...");
    try {
      serverSocket = new ServerSocket(port);
      logger.info("Server listening on port: " + serverSocket.getLocalPort());
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
      logger.error("Error! " + "Unable to close socket on port: " + port, e);
    }
  }

  @Override
  public void close() {
    running = false;
    try {
      for (int i = 0; i < threadList.size(); i++) {
        threadList.get(i).interrupt();
      }
      serverSocket.close();
    } catch (IOException e) {
      logger.error("Error! " + "Unable to close socket on port: " + port, e);
    }
  }

  /**
   * Main entry point for the echo server application.
   *
   * @param args contains the port number at args[0], cacheSize at args[1], strategy at args[2]
   */
  public static void main(String[] args) {
    try {
      new LogSetup("logs/server.log", Level.ALL);
      if (args.length != 3) {
        System.out.println("Error! Invalid number of arguments!");
        System.out.println("Usage: Server <port> <cachesize> <cachetype>!");
      } else {
        int port = Integer.parseInt(args[0]);
        int cacheSize = Integer.parseInt(args[1]);
        String strategy = args[2];
        new KVServer(port, cacheSize, strategy);
      }
    } catch (IOException e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      System.exit(1);
    } catch (NumberFormatException nfe) {
      System.out.println("Error! Invalid argument <port>! Not a number!");
      System.out.println("Usage: Server <port> <cachesize> <cachetype>!");
      System.exit(1);
    }
  }
}
