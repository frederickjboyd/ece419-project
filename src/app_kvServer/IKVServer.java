package app_kvServer;

import java.util.Map;

import shared.Metadata;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    // For Milestone 2 - status of KVServer
    public enum ServerStatus {
        START, // Starts the KVServer, all client requests and all ECS requests are processed.
        STOP, // Stops the KVServer, all client requests are rejected and only ECS requests
              // are processed.
        SHUTDOWN // Exits the KVServer application.
    }

    // Milestone 1 Commands

    /**
     * Get the port number of the server
     * 
     * @return port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * 
     * @return hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * 
     * @return cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * 
     * @return cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * 
     * @return true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * 
     * @return true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * 
     * @return value associated with key
     * @throws Exception
     *                   when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * 
     * @throws Exception
     *                   when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

    // ********** Milestone 2 Commands **********

    /**
     * Get server status
     * 
     * @return START, STOP, SHUTDOWN
     */
    public ServerStatus getStatus();

    /**
     * Get write lock status
     * 
     * @return True if locked, False if unlocked
     */
    public boolean getLock();

    public void start();

    public void stop();

    public void shutDown();

    public void lockWrite();

    public void unLockWrite();

    public boolean getLockWrite();

    /**
     * Update server metadata, shift entries as required
     */
    public void update(String adminMessageString);

    /**
     * Process incoming data transfer
     */
    public void processDataTransfer(String adminMessageString);

    public Metadata getLocalMetadata();

    public Map<String, Metadata> getAllMetadata();
}
