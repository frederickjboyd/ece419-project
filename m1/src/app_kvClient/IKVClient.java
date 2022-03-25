package app_kvClient;

import java.io.IOException;
import java.net.UnknownHostException;

import client.KVStore;

public interface IKVClient {
    /**
     * Creates a new connection to hostname:port
     * 
     * @throws Exception
     *                   when a connection to the server can not be established
     */
    public void newConnection(String hostname, int port) throws Exception;

    /**
     * Get the current instance of the Store object
     * 
     * @return instance of KVStore
     */
    public KVStore getStore();
}