package app_kvClient;

import java.io.IOException;
import java.net.UnknownHostException;

import client.KVCommInterface;

public interface IKVClient {
    /**
     * Creates a new connection to hostname:port
     * 
     * @throws Exception
     *                   when a connection to the server can not be established
     */
    public void newConnection(String hostname, int port) throws UnknownHostException, IOException;

    /**
     * Get the current instance of the Store object
     * 
     * @return instance of KVCommInterface
     */
    public KVCommInterface getStore();
}
