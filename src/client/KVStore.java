package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import app_kvClient.ClientSocketListener;

import shared.communication.KVMessage;

public class KVStore {
    /**
     * Initialize KVStore with address and port of KVServer
     * 
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */

    private Logger logger = Logger.getRootLogger();
    private Set<ClientSocketListener> listeners;
    private boolean running;

    private Socket clientSocket;

    private String address;
    private int port;

    public KVStore(String address, int port) {
    }

    public boolean isRunning() {
        // return (kvCommunication != null) && kvCommunication.isOpen();
        return true;
    }

    @Override
    public void connect() throws Exception {
        // TODO Auto-generated method stub
    }

    // @Override
    public void disconnect() {
        // TODO Auto-generated method stub
    }

    // @Override
    public KVMessage put(String key, String value) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    // @Override
    public KVMessage get(String key) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isRunning() {
        return running;
    }
}
