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
import shared.communication.IKVMessage;
import shared.communication.KVCommunicationClient;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;

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
    private KVCommunicationClient kvCommunication;

    private String address;
    private int port;

    public KVStore(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public void connect() throws Exception {
        try {
            clientSocket = new Socket(this.address, this.port);
            kvCommunication = new KVCommunicationClient(clientSocket);
            // System.out.println("Connection is established! Server address = "+
            // serverAddress +", port = "+serverPort);
            // logger.info("Connection is established! Server address = "+ serverAddress +",
            // port = "+serverPort);
        } catch (Exception e) {
            e.getMessage();
        }
    }

    public void disconnect() {
        if (isRunning()) {
            try {
                KVMessage kvmessage = new KVMessage(StatusType.DISCONNECT, "", "");
                kvCommunication.sendMessage(kvmessage);
                kvCommunication.receiveMessage();
                kvCommunication.disconnect();
                logger.debug("Disconnected from server.");
            } catch (Exception e) {
                System.out.println("Error! Close Socket Failed!");
                logger.error("Close Socket Failed!", e);
            }
        }
    }

    public KVMessage get(String key) throws Exception {
        KVMessage kvmessage = new KVMessage(StatusType.GET, key, "");
        kvCommunication.sendMessage(kvmessage);
        return kvCommunication.receiveMessage();
    }

    public KVMessage put(String key, String value) throws Exception {
        KVMessage kvmessage = new KVMessage(KVMessage.StatusType.PUT, key, value);
        kvCommunication.sendMessage(kvmessage);
        return kvCommunication.receiveMessage();
    }

    public boolean isRunning() {
        return (kvCommunication != null) && (kvCommunication.getIsOpen());
    }
}
