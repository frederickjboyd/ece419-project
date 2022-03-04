package client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.log4j.Logger;
import java.math.BigInteger;

import shared.communication.KVCommunicationClient;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;

import shared.KeyDigest;
import shared.Metadata;

public class KVStore {
    /**
     * Initialize KVStore with address and port of KVServer
     * 
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */

    private Logger logger = Logger.getRootLogger();
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
            logger.info("Connection established! Server address = " + this.address + ",port = " + this.port);
        } catch (NumberFormatException nfe) {
            logger.error("Unable to parse argument <port>", nfe);
            throw new NumberFormatException();
        } catch (UnknownHostException e) {
            // logger.error("Unknown Host!", e);
            throw new UnknownHostException();
        } catch (IllegalArgumentException e) {
            // logger.error("Unknown Host!", e);
            throw new IllegalArgumentException();
        } catch (IOException e) {
            logger.error("Could not establish connection!", e);
            throw new IOException();
        } catch (Exception e) {
            logger.error("Other exception", e);
            throw new Exception();
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
        KVMessage kvmessageReceived = null;

        try {
            kvmessageReceived = kvCommunication.receiveMessage();
        } catch (IOException e){
            logger.error("get msg receive failed!", e);
        }

        checkAndUpdateServer(kvmessageReceived, key);

        return kvmessageReceived;
    }

    public KVMessage put(String key, String value) throws Exception {
        System.out.println(key);
        System.out.println(value);

        KVMessage kvmessage = new KVMessage(StatusType.PUT, key, value);
        kvCommunication.sendMessage(kvmessage);
        KVMessage kvmessageReceived = null;
        kvmessageReceived = kvCommunication.receiveMessage();

        // try {
        //     kvmessageReceived = kvCommunication.receiveMessage();
        // } catch (IOException e){
        //     logger.error("put msg receive failed!", e);
        // }

        checkAndUpdateServer(kvmessageReceived, key);

        return kvmessageReceived;
    }

    public void checkAndUpdateServer(KVMessage kvmessageReceived, String key) {
        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            // if the server is not the supposed to be server then request metadata update
            List<Metadata> metadata = kvmessageReceived.updateMetadata();
            BigInteger hexkeyInt = null;

            try {
                hexkeyInt = KeyDigest.getHashedKey(key);
            } catch (NoSuchAlgorithmException e) {
                logger.error("Error in generating MD5 hash!");
            }

            if (metadata == null) {
                String errorMsg = String.format("Metadata is empty");
                System.out.println(errorMsg);
                logger.error(errorMsg);
                // throw new Exception("Metadata is empty");
            }

            String originServerAddress = this.address;
            int originServerPort = this.port;

            // Insert test case when metadata size is very small or dun contain required
            // server or port to see what's the end server and port and response
            int i;

            for (i = 0; i < metadata.size(); i++) {
                Metadata obj = metadata.get(i);
                // if request key is larger than start and smaller than end of current ith
                // server range
                if (hexkeyInt.compareTo(obj.getHashStart()) > 0 && hexkeyInt.compareTo(obj.getHashStop()) <= 0) { // Are
                                                                                                                  // there
                                                                                                                  // alternatives
                    // towards the range or
                    // has this be set
                    disconnect();
                    this.address = obj.getHost();
                    this.port = obj.getPort();

                    try {
                        connect();
                        kvCommunication.sendMessage(kvmessageReceived);
                        String infoMsg = String.format("Metadata updated and switched to server %s and port:%s",
                                this.address, this.port);
                        System.out.println(infoMsg);
                        logger.info(infoMsg);

                    } catch (Exception e) {
                        this.address = originServerAddress;
                        this.port = originServerPort;
                        try {
                            connect();
                        } catch (Exception ex) {
                            logger.error("connect() failed!");
                        }
                        logger.error("new connection failed, origin server and port connection restored");
                    }
                    // break;
                    return;
                }
            }

            if (i >= metadata.size()) {
                String errorMsg = String
                        .format("updated metadata also doesn't have server or port corresponding to this action");
                System.out.println(errorMsg);
                logger.error(errorMsg);
                // throw new Exception("Cannot find avaliable server to connect");
            }

        }
    }

    public boolean isRunning() {
        return (kvCommunication != null) && (kvCommunication.getIsOpen());
    }
}
