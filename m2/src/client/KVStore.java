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
    private List<Metadata> metadata;

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
        kvmessageReceived = kvCommunication.receiveMessage();

        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            // kvmessageReceived=checkServer(kvmessage);
            checkAndUpdateServer(kvmessageReceived, key);
            kvCommunication.sendMessage(kvmessage);
            kvmessageReceived = kvCommunication.receiveMessage();
        }

        return kvmessageReceived;
    }

    public KVMessage put(String key, String value) throws Exception {
        KVMessage kvmessage = new KVMessage(StatusType.PUT, key, value);
        kvCommunication.sendMessage(kvmessage);
        KVMessage kvmessageReceived = null;
        kvmessageReceived = kvCommunication.receiveMessage();

        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            // kvmessageReceived=checkServer(kvmessage);
            checkAndUpdateServer(kvmessageReceived, key);
            kvCommunication.sendMessage(kvmessage);
            kvmessageReceived = kvCommunication.receiveMessage();

        }

        return kvmessageReceived;
    }

    public void checkAndUpdateServer(KVMessage kvmessageReceived, String key) {

        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            // if the server is not the supposed to be server then request metadata update
            metadata = kvmessageReceived.updateMetadata();
            BigInteger hexkeyInt = null;

            try {
                hexkeyInt = KeyDigest.getHashedKey(key);
            } catch (NoSuchAlgorithmException e) {
                logger.error("Error in generating MD5 hash!");
            }

            String originServerAddress = address;
            int originServerPort = port;
            int i;

            for (i = 0; i < metadata.size(); i++) {
                Metadata obj = metadata.get(i);
                BigInteger begin = obj.getHashStart();
                BigInteger end = obj.getHashStop();

                if ((begin.compareTo(end) != 1) && (hexkeyInt.compareTo(begin) == 1) && (hexkeyInt.compareTo(end) == -1)
                        ||
                        (begin.compareTo(end) != -1) && (hexkeyInt.compareTo(begin) == -1)
                                && (hexkeyInt.compareTo(end) == -1)
                        ||
                        (begin.compareTo(end) != -1) && (hexkeyInt.compareTo(begin) == 1)
                                && (hexkeyInt.compareTo(end) == 1)) {
                    disconnect();
                    address = obj.getHost();
                    port = obj.getPort();

                    try {
                        connect();
                        String infoMsg = String.format("Metadata updated and switched to server %s and port:%s",
                                address, port);
                        System.out.println(infoMsg);
                        logger.info(infoMsg);

                    } catch (Exception e) {
                        address = originServerAddress;
                        port = originServerPort;

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

            if (i > metadata.size()) {
                String errorMsg = String
                        .format("updated metadata also doesn't have server or port corresponding to this action");
                System.out.println(errorMsg);
                logger.error(errorMsg);
                return;
            }
        }
        return;
    }

    public List<Metadata> returnCurrentMetadata() {
        return metadata;
    }

    public boolean isRunning() {
        return (kvCommunication != null) && (kvCommunication.getIsOpen());
    }

    public KVMessage checkServer(KVMessage msg) throws Exception {
        System.out.println("Server not found, searching and reconnecting");
        logger.info("Server not found, searching and reconnecting");

        if (metadata == null) {
            throw new Exception("Metadata empty, no server available");
        }

        KVMessage kvmessageReceived = null;

        int i;
        for (i = 0; i < metadata.size(); i++) {
            Metadata obj = metadata.get(i);
            this.address = obj.getHost();
            this.port = obj.getPort();

            try {
                connect();
                kvCommunication.sendMessage(msg);
                kvmessageReceived = kvCommunication.receiveMessage();

                String infoMsg = String.format("Switched to server %s and port:%s",
                        this.address, this.port);
                logger.info(infoMsg);
                return kvmessageReceived;

            } catch (Exception e) {
                logger.error("new connection failed, origin server and port connection restored");
                kvCommunication.sendMessage(msg);
                kvmessageReceived = kvCommunication.receiveMessage();
                // return kvmessageReceived;

            }
            System.out.println(i);
            // break;
        }

        if (i >= metadata.size()) {
            throw new Exception("No server available");

        }
        return kvmessageReceived;
    }
}
