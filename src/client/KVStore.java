package client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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
    private List<String> serverInUse = new ArrayList<>();

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
            logger.error("Unknown Host!");
            throw new UnknownHostException();
        } catch (IllegalArgumentException e) {
            logger.error("Illegal Argument!");
            throw new IllegalArgumentException();
        } catch (IOException e) {
            logger.error("Could not establish connection, check server!", e);
            throw new IOException();
        } catch (Exception e) {
            logger.error("Other exception, maybe check server", e);
            throw new Exception();
        }
        return;
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

        // current implementation for m3
        try{
            kvmessageReceived = kvCommunication.receiveMessage();
        } catch (Exception e){
            kvmessageReceived = relocateServer(kvmessage);
		}
        
        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_STOPPED) {
            logger.info("get: current server stopped, trying to relocate");
            kvmessageReceived = relocateServer(kvmessage);
            // System.out.println(12);
        } 
        // server crash not in use
        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_CRASHED) { // or SERVER_CRASHED
            System.out.println("Server crashed, trying to connect to other server..");
            logger.error("current server crashed");
            kvmessageReceived = relocateServer(kvmessage);
        }

        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
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

        try{
            kvmessageReceived = kvCommunication.receiveMessage();
        } catch (Exception e){
            kvmessageReceived = relocateServer(kvmessage);
        }
        
        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_STOPPED) {
            logger.info("current server stopped, trying to relocate");
            kvmessageReceived = relocateServer(kvmessage);
            // System.out.println(12);
        } 
        
        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            checkAndUpdateServer(kvmessageReceived, key);
            kvCommunication.sendMessage(kvmessage);
            kvmessageReceived = kvCommunication.receiveMessage();
        }

        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_CRASHED) { // or SERVER_CRASHED
            System.out.println("Server crashed, trying to connect to other server..");
            logger.error("current server crashed");
            kvmessageReceived = relocateServer(kvmessage);
        }
        

        return kvmessageReceived;
    }

    public List<String> getServerInUseList() {
        return serverInUse;
    }

    public String getCurrentAddress() {
        return this.address;
    }

    public int getCurrentPort() {
        return this.port;
    }
    
    public void checkAndUpdateServer(KVMessage kvmessageReceived, String key) {
        System.out.println("checkAndUpdateServer in progress");

        if (kvmessageReceived.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            // if the server is not the supposed to be server then request metadata update
            metadata = kvmessageReceived.updateMetadata(this.address);
            BigInteger hexkeyInt = null;

            try {
                hexkeyInt = KeyDigest.getHashedKey(key);
            } catch (NoSuchAlgorithmException e) {
                logger.error("Error in generating MD5 hash!");
            }

            String originServerAddress = address;
            int originServerPort = port;
            int i;
            String tempStt="";
            Metadata temp=null;
            int j=0;

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
                        this.address = address;
                        this.port = port;
                        String infoMsg = String.format("Metadata updated and switched to server %s and port:%s",
                                address, port);
                        System.out.println(infoMsg);
                        logger.info(infoMsg);

                        
                        System.out.println("----------- Server currently running -------------");

                        serverInUse = new ArrayList<>();
                        for (j=0; j<metadata.size();j++){
                            temp=metadata.get(j);
                            tempStt=("host: " + temp.getHost()+ " port:"+ temp.getPort());
                            System.out.println(tempStt);
                            tempStt=(temp.getHost()+ ":"+ temp.getPort());
                            serverInUse.add(tempStt);
                            // displayInfo.add(tempStt);
                        }
                        // System.out.println(serverInUse);

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

    public KVMessage relocateServer(KVMessage kvmessage) throws Exception {
        System.out.println("Server not found or crashed, searching and reconnecting");
        logger.info("Server not found or crashed, searching and reconnecting");

        // metadata = KVMessage(StatusType.SERVER_CRASHED,"", "").updateMetadata(this.address);

        if (metadata == null) {
            System.out.println("No other server available");
            throw new Exception("Metadata empty, no server available");
        }

        KVMessage kvmessageReceived = null;
        String tempStt="";
        Metadata temp=null;
        int j=0;

        int i;
        for (i = 0; i < metadata.size(); i++) {
            Metadata obj = metadata.get(i);
            this.address = obj.getHost();
            this.port = obj.getPort();

            try {
                connect();
                kvCommunication.sendMessage(kvmessage);
			    kvmessageReceived = kvCommunication.receiveMessage();
                metadata = kvmessageReceived.updateMetadata(this.address);

                String infoMsg = String.format("Switched to server %s and port:%s",
                        this.address, this.port);
                logger.info(infoMsg);
                System.out.println("----------- Server currently running -------------");

                serverInUse = new ArrayList<>();
                for (j=0; j<metadata.size();j++){
                    temp=metadata.get(j);
                    tempStt=("host: " + temp.getHost()+ " port:"+ temp.getPort());
                    System.out.println(tempStt);
                    tempStt=(temp.getHost()+ ":"+ temp.getPort());
                    serverInUse.add(tempStt);
                }
                // System.out.println(serverInUse);

                return kvmessageReceived;
            } catch(Exception e){}
            // break;
        }
        if (i >= metadata.size()) {
            System.out.println("No other server available");
            throw new Exception("No server available, please check server connection");
        }
        return kvmessageReceived;
    }

    
}
