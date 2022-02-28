package shared.communication;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.security.InvalidParameterException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import shared.DebugHelper;
import shared.communication.KVCommunicationClient;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;
import shared.KeyDigest;
import java.security.NoSuchAlgorithmException;
import org.json.JSONObject;
import shared.Metadata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;



/**
 * Communication class with extra server-specific methods.
 */
public class KVCommunicationServer extends KVCommunicationClient implements Runnable {
    private static Logger logger = Logger.getRootLogger();
    private KVServer server;

    /**
     * Constructor for server.
     * 
     * @param socket Endpoint for server to communicate with client.
     * @param server Main server instance.
     */
    public KVCommunicationServer(Socket socket, KVServer server) {
        super(socket);
        DebugHelper.logFuncEnter(logger);

        this.server = server;

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Call appropriate server methods to handle a request.
     * 
     * @param msg Message from client
     * @return Message to return to client, which should indicate whether the
     *         request was successful or not
     */

    public boolean hashReachable(String key) {
        // Case 1: Begin <= End, key > begin, key < end
        // Case 2: Begin >= End, key < begin, key < end
        // Case 3: Begin >= End, key > begin, key > end
        BigInteger begin = server.getLocalMetadata().getHashStart();
        BigInteger end = server.getLocalMetadata().getHashStop();
        BigInteger mdKey;

        try {
            mdKey = KeyDigest.getHashedKey(key);
        } catch (NoSuchAlgorithmException e) {
            logger.error("Error in generating MD5 hash!");
            return false;
        }

        if ((begin.compareTo(end) != 1) && (mdKey.compareTo(begin) == 1) && (mdKey.compareTo(end) == -1) ||
                (begin.compareTo(end) != -1) && (mdKey.compareTo(begin) == -1) && (mdKey.compareTo(end) == -1) ||
                (begin.compareTo(end) != -1) && (mdKey.compareTo(begin) == 1) && (mdKey.compareTo(end) == 1)) {
            return true;
        } else {
            return false;
        }
    }

    public KVMessage handleMsg(KVMessage msg) {
        DebugHelper.logFuncEnter(logger);

        StatusType returnMsgType = null;
        String msgKey = msg.getKey(); // Should never change
        String returnMsgValue = "";
        KVMessage returnMsg = null;

        switch (msg.getStatus()) {
            case GET:
                logger.trace("GET");

                if (server.distributed() && !hashReachable(msgKey)) {
                    returnMsgType = StatusType.SERVER_NOT_RESPONSIBLE;
                    logger.info("server not responsible");
                    String sendMsgValue = getUpdatedMetadata(server).toString();

                    try {
                        returnMsg = new KVMessage(returnMsgType, msgKey, sendMsgValue);
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                    return returnMsg;

                }
                try {
                    returnMsgValue = server.getKV(msgKey);
                    returnMsgType = StatusType.GET_SUCCESS;
                    logger.info(String.format("%s: %s, %s", returnMsgType.toString(), msgKey, returnMsgValue));
                } catch (Exception e) {
                    returnMsgType = StatusType.GET_ERROR;
                    logger.error(String.format("%s: %s", returnMsgType.toString(), msgKey));
                }
                break;

            case PUT:
                if (server.distributed() && server.getLockWrite()){
                    returnMsgType =  StatusType.SERVER_WRITE_LOCK;
                    logger.info("server is locked for writing");

                    try {
                        returnMsg = new KVMessage(returnMsgType, msgKey, returnMsgValue);
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                    
                    return returnMsg;
                }
                // check if server is responsible for this KV pair
                if (server.distributed() && !hashReachable(msgKey)) {
                    returnMsgType = StatusType.SERVER_NOT_RESPONSIBLE;
                    logger.info("server not responsible");
                    String sendMsgValue = getUpdatedMetadata(server).toString();

                    try {
                        returnMsg = new KVMessage(returnMsgType, msgKey, sendMsgValue);
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }

                    return returnMsg;
                }

                if (msg.getValue().equals("") || msg.getValue().equals("null")) { // Delete
                    logger.trace("PUT DELETE");

                    try {
                        server.putKV(msgKey, "");
                        returnMsgType = StatusType.DELETE_SUCCESS;
                    } catch (Exception e) {
                        returnMsgType = StatusType.DELETE_ERROR;
                        logger.error(String.format("%s: %s", returnMsgType.toString(), msgKey));
                    }
                } else { // Store/Update
                    logger.trace("PUT STORE/UPDATE");
                    // Check if there is an existing key
                    try {
                        server.getKV(msgKey);
                        returnMsgType = StatusType.PUT_UPDATE;
                        logger.trace("PUT UPDATE");
                    } catch (Exception e) {
                        returnMsgType = StatusType.PUT_SUCCESS;
                        logger.trace("PUT STORE");
                    }

                    // Store/Update key-value pair
                    try {
                        server.putKV(msgKey, msg.getValue());
                        returnMsgValue = msg.getValue();
                    } catch (Exception e) {
                        returnMsgType = StatusType.PUT_ERROR;
                    }

                    logger.info(String.format("%s: %s, %s", returnMsgType.toString(), msgKey, returnMsgValue));
                }
                break;

            case DISCONNECT:
                logger.trace("DISCONNECT");
                setIsOpen(false);
                returnMsgType = StatusType.DISCONNECT;
                break;

            default:
                String errorMsg = "Message's StatusType is unsupported!";
                logger.error(errorMsg);
                throw new InvalidParameterException(errorMsg);
        }

        // Build return message;
        try {
            returnMsg = new KVMessage(returnMsgType, msgKey, returnMsgValue);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        DebugHelper.logFuncExit(logger);

        return returnMsg;
    }

    public JSONObject getUpdatedMetadata(KVServer server) {
        Map<String, Metadata> updatedMetadata = server.getAllMetadata();
        JSONObject metadataJson = new JSONObject();
        // Make it a json string as value for the ease of passing in in kvmsg class
        int i = 0;
        for (Metadata metadataObj : updatedMetadata.values()){
            JSONObject obj = new JSONObject();
            obj.put("host", metadataObj.getHost());
            obj.put("port", metadataObj.getPort());
            obj.put("hashStart", metadataObj.getHashStart());
            obj.put("hashStop", metadataObj.getHashStop());
            metadataJson.put("metadata" + String.valueOf(i), obj);
            i++;
        }
        return metadataJson;

    }

    /**
     * Main loop to receive and handle messages.
     */
    public void run() {
        DebugHelper.logFuncEnter(logger);

        while (getIsOpen()) {
            try {
                KVMessage newMsg = receiveMessage();
                KVMessage returnMsg = handleMsg(newMsg);
                sendMessage(returnMsg);
            } catch (IOException io) {
                logger.error("Server lost connection!", io);
                setIsOpen(false);
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

        // Clean up
        disconnect();

        DebugHelper.logFuncExit(logger);
    }
}
