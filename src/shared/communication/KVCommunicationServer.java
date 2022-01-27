package shared.communication;

import java.io.IOException;
import java.net.Socket;
import java.security.InvalidParameterException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import shared.communication.KVCommunicationClient;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;

public class KVCommunicationServer extends KVCommunicationClient implements Runnable {
    private static Logger logger = Logger.getRootLogger();

    private KVServer server;

    public KVCommunicationServer(Socket socket, KVServer server) {
        super(socket);

        this.server = server;
    }

    public KVMessage handleMsg(KVMessage msg) {
        logger.debug("handleMsg enter");

        StatusType returnMsgType = null;
        String returnMsgKey = msg.getKey();
        String returnMsgValue = null;

        switch (msg.getStatus()) {
            case GET:
                try {
                    returnMsgValue = server.getKV(returnMsgKey);
                    returnMsgType = StatusType.GET_SUCCESS;
                    logger.info(String.format("%s: %s, %s", returnMsgType.toString(), returnMsgKey, returnMsgValue));
                } catch (Exception e) {
                    returnMsgType = StatusType.GET_ERROR;
                    logger.info(String.format("%s: %s", returnMsgType.toString(),
                            returnMsgKey));
                }
                break;

            case PUT:
                if (msg.getValue() == "") { // Delete
                    try {
                        // TODO: add code here
                    } catch (Exception e) {
                        returnMsgType = StatusType.DELETE_ERROR;
                        logger.info(
                                String.format("%s: %s", returnMsgType.toString(), returnMsgKey));
                    }
                } else { // Put
                    // Check if there is an existing key
                    try {
                        server.getKV(returnMsgKey);
                    } catch (Exception e) {
                        returnMsgType = StatusType.PUT; // New key-value pair
                    }

                    // Store/Update key-value pair
                    try {
                        server.putKV(msg.getKey(), msg.getValue());
                        returnMsgType = StatusType.PUT_SUCCESS;
                        returnMsgValue = msg.getValue();
                    } catch (Exception e) {
                        returnMsgType = StatusType.PUT_ERROR;
                    }

                    logger.info(String.format("%s: %s, %s", returnMsgType.toString(), returnMsgKey, returnMsgValue));
                }
                break;

            case DISCONNECT:
                setIsOpen(false);
                returnMsgType = StatusType.DISCONNECT;
                logger.info(String.format("%s", returnMsgType.toString()));
                break;

            default:
                String errorMsg = "Message's StatusType is unsupported!";
                logger.error(errorMsg);
                throw new InvalidParameterException(errorMsg);
        }

        logger.debug("handleMsg exit");

        // Build return message
        KVMessage returnMsg = null;
        try {
            returnMsg = new KVMessage(returnMsgType, returnMsgKey, returnMsgValue);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        return returnMsg;
    }

    public void run() {
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
    }
}
