package shared.communication;

import java.io.IOException;
import java.net.Socket;
import java.security.InvalidParameterException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import shared.DebugHelper;
import shared.communication.KVCommunicationClient;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;

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
    public KVMessage handleMsg(KVMessage msg) {
        DebugHelper.logFuncEnter(logger);

        StatusType returnMsgType = null;
        String msgKey = msg.getKey(); // Should never change
        String returnMsgValue = "";

        switch (msg.getStatus()) {
            case GET:
                logger.trace("GET");
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
                if (msg.getValue().equals("")) { // Delete
                    logger.trace("PUT DELETE");
                    try {
                        server.putKV(msgKey, msg.getValue());
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

        // Build return message
        KVMessage returnMsg = null;
        try {
            returnMsg = new KVMessage(returnMsgType, msgKey, returnMsgValue);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        DebugHelper.logFuncExit(logger);

        return returnMsg;
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
