package shared.communication;

import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import shared.DebugHelper;
import shared.Metadata;

public class AdminMessage {
    public enum MessageType {
        INIT, // Initialize KVServer, but do not respond to clients yet
        START, // Start KVServer
        UPDATE, // KVServer needs to update metadata
        STOP, // Stop KVServer from responding to clients, but do not terminate it
        SHUTDOWN, // Terminate KVServer
        TRANSFER_DATA, // Request from a node to move its key-value pairs
        TRANSFER_DATA_COMPLETE // All key-value pairs have been successfully received
    }

    private static Logger logger = Logger.getRootLogger();

    private static final char SEP = 0x1F; // Unit separator

    private MessageType msgType;
    private Map<String, Metadata> msgMetadata;
    private Map<String, String> msgKeyValues;
    private String sendingServer;

    /**
     * Construct AdminMessage for most tasks.
     * 
     * @param msgType Type of AdminMessage
     */
    public AdminMessage(MessageType msgType) {
        DebugHelper.logFuncEnter(logger);
        this.msgType = msgType;
        DebugHelper.logFuncExit(logger);
    }

    /**
     * Construct AdminMessage for most sending metadata.
     * 
     * @param msgType     Type of AdminMessage
     * @param msgMetadata Metadata for all servers - keys are in ip:port format
     */
    public AdminMessage(MessageType msgType, Map<String, Metadata> msgMetadata) {
        DebugHelper.logFuncEnter(logger);
        this.msgType = msgType;
        this.msgMetadata = msgMetadata;
        DebugHelper.logFuncExit(logger);
    }

    /**
     * Construct AdminMessage for data transfers.
     * 
     * @param msgType       Type of AdminMessage
     * @param msgMetadata   Metadata for all servers - keys are in ip:port format
     * @param msgKeyValues  Map of key-value pairs that may need to be transferred
     * @param sendingServer Note which server is sending this message so that
     *                      recipient can reply
     */
    public AdminMessage(MessageType msgType, Map<String, Metadata> msgMetadata, Map<String, String> msgKeyValues,
            String sendingServer) {
        DebugHelper.logFuncEnter(logger);
        this.msgType = msgType;
        this.msgMetadata = msgMetadata;
        this.msgKeyValues = msgKeyValues;
        this.sendingServer = sendingServer;
        DebugHelper.logFuncExit(logger);
    }

    public AdminMessage(String msg) {
        DebugHelper.logFuncEnter(logger);

        logger.debug(String.format("Constructing AdminMessage with %s", msg));
        String[] tokens = msg.split(String.valueOf(SEP), 4);
        this.msgType = MessageType.valueOf(tokens[0].toUpperCase());

        Gson gson = new Gson();
        Type metadataMapType = new TypeToken<Map<String, Metadata>>() {
        }.getType();
        this.msgMetadata = gson.fromJson(tokens[1], metadataMapType);
        this.msgKeyValues = gson.fromJson(tokens[2], Map.class);

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Convert AdminMessage to byte array representation.
     * 
     * @return
     */
    public byte[] toBytes() {
        // Convert to string
        Gson gson = new Gson();
        String type = msgType.toString();
        String metadata = gson.toJson(msgMetadata);
        String keyValue = gson.toJson(msgKeyValues);
        String msgString = type + SEP + metadata + SEP + keyValue;

        return msgString.getBytes(StandardCharsets.US_ASCII);
    }

    public MessageType getMsgType() {
        return this.msgType;
    }

    public Map<String, Metadata> getMsgMetadata() {
        return this.msgMetadata;
    }

    public Map<String, String> getMsgKeyValue() {
        return this.msgKeyValues;
    }

    public String getSendingServer() {
        return this.sendingServer;
    }
}
