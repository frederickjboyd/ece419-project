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
        ACK, // Acknowledge message receipt
        INIT, // Initialize KVServer, but do not respond to clients yet
        START, // Start KVServer
        UPDATE, // KVServer needs to update metadata
        STOP, // Stop KVServer from responding to clients, but do not terminate it
        SHUTDOWN, // Terminate KVServer
        TRANSFER_DATA // Transfer data between nodes
    }

    private static Logger logger = Logger.getRootLogger();

    private static final char SEP = 0x1F; // Unit separator

    private MessageType msgType;
    private Map<String, Metadata> msgMetadata;
    private Map<String, String> msgKeyValue;

    public AdminMessage(MessageType msgType, Map<String, Metadata> msgMetadata, Map<String, String> msgKeyValue) {
        DebugHelper.logFuncEnter(logger);
        this.msgType = msgType;
        this.msgMetadata = msgMetadata;
        this.msgKeyValue = msgKeyValue;
        DebugHelper.logFuncExit(logger);
    }

    public AdminMessage(String msg) {
        DebugHelper.logFuncEnter(logger);

        String[] tokens = msg.split(Character.toString(SEP), 4);
        this.msgType = MessageType.valueOf(tokens[0].toUpperCase());

        Gson gson = new Gson();
        Type metadataMapType = new TypeToken<Map<String, Metadata>>() {
        }.getType();
        this.msgMetadata = gson.fromJson(tokens[1], metadataMapType);

        this.msgKeyValue = gson.fromJson(tokens[2], Map.class);

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
        String keyValue = gson.toJson(msgKeyValue);
        String msgString = type + SEP + metadata + keyValue;

        return msgString.getBytes(StandardCharsets.US_ASCII);
    }

    public MessageType getMsgType() {
        return this.msgType;
    }

    public Map<String, Metadata> getMsgMetadata() {
        return this.msgMetadata;
    }

    public Map<String, String> getMsgKeyValue() {
        return this.msgKeyValue;
    }
}
