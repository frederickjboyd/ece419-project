package shared.communication;

import java.nio.charset.StandardCharsets;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.communication.IKVMessage;

public class KVMessage implements IKVMessage {
    private static Logger logger = Logger.getRootLogger();

    private static final int MAX_KEY_SIZE_BYTES = 20;
    private static final int MAX_VALUE_SIZE_BYTES = 1024 * 120;
    public static final char SEP = 0x1F; // Unit separator

    private byte[] msgBytes;
    private StatusType statusType;
    private String key;
    private String value;

    /**
     * Constructor using byte array
     * 
     * @param msgBytes Message in byte array format
     * @throws Exception
     */
    public KVMessage(byte[] msgBytes) throws Exception {
        logger.debug("KVMessage(byte[]) enter");

        String rawString;
        this.msgBytes = new byte[msgBytes.length];
        System.arraycopy(msgBytes, 0, this.msgBytes, 0, msgBytes.length);

        // Convert byte array to string
        try {
            rawString = new String(this.msgBytes, StandardCharsets.US_ASCII);
        } catch (Exception e) {
            String errorMsg = "Unable to convert byte array to string.";
            logger.error(errorMsg);
            throw new Exception(errorMsg);
        }

        // Extract message type, key, value information
        splitAndSetMessageInfo(rawString, String.valueOf(SEP));

        logger.debug("statusType: " + this.statusType);
        logger.debug("key: " + this.key);
        logger.debug("value: " + this.value);

        isMessageLengthValid(this.key, this.value);

        logger.debug("KVMessage(byte[]) exit");
    }

    /**
     * Constructor using raw input string from user
     * 
     * @param rawInputString User's input string into client
     * @throws Exception
     */
    public KVMessage(String rawInputString) throws Exception {
        logger.debug("KVMessage(String) enter");
        String errorMsg;

        try {
            splitAndSetMessageInfo(rawInputString, "[ ]+");
        } catch (Exception e) {
            errorMsg = "Unable to extract message information.";
            logger.error(errorMsg);
            throw new Exception(errorMsg);
        }

        logger.debug("statusType: " + this.statusType);
        logger.debug("key: " + this.key);
        logger.debug("value: " + this.value);

        isMessageLengthValid(this.key, this.value);

        try {
            String formattedString = this.statusType.toString() + SEP + key + SEP + value + SEP;
            this.msgBytes = formattedString.getBytes(StandardCharsets.US_ASCII);
        } catch (Exception e) {
            errorMsg = "Unable to convert string to byte array.";
            logger.error(errorMsg);
            throw new Exception(errorMsg);
        }

        logger.debug("KVMessage(String) exit");
    }

    /**
     * Constructor using explicit definitions of message parameters
     * 
     * @param statusType Type of message (e.g. GET, PUT)
     * @param key        Unique key identifying message
     * @param value      Message itself
     * @throws Exception
     */
    public KVMessage(StatusType statusType, String key, String value) throws Exception {
        logger.debug("KVMessage(StatusType, String, String) enter");

        isMessageLengthValid(key, value);

        try {
            String msgStr = statusType.toString() + SEP + key + SEP + value + SEP;
            this.msgBytes = msgStr.getBytes(StandardCharsets.US_ASCII);
        } catch (Exception e) {
            String errorMsg = "Unable to convert input parameters to byte array.";
            logger.error(errorMsg);
            throw new Exception(errorMsg);
        }

        this.statusType = statusType;
        this.key = key;
        this.value = value;

        logger.debug("statusType: " + this.statusType);
        logger.debug("key: " + this.key);
        logger.debug("value: " + this.value);

        logger.debug("KVMessage(StatusType, String, String) exit");
    }

    /**
     * Split a message into its type, key, and value.
     * Then, assign them to associated class members.
     * 
     * @param rawMessage Message in string format.
     * @param separator  Where to split string.
     */
    private void splitAndSetMessageInfo(String rawMessage, String separator) {
        logger.debug("splitAndSetMessageInfo enter");

        String[] rawStringArr = rawMessage.split(separator, 4);
        // statusType needs to match enum value (case sensitive)
        this.statusType = StatusType.valueOf(rawStringArr[0].toUpperCase());
        this.key = rawStringArr[1];
        this.value = rawStringArr[2];

        logger.debug("splitAndSetMessageInfo exit");
    }

    /**
     * Check that a message's key and value lengths are valid.
     * 
     * @param key
     * @param value
     * @return True is valid
     * @throws Exception
     */
    private boolean isMessageLengthValid(String key, String value) throws Exception {
        logger.debug("isMessageLengthValid enter");

        int keySizeBytes = key.getBytes(StandardCharsets.US_ASCII).length;
        int valueSizeBytes = value.getBytes(StandardCharsets.US_ASCII).length;
        String errorMsg;

        if (keySizeBytes > MAX_KEY_SIZE_BYTES) {
            errorMsg = String.format("Key length of %s bytes exceeds limit of %s bytes.", keySizeBytes,
                    MAX_KEY_SIZE_BYTES);
            logger.error(errorMsg);
            throw new Exception(errorMsg);
        }

        if (valueSizeBytes > MAX_VALUE_SIZE_BYTES) {
            errorMsg = String.format("Value length of %s bytes exceeds limit of %s bytes.", valueSizeBytes,
                    MAX_VALUE_SIZE_BYTES);
            logger.error(errorMsg);
            throw new Exception(errorMsg);
        }

        logger.debug("isMessageLengthValid exit");

        return true;
    }

    /**
     * @return The byte array associated with this message.
     */
    public byte[] getMsgBytes() {
        return this.msgBytes;
    }

    /**
     * @return The key that is associated with this message,
     *         null if not key is associated.
     */
    public String getKey() {
        return this.key;
    }

    /**
     * @return The value that is associated with this message,
     *         null if not value is associated.
     */
    public String getValue() {
        return this.value;
    }

    /**
     * @return A status string that is used to identify request types,
     *         response types, and error types associated to the message.
     */
    public StatusType getStatus() {
        return this.statusType;
    }
}
