package shared.communication;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;

import org.apache.log4j.Logger;
import shared.DebugHelper;

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
     * Constructor using byte array.
     * 
     * @param msgBytes Message in byte array format
     * @throws Exception
     */
    public KVMessage(byte[] msgBytes) throws Exception {
        DebugHelper.logFuncEnter(logger);

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
        isMessageASCII(this.key, this.value);

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Constructor using raw input string from user.
     * 
     * @param rawInputString User's input string into client
     * @throws Exception
     */
    public KVMessage(String rawInputString) throws Exception {
        DebugHelper.logFuncEnter(logger);

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
        isMessageASCII(this.key, this.value);

        try {
            String formattedString = this.statusType.toString() + SEP + key + SEP + value + SEP;
            this.msgBytes = formattedString.getBytes(StandardCharsets.US_ASCII);
        } catch (Exception e) {
            errorMsg = "Unable to convert string to byte array.";
            logger.error(errorMsg);
            throw new Exception(errorMsg);
        }

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Constructor using explicit definitions of message parameters.
     * 
     * @param statusType Type of message (e.g. GET, PUT)
     * @param key        Unique key identifying message
     * @param value      Message itself
     * @throws Exception
     */
    public KVMessage(StatusType statusType, String key, String value) throws Exception {
        DebugHelper.logFuncEnter(logger);

        isMessageLengthValid(key, value);
        isMessageASCII(key, value);

        try {
            String msgStr = statusType.toString() + SEP + key + SEP + value + SEP;
            logger.debug(String.format("msgStr: %s", msgStr));
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

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Split a message into its type, key, and value.
     * Then, assign them to associated class members.
     * 
     * @param rawMessage Message in string format.
     * @param separator  Where to split string.
     */
    private void splitAndSetMessageInfo(String rawMessage, String separator) {
        DebugHelper.logFuncEnter(logger);

        String[] rawStringArr = rawMessage.split(separator, 4);
        // statusType needs to match enum value (case sensitive)
        this.statusType = StatusType.valueOf(rawStringArr[0].toUpperCase());
        this.key = rawStringArr[1];
        this.value = rawStringArr[2];

        DebugHelper.logFuncExit(logger);
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
        DebugHelper.logFuncEnter(logger);
        String errorMsg;

        int keySizeBytes = key.getBytes(StandardCharsets.US_ASCII).length;
        int valueSizeBytes = value.getBytes(StandardCharsets.US_ASCII).length;
        logger.trace(String.format("keySizeBytes: %d", keySizeBytes));
        logger.trace(String.format("valueSizeBytes: %d", valueSizeBytes));

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

        DebugHelper.logFuncExit(logger);

        return true;
    }

    private boolean isMessageASCII(String key, String value) {
        boolean isASCII = true;
        String errorMsg;

        if (!Charset.forName("US-ASCII").newEncoder().canEncode(key)) {
            isASCII = false;
            errorMsg = String.format("Key %s contains non-ASCII characters.", key);
            logger.error(errorMsg);
            throw new InvalidParameterException(errorMsg);
        }

        if (!Charset.forName("US-ASCII").newEncoder().canEncode(value)) {
            isASCII = false;
            errorMsg = String.format("Value %s contains non-ASCII characters.", value);
            logger.error(errorMsg);
            throw new InvalidParameterException(errorMsg);
        }

        return isASCII;
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
