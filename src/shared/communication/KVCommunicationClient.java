package shared.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.DebugHelper;
import shared.communication.KVMessage;

/**
 * Main class for communication.
 */
public class KVCommunicationClient {
    private static Logger logger = Logger.getRootLogger();

    private Socket socket;
    private boolean isOpen;
    private InputStream in;
    private OutputStream out;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    /**
     * Constructor for client.
     * 
     * @param socket Endpoint for client to communicate with server
     */
    public KVCommunicationClient(Socket socket) {
        DebugHelper.logFuncEnter(logger);

        this.socket = socket;

        try {
            this.in = socket.getInputStream();
            this.out = socket.getOutputStream();
            logger.info("Opened connection.");
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        setIsOpen(true);

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Write and flush message to output stream.
     * 
     * @param msg
     * @throws IOException
     */
    public void sendMessage(KVMessage msg) throws IOException {
        DebugHelper.logFuncEnter(logger);

        byte[] msgBytes = msg.getMsgBytes();
        out.write(msgBytes);
        out.flush();
        logger.debug(String.format("SEND %s > key: %s || value: %s", msg.getStatus().toString(), msg.getKey(),
                msg.getValue()));

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Receive message from input stream, parse the bytes, and assemble a readable
     * message.
     * 
     * @return Message containing status, key, and value
     * @throws IOException
     */
    public KVMessage receiveMessage() throws IOException {
        DebugHelper.logFuncEnter(logger);

        int idx = 0;
        byte[] msgBytes = null;
        byte[] tmp = null;
        byte[] buffer = new byte[BUFFER_SIZE];

        // Read first char from stream
        byte read = (byte) in.read();
        boolean reading = true;
        int separatorAccum = 0;

        logger.trace("Entering while loop");

        while (reading && separatorAccum != 3) {
            logger.trace(String.format("idx %d: %d", idx, read));
            if (idx == BUFFER_SIZE) {
                if (msgBytes == null) {
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(buffer, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(buffer, 0, tmp, msgBytes.length, BUFFER_SIZE);
                }

                msgBytes = tmp;
                buffer = new byte[BUFFER_SIZE];
                idx = 0;
            }

            buffer[idx] = read;
            idx++;

            if (msgBytes != null && msgBytes.length + idx >= DROP_SIZE) {
                reading = false;
            }

            logger.trace("Reading next byte...");
            read = (byte) in.read();

            if (read == KVMessage.SEP) {
                separatorAccum++;
            }

            logger.trace(String.format("idx %d: %d", idx, read));
        }

        logger.trace("Exited while loop");

        if (msgBytes == null) {
            tmp = new byte[idx];
            System.arraycopy(buffer, 0, tmp, 0, idx);
        } else {
            tmp = new byte[msgBytes.length + idx];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(buffer, 0, tmp, msgBytes.length, idx);
        }

        msgBytes = tmp;

        logger.trace("Building final message");

        // Build final string
        KVMessage msg = null;
        try {
            msg = new KVMessage(msgBytes);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        logger.debug(String.format("RECEIVE %s > key: %s || value: %s", msg.getStatus().toString(), msg.getKey(),
                msg.getValue()));

        DebugHelper.logFuncExit(logger);

        return msg;
    }

    /**
     * Disconnect from the currently connected server.
     */
    public void disconnect() {
        DebugHelper.logFuncEnter(logger);

        setIsOpen(false);

        try {
            logger.info("Closing " + socket.getInetAddress().getHostAddress() + ":" + socket.getPort());

            if (socket != null) {
                socket.close();
            }
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }

            logger.info("Connection closed.");
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        DebugHelper.logFuncExit(logger);
    }

    /**
     * Get whether an instance is ready to send/receive messages.
     * 
     * @return True is ready, false otherwise
     */
    public boolean getIsOpen() {
        return this.isOpen;
    }

    /**
     * Set whether an instance should be able to send/receive messages.
     * 
     * @param newValue
     */
    public void setIsOpen(boolean newValue) {
        this.isOpen = newValue;
    }
}
