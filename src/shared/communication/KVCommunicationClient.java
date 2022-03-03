package shared.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import shared.DebugHelper;

/**
 * Main class for communication.
 */
public class KVCommunicationClient {
    private static Logger logger = Logger.getRootLogger();

    private Socket socket;
    private boolean isOpen;
    private InputStream in;
    private OutputStream out;
    private ObjectOutputStream objOut;
    private ObjectInputStream objIn;

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
            // Need to initialize ObjectOutputStream before ObjectInputStream, otherwise
            // program gets stuck
            this.objOut = new ObjectOutputStream(this.out);
            this.objIn = new ObjectInputStream(this.in);
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

        objOut.writeObject(msg);
        objOut.flush();
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
        KVMessage msg = null;

        try {
            msg = (KVMessage) objIn.readObject();
        } catch (Exception e) {
            logger.error("Unable to read object input stream.");
            e.getStackTrace();
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
            if (objIn != null) {
                objIn.close();
            }
            if (objOut != null) {
                objOut.close();
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
