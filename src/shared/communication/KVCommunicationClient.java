package shared.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.communication.KVMessage;

public class KVCommunicationClient {
    private static Logger logger = Logger.getRootLogger();

    private Socket socket;
    private boolean isOpen;
    private InputStream in;
    private OutputStream out;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    public KVCommunicationClient(Socket socket) {
        logger.debug("KVCommunicationClient enter");

        this.socket = socket;
        this.isOpen = true;

        try {
            this.in = socket.getInputStream();
            this.out = socket.getOutputStream();
            logger.info("Opened connection.");
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        logger.debug("KVCommunicationClient exit");
    }

    public void sendMessage(KVMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        out.write(msgBytes);
        out.flush();
        logger.debug(String.format("SEND\nkey: %s\nvalue: %s", msg.getKey(), msg.getValue()));
    }

    private KVMessage receiveMessage() throws IOException {
        logger.debug("receiveMessage enter");

        int idx = 0;
        byte[] msgBytes = null;
        byte[] tmp = null;
        byte[] buffer = new byte[BUFFER_SIZE];

        // Read first char from stream
        byte read = (byte) in.read();
        boolean reading = true;

        while (read != 0xA && read != -1 && reading) {
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

            read = (byte) in.read();
        }

        if (msgBytes == null) {
            tmp = new byte[idx];
            System.arraycopy(buffer, 0, tmp, 0, idx);
        } else {
            tmp = new byte[msgBytes.length + idx];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(buffer, 0, tmp, msgBytes.length, idx);
        }

        msgBytes = tmp;

        // Build final string
        KVMessage msg = null;
        try {
            msg = new KVMessage(msgBytes);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        logger.debug(String.format("RECEIVE\nkey: %s\nvalue: %s", msg.getKey(), msg.getValue()));

        logger.debug("receiveMessage exit");

        return msg;
    }

    /**
     * Disconnect from the currently connected server.
     */
    public void disconnect() {
        isOpen = false;

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
    }
}
