import java.net.Socket;

import shared.communication.KVCommunicationClient;

public class KVCommunicationServer extends KVCommunicationClient implements Runnable {
    public KVCommunicationServer(Socket socket) {
        super(socket);
    }

    public void run() {
        System.out.println("I am KVCommunicationServer!");
    }
}
