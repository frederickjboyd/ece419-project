package app_kvClient;

public interface ClientSocketListener {
    public enum SocketStatus {
        CONNECTED, DISCONNECTED, CONNECTION_LOST
    };
}
