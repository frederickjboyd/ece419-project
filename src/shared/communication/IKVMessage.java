package shared.communication;

public interface IKVMessage {

    /**
     * Statuses for sending/receiving messages.
     */
    public enum StatusType {
        GET, // Get - request
        GET_ERROR, // requested tuple (i.e. value) not found
        GET_SUCCESS, // requested tuple (i.e. value) found
        PUT, // Put - request
        PUT_SUCCESS, // Put - request successful, tuple inserted
        PUT_UPDATE, // Put - request successful, i.e. value updated
        PUT_ERROR, // Put - request not successful
        DELETE_SUCCESS, // Delete - request successful
        DELETE_ERROR, // Delete - request successful
        DISCONNECT, // Request to disconnect
        SERVER_NOT_RESPONSIBLE, // if the server didn't respond
        SERVER_WRITE_LOCK, // Write lock
        SERVER_STOPPED, // when server stopped

    }

}
