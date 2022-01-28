package shared.communication;

public interface IKVMessage {

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
    }

}
