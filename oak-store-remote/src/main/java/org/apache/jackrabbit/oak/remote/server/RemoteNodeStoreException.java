package org.apache.jackrabbit.oak.remote.server;

public class RemoteNodeStoreException extends Exception {

    public RemoteNodeStoreException(String message) {
        super(message);
    }

    public RemoteNodeStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public RemoteNodeStoreException(Throwable cause) {
        super(cause);
    }

}
