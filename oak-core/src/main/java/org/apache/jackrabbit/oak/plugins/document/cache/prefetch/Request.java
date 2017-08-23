package org.apache.jackrabbit.oak.plugins.document.cache.prefetch;

public class Request {

    /**
     * The type of the request.
     */
    enum RequestType {
        /**
         * Request for a single document.
         */
        FIND,

        /**
         * Request for a range of documents (typically a children of a node).
         */
        QUERY,

        /**
         * Request for a range of documents with an extra condition.
         */
        QUERY_WITH_INDEXED_PROPERTY
    }

    private final String threadName;

    private final RequestType type;

    private final String key;

    private final String keyFrom;

    private final String keyTo;

    private final String indexedProperty;

    private final long startValue;

    private Request(String threadName, RequestType type, String key, String keyFrom, String keyTo, String indexedProperty, long startValue) {
        this.threadName = threadName;
        this.type = type;
        this.key = key;
        this.keyFrom = keyFrom;
        this.keyTo = keyTo;
        this.indexedProperty = indexedProperty;
        this.startValue = startValue;
    }

    public static Request createFindRequest(String threadName, String key) {
        return new Request(threadName, RequestType.FIND, key, null, null, null, 0);
    }

    public static Request createQueryRequest(String threadName, String keyFrom, String keyTo) {
        return new Request(threadName, RequestType.QUERY, null, keyFrom, keyTo, null, 0);
    }

    public static Request createQueryRequest(String threadName, String keyFrom, String keyTo, String indexedProperty, long startValue) {
        return new Request(threadName, RequestType.QUERY_WITH_INDEXED_PROPERTY, null, keyFrom, keyTo, indexedProperty, startValue);
    }

    public RequestType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getKeyFrom() {
        return keyFrom;
    }

    public String getKeyTo() {
        return keyTo;
    }

    public String getIndexedProperty() {
        return indexedProperty;
    }

    public long getStartValue() {
        return startValue;
    }

    public String getThreadName() {
        return threadName;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("Request");
        if (type == RequestType.FIND) {
            result.append('[').append(key).append(']');
        }
        if (type == RequestType.QUERY || type == RequestType.QUERY_WITH_INDEXED_PROPERTY) {
            result.append('[').append(keyFrom).append("]...[").append(keyTo).append(']');
        }
        if (type == RequestType.QUERY_WITH_INDEXED_PROPERTY) {
            result.append('[').append(indexedProperty).append(">=").append(startValue).append(']');
        }
        return result.toString();
    }
}
