package org.apache.jackrabbit.oak.remote.client;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class RemoteNodeStoreContext {

    private final SegmentIdProvider idProvider;

    private final SegmentReader reader;

    private final SegmentWriter writer;

    private final BlobStore blobStore;

    public RemoteNodeStoreContext(FileStore fileStore) {
        this.idProvider = fileStore.getSegmentIdProvider();
        this.reader = fileStore.getReader();
        this.writer = fileStore.getWriter();
        this.blobStore = fileStore.getBlobStore();
    }

    public SegmentNodeState loadNode(String recordId) {
        return reader.readNode(RecordId.fromString(idProvider, recordId));
    }

    public BlobStore getBlobStore() {
        return blobStore;
    }

    public SegmentWriter getSegmentWriter() {
        return writer;
    }
}
