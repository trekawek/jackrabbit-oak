package org.apache.jackrabbit.oak.remote.client;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;

import java.io.IOException;
import java.io.InputStream;

public class RemoteNodeBuilder extends MemoryNodeBuilder {

    private final RemoteNodeStoreContext context;

    public RemoteNodeBuilder(RemoteNodeStoreContext context, RemoteNodeState base) {
        super(base);
        this.context = context;
    }

    protected RemoteNodeBuilder(RemoteNodeBuilder parent, String name) {
        super(parent, name);
        this.context = parent.context;
    }

    @Override
    protected RemoteNodeBuilder createChildBuilder(String name) {
        return new RemoteNodeBuilder(this, name);
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return new BlobStoreBlob(context.getBlobStore(), context.getBlobStore().writeBlob(stream));
    }

}
