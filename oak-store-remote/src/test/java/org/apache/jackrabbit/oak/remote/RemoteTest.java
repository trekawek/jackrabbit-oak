package org.apache.jackrabbit.oak.remote;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStore;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStoreClient;
import org.apache.jackrabbit.oak.remote.server.NodeStoreServer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import java.io.IOException;
import java.rmi.server.RemoteServer;

import static org.junit.Assert.assertEquals;

public class RemoteTest {

    @Test
    public void test() throws IOException, CommitFailedException {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        MemoryBlobStore blobStore = new MemoryBlobStore();
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeStoreServer server = new NodeStoreServer(1234, nodeStore);
        server.start();

        RemoteNodeStoreClient client = new RemoteNodeStoreClient("localhost", 1234);
        RemoteNodeStore remoteNodeStore = new RemoteNodeStore(client, blobStore);
        NodeState root = remoteNodeStore.getRoot();
        assertEquals("bar", root.getString("foo"));

        NodeBuilder builder2 = root.builder();
        builder2.setProperty("foo2", "bar2");
        remoteNodeStore.merge(builder2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertEquals("bar2", nodeStore.getRoot().getString("foo2"));
    }

}
