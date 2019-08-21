package org.apache.jackrabbit.oak.remote;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class RemoteNodeStoreTest extends AbstractRemoteNodeStoreTest {

    @Test
    public void simpleMergeTest() throws CommitFailedException {
        NodeBuilder builder = delegateNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        delegateNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState root = remoteNodeStore.getRoot();
        assertEquals("bar", root.getString("foo"));

        NodeBuilder builder2 = root.builder();
        builder2.setProperty("foo2", "bar2");
        remoteNodeStore.merge(builder2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertEquals("bar2", delegateNodeStore.getRoot().getString("foo2"));
    }

    @Test
    public void testBlob() throws IOException, CommitFailedException {
        NodeBuilder builder = remoteNodeStore.getRoot().builder();
        builder.setProperty("smallBlob", builder.createBlob(new ByteArrayInputStream(new byte[10])));
        builder.setProperty("largeBlob", builder.createBlob(new ByteArrayInputStream(new byte[10 * 1024])));
        remoteNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals(10, remoteNodeStore.getRoot().getProperty("smallBlob").getValue(Type.BINARY).length());
        assertEquals(10240, remoteNodeStore.getRoot().getProperty("largeBlob").getValue(Type.BINARY).length());
    }
}
