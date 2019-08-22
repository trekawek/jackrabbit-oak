package org.apache.jackrabbit.oak.remote;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class RemoteNodeStoreTest extends AbstractRemoteNodeStoreTest {

    @Test
    public void simpleMergeTest() throws CommitFailedException {
        NodeState root = remoteNodeStore.getRoot();
        NodeBuilder builder2 = root.builder();
        builder2.setProperty("foo", "bar");
        remoteNodeStore.merge(builder2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertEquals("bar", delegateNodeStore.getRoot().getString("foo"));
    }

    @Test
    @Ignore
    public void manyChangesTest() throws CommitFailedException {
        NodeState root = remoteNodeStore.getRoot();
        NodeBuilder builder = root.builder();
        NodeBuilder test = builder.child("test");
        for (int i = 0; i < 15_000; i++) {
            test.setProperty("foo_" + i, i);
        }

        root = remoteNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState testState = root.getChildNode("test");
        for (long i = 0; i < 15_000; i++) {
            assertEquals(i, (long) test.getProperty("foo_" + i).getValue(Type.LONG));
        }
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
