package org.apache.jackrabbit.oak.remote;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStore;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStoreClient;
import org.apache.jackrabbit.oak.remote.server.NodeStoreServer;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public abstract class AbstractRemoteNodeStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private long index;

    private FileStore fs;

    protected SegmentNodeStore delegateNodeStore;

    protected NodeStoreServer server;

    protected RemoteNodeStore remoteNodeStore;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        MemoryBlobStore blobStore = new MemoryBlobStore();

        fs = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withBlobStore(blobStore).build();
        delegateNodeStore = SegmentNodeStoreBuilders.builder(fs).build();

        String name = "oak-test-" + (++index);
        InProcessServerBuilder inProcessServerBuilder = InProcessServerBuilder.forName(name);
        server = new NodeStoreServer(inProcessServerBuilder, delegateNodeStore);
        server.start();

        InProcessChannelBuilder inProcessChannelBuilder = InProcessChannelBuilder.forName(name);
        RemoteNodeStoreClient client = new RemoteNodeStoreClient(inProcessChannelBuilder);
        remoteNodeStore = new RemoteNodeStore(client, blobStore);
    }

    @After
    public void teardown() throws IOException {
        remoteNodeStore.close();
        server.stop();
        fs.close();
    }
}
