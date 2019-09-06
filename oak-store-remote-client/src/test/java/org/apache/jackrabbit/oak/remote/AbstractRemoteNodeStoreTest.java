package org.apache.jackrabbit.oak.remote;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStore;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStoreClient;
import org.apache.jackrabbit.oak.remote.server.NodeStoreServer;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public abstract class AbstractRemoteNodeStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    private long index;

    protected SegmentNodeStore delegateNodeStore;

    protected NodeStoreServer server;

    protected RemoteNodeStore remoteNodeStore;

    @Before
    public void setup() throws Exception {
        container = azurite.getContainer("oak-test");

        FileDataStore fds = new FileDataStore();
        fds.setPath(folder.newFolder().getPath());
        BlobStore blobStore = new DataStoreBlobStore(fds);

        String name = "oak-test-" + (++index);

        InProcessServerBuilder inProcessServerBuilder = InProcessServerBuilder.forName(name);
        server = new NodeStoreServer(inProcessServerBuilder, container.getDirectoryReference(name), blobStore);
        delegateNodeStore = server.getNodeStore();
        server.start();

        InProcessChannelBuilder inProcessChannelBuilder = InProcessChannelBuilder.forName(name);
        RemoteNodeStoreClient client = new RemoteNodeStoreClient(inProcessChannelBuilder);
        remoteNodeStore = new RemoteNodeStore.Builder()
                .setBlobStore(blobStore)
                .setClient(client)
                .setCloudContainer(container)
                .setSharedDirName(name)
                .setPrivateDirName(name + "-private")
                .build();
    }

    @After
    public void teardown() throws IOException {
        remoteNodeStore.close();
        server.close();
    }
}
