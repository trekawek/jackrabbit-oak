package org.apache.jackrabbit.oak.remote;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStore;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStoreClient;
import org.apache.jackrabbit.oak.remote.server.NodeStoreServer;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
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

    protected FileStore fs;

    protected SegmentNodeStore delegateNodeStore;

    protected NodeStoreServer server;

    protected RemoteNodeStore remoteNodeStore;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException, URISyntaxException, InvalidKeyException, StorageException {
        container = azurite.getContainer("oak-test");

        FileDataStore fds = new FileDataStore();
        fds.setPath(folder.newFolder().getPath());
        BlobStore blobStore = new DataStoreBlobStore(fds);

        String name = "oak-test-" + (++index);

        fs = FileStoreBuilder.fileStoreBuilder(folder.newFolder())
                .withBlobStore(blobStore)
                .withCustomPersistence(new AzurePersistence(container.getDirectoryReference(name)))
                .build();
        delegateNodeStore = SegmentNodeStoreBuilders.builder(fs).build();

        InProcessServerBuilder inProcessServerBuilder = InProcessServerBuilder.forName(name);
        server = new NodeStoreServer(inProcessServerBuilder, delegateNodeStore, fs, blobStore);
        server.start();

        InProcessChannelBuilder inProcessChannelBuilder = InProcessChannelBuilder.forName(name);
        RemoteNodeStoreClient client = new RemoteNodeStoreClient(inProcessChannelBuilder);
        remoteNodeStore = new RemoteNodeStore.Builder()
                .setBlobStore(blobStore)
                .setClient(client)
                .setLocalPersistence(new TarPersistence(folder.newFolder()))
                .setSharedPersistence(new AzurePersistence(container.getDirectoryReference(name)))
                .build();
    }

    @After
    public void teardown() throws IOException {
        remoteNodeStore.close();
        server.stop();
        fs.close();
    }
}
