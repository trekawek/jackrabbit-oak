package org.apache.jackrabbit.oak.remote.client;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.remote.common.SegmentWriteListener;
import org.apache.jackrabbit.oak.remote.common.persistence.TailingPersistence;
import org.apache.jackrabbit.oak.remote.proto.SegmentProtos;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.CompositeIOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.WrappingPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.split.SplitPersistence;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

public class TailingPersistenceFactory implements Closeable {

    private final CloudBlobContainer container;

    private final RemoteNodeStoreClient client;

    private final String sharedDirName;

    private final String privateDirName;

    private StreamObserver segmentStreamObserver;

    public TailingPersistenceFactory(CloudBlobContainer container,
                                     RemoteNodeStoreClient client,
                                     String sharedDirName,
                                     String privateDirName) {
        this.container = container;
        this.client = client;
        this.sharedDirName = sharedDirName;
        this.privateDirName = privateDirName;
    }

    public SegmentNodeStorePersistence create() throws IOException, URISyntaxException, StorageException {
        AzurePersistence sharedPersistence;
        AzurePersistence privatePersistence;
        try {
            sharedPersistence = new AzurePersistence(container.getDirectoryReference(sharedDirName));
            privatePersistence = new AzurePersistence(container.getDirectoryReference(privateDirName));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
        TailingPersistence tailingPersistence = new TailingPersistence(sharedPersistence, client.getSegmentService());
        segmentStreamObserver = client.getSegmentAsyncService().observeSegments(new StreamObserver<SegmentProtos.SegmentBlob>() {
            @Override
            public void onNext(SegmentProtos.SegmentBlob segmentBlob) {
                tailingPersistence.onNewSegment(segmentBlob);
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        });
        SplitPersistence splitPersistence = new SplitPersistence(tailingPersistence, privatePersistence);
        SegmentWriteListener listener = new SegmentWriteListener();
        listener.setDelegate(segmentBlob -> {
                        client.getSegmentService().newPrivateSegment(SegmentProtos.PrivateSegment.newBuilder()
                                .setSegmentStoreDir(privateDirName)
                                .setSegmentBlob(segmentBlob)
                                .build());
                    }
        );
        return new WrappingPersistence(splitPersistence) {
            @Override
            public SegmentArchiveManager createArchiveManager(boolean memoryMapping, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) throws IOException {
                IOMonitor effectiveIOMonitor = new CompositeIOMonitor(Arrays.asList(ioMonitor, listener));
                return splitPersistence.createArchiveManager(memoryMapping, offHeapAccess, effectiveIOMonitor, fileStoreMonitor, remoteStoreMonitor);
            }
        };
    }

    @Override
    public void close() throws IOException {
        segmentStreamObserver.onCompleted();
    }
}
