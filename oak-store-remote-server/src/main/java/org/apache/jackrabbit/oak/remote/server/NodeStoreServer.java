/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.remote.server;

import com.google.common.io.Closer;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.remote.common.SegmentWriteListener;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static com.google.common.io.Files.createTempDir;

public class NodeStoreServer implements Closeable  {

    private static final Logger log = LoggerFactory.getLogger(NodeStoreServer.class);

    private final Server server;

    private final FileStore fileStore;

    private final SegmentNodeStore nodeStore;

    private final Closer closer = Closer.create();

    public NodeStoreServer(int port, CloudBlobDirectory sharedSegmentStoreDir, BlobStore blobStore) throws URISyntaxException, StorageException, IOException, InvalidFileStoreVersionException {
        this(ServerBuilder.forPort(port), sharedSegmentStoreDir, blobStore);
    }

    public NodeStoreServer(ServerBuilder<?> serverBuilder, CloudBlobDirectory sharedSegmentStoreDir, BlobStore blobStore) throws URISyntaxException, StorageException, IOException, InvalidFileStoreVersionException {
        SegmentWriteListener segmentWriteListener = new SegmentWriteListener();
        this.fileStore = createFileStore(sharedSegmentStoreDir, blobStore, segmentWriteListener);
        init(fileStore);
        this.nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        PrivateFileStores privateFileStores = new PrivateFileStores(sharedSegmentStoreDir, blobStore);
        this.server = serverBuilder
                .addService(new CheckpointService(nodeStore))
                .addService(new NodeStoreService(nodeStore, fileStore, privateFileStores))
                .addService(new LeaseService(nodeStore))
                .addService(new SegmentService(segmentWriteListener, fileStore, privateFileStores))
                .build();
    }

    private FileStore createFileStore(CloudBlobDirectory sharedSegmentStoreDir, BlobStore blobStore, SegmentWriteListener listener) throws IOException, InvalidFileStoreVersionException {
        File dir = createTempDir();
        closer.register(() -> FileUtils.deleteDirectory(dir));
        SegmentNodeStorePersistence persistence;
        AzurePersistence azurePersistence = new AzurePersistence(sharedSegmentStoreDir);
        persistence = azurePersistence;
        FileStoreBuilder builder = FileStoreBuilder
                .fileStoreBuilder(dir)
                .withCustomPersistence(persistence)
                .withBlobStore(blobStore)
                .withIOMonitor(listener);
        FileStore fileStore = builder.build();
        closer.register(fileStore);
        return fileStore;
    }

    private void init(FileStore fileStore) throws IOException {
        try {
            SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = segmentNodeStore.getRoot().builder();
            builder.setProperty(":initialized", true);
            segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        } catch (CommitFailedException e) {
            throw new IOException(e);
        }
    }

    public SegmentNodeStore getNodeStore() {
        return nodeStore;
    }

    public void start() throws IOException {
        fileStore.flush(); // flush, to make the head segment available immediately
        server.start();
        log.info("Server started");
        closer.register(() -> server.shutdown());
    }

    public void close() throws IOException {
        closer.close();
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

}
