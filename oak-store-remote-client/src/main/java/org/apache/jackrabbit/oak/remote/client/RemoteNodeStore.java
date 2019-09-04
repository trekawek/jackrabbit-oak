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
package org.apache.jackrabbit.oak.remote.client;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.google.protobuf.Empty;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.remote.common.persistence.TailingPersistence;
import org.apache.jackrabbit.oak.remote.common.CommitInfoUtil;
import org.apache.jackrabbit.oak.remote.common.SegmentWriteListener;
import org.apache.jackrabbit.oak.remote.proto.ChangeEventProtos;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CreateCheckpointRequest;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos.Commit;
import org.apache.jackrabbit.oak.remote.proto.LeaseProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.SegmentProtos;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.split.SplitPersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeObserver;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RemoteNodeStore implements NodeStore, Closeable, Observable {

    private static final Logger log = LoggerFactory.getLogger(RemoteNodeStore.class);

    private final RemoteNodeStoreClient client;

    private final BlobStore blobStore;

    private final CompositeObserver compositeObserver;

    private final StreamObserver observerStreamEvent;

    private final String privateDirName;

    private final StreamObserver segmentStreamObserver;

    private LeaseProtos.LeaseInfo leaseInfo;

    private volatile LeaseProtos.ClusterView lastClusterView;

    private ScheduledExecutorService leaseRenewProcess = Executors.newScheduledThreadPool(1);

    private FileStore fileStore;

    public static class Builder {

        private RemoteNodeStoreClient client;

        private BlobStore blobStore;

        private CloudBlobContainer cloudBlobContainer;

        private String sharedDirName;

        private String privateDirName;

        public Builder setClient(RemoteNodeStoreClient client) {
            this.client = client;
            return this;
        }

        public Builder setBlobStore(BlobStore blobStore) {
            this.blobStore = blobStore;
            return this;
        }

        public Builder setCloudContainer(CloudBlobContainer cloudBlobContainer) {
            this.cloudBlobContainer = cloudBlobContainer;
            return this;
        }

        public Builder setSharedDirName(String sharedDirName) {
            this.sharedDirName = sharedDirName;
            return this;
        }

        public Builder setPrivateDirName(String privateDirName) {
            this.privateDirName = privateDirName;
            return this;
        }

        public RemoteNodeStore build() throws IOException, InvalidFileStoreVersionException, URISyntaxException, StorageException, CommitFailedException {
            return new RemoteNodeStore(this);
        }
    }

    private RemoteNodeStore(Builder builder) throws IOException, InvalidFileStoreVersionException, URISyntaxException, StorageException, CommitFailedException {
        this.client = builder.client;
        this.blobStore = builder.blobStore;
        this.privateDirName = builder.privateDirName;
        this.compositeObserver = new CompositeObserver();

        AzurePersistence sharedPersistence;
        AzurePersistence privatePersistence;
        try {
            sharedPersistence = new AzurePersistence(builder.cloudBlobContainer.getDirectoryReference(builder.sharedDirName));
            privatePersistence = new AzurePersistence(builder.cloudBlobContainer.getDirectoryReference(builder.privateDirName));
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
        fileStore = FileStoreBuilder.fileStoreBuilder(Files.createTempDir())
                .withBlobStore(blobStore)
                .withCustomPersistence(splitPersistence)
                .withIOMonitor(listener)
                .build();
        createHead(fileStore);

        listener.setDelegate(segmentBlob -> {
                    client.getSegmentService().newPrivateSegment(SegmentProtos.PrivateSegment.newBuilder()
                            .setSegmentStoreDir(privateDirName)
                            .setSegmentBlob(segmentBlob)
                            .build());
                }
        );

        leaseInfo = client.getLeaseService().acquire(Empty.getDefaultInstance());
        lastClusterView = client.getLeaseService().renew(leaseInfo);
        leaseRenewProcess.scheduleAtFixedRate(() -> renewLease(), 2, 2, TimeUnit.SECONDS);

        observerStreamEvent = client.getNodeStoreAsyncService().observe(new StreamObserver<ChangeEventProtos.ChangeEvent>() {
            @Override
            public void onNext(ChangeEventProtos.ChangeEvent changeEvent) {
                NodeState root = createNodeState(changeEvent.getNodeStateId());
                compositeObserver.contentChanged(root, CommitInfoUtil.deserialize(changeEvent.getCommitInfo()));
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        });
    }

    private void createHead(FileStore fileStore) throws CommitFailedException, IOException {
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty(":initialized", true);
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fileStore.flush();
    }

    private void renewLease() {
        LeaseProtos.ClusterView response = client.getLeaseService().renew(leaseInfo);
        if (Strings.isNullOrEmpty(response.getId())) {
            log.error("Lost the lease, acquiring a new one");
            leaseInfo = client.getLeaseService().acquire(Empty.getDefaultInstance());
            return;
        }
        lastClusterView = response;
    }

    public void close() throws IOException {
        observerStreamEvent.onCompleted();
        segmentStreamObserver.onCompleted();
        leaseRenewProcess.shutdown();
        try {
            leaseRenewProcess.awaitTermination(1, TimeUnit.MINUTES);
            this.client.getLeaseService().release(leaseInfo);
            this.client.shutdown();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        fileStore.close();
    }

    @Override
    public Closeable addObserver(Observer observer) {
        compositeObserver.addObserver(observer);
        return () -> compositeObserver.removeObserver(observer);
    }

    @Override
    @NotNull
    public SegmentNodeState getRoot() {
        NodeStateId id = client.getNodeStoreService().getRoot(Empty.getDefaultInstance());
        return createNodeState(id);
    }

    @Override
    public synchronized @NotNull NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook, @NotNull CommitInfo info) throws CommitFailedException {
        SegmentNodeBuilder nodeBuilder = assertRootBuilder(builder);
        NodeState head = nodeBuilder.getNodeState();
        NodeState base = nodeBuilder.getBaseState();

        CommitFailedException ex = null;
        for (int i = 0; i < 5; i++) {
            SegmentNodeState rootState = getRoot();

            if (!SegmentNodeState.fastEquals(rootState, nodeBuilder.getBaseState())) {
                nodeBuilder.reset(rootState);
                head.compareAgainstBaseState(base, new ConflictAnnotatingRebaseDiff(nodeBuilder));
            }

            SegmentNodeState baseNodeState = (SegmentNodeState) nodeBuilder.getBaseState();
            SegmentNodeState headNodeState;
            try {
                headNodeState = (SegmentNodeState) commitHook.processCommit(baseNodeState, nodeBuilder.getNodeState(), info);
            } catch (CommitFailedException e) {
                log.warn("Hooks failed", e);
                ex = e;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    log.error("Interrupted", e1);
                }
                continue;
            }

            try {
                fileStore.getWriter().flush();
            } catch (IOException e) {
                log.error("Can't flush", e);
                continue;
            }

            NodeStateId id = client.getNodeStoreService().merge(createCommitObject(info, baseNodeState, headNodeState));

            if (Strings.isNullOrEmpty(id.getRevision())) {
                log.warn("Rebased to outdated root state, retrying");
                ex = new CommitFailedException(CommitFailedException.MERGE, 1, "Can't merge, revision on remote has been updated");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
                continue;
            }

            NodeState mergedRoot = createNodeState(id);
            nodeBuilder.reset(mergedRoot);
            return mergedRoot;
        }
        throw ex;
    }

    @Override
    public @NotNull NodeState rebase(@NotNull NodeBuilder builder) {
        SegmentNodeBuilder nodeBuilder = assertRootBuilder(builder);
        NodeState head = nodeBuilder.getNodeState();
        NodeState base = nodeBuilder.getBaseState();
        SegmentNodeState newBase = getRoot();
        if (!SegmentNodeState.fastEquals(base, newBase)) {
            nodeBuilder.reset(newBase);
            head.compareAgainstBaseState(base, new ConflictAnnotatingRebaseDiff(nodeBuilder));
            head = nodeBuilder.getNodeState();
        }
        return head;
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        SegmentNodeBuilder nodeBuilder = assertRootBuilder(builder);
        NodeState root = getRoot();
        nodeBuilder.reset(root);
        return root;
    }

    private SegmentNodeBuilder assertRootBuilder(NodeBuilder builder) {
        if (!(builder instanceof SegmentNodeBuilder)) {
            throw new IllegalArgumentException("Invalid node builder: " + builder.getClass());
        }
        SegmentNodeBuilder nodeBuilder = (SegmentNodeBuilder) builder;
        if (!nodeBuilder.isRootBuilder()) {
            throw new IllegalArgumentException("Not a root builder: " + builder.getClass());
        }
        return nodeBuilder;
    }

    @Override
    public @NotNull Blob createBlob(InputStream inputStream) throws IOException {
        return getBlob(blobStore.writeBlob(inputStream));
    }

    @Override
    public @Nullable Blob getBlob(@NotNull String reference) {
        return new BlobStoreBlob(blobStore, reference);
    }

    @Override
    public @NotNull String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
        CreateCheckpointRequest.Builder builder = CreateCheckpointRequest.newBuilder().setLifetime(lifetime);
        builder.getInfoBuilder().putAllCheckpointInfo(properties);
        return client.getCheckpointService().createCheckpoint(builder.build()).getId();
    }

    @Override
    public @NotNull String checkpoint(long lifetime) {
        CreateCheckpointRequest.Builder builder = CreateCheckpointRequest.newBuilder().setLifetime(lifetime);
        return client.getCheckpointService().createCheckpoint(builder.build()).getId();
    }

    @Override
    public @NotNull Map<String, String> checkpointInfo(@NotNull String checkpoint) {
        CheckpointProtos.CheckpointId checkpointId = CheckpointProtos.CheckpointId.newBuilder().setId(checkpoint).build();
        CheckpointProtos.CheckpointInfo info = client.getCheckpointService().getCheckpointInfo(checkpointId);
        return info.getCheckpointInfoMap();
    }

    @Override
    public @NotNull Iterable<String> checkpoints() {
        return client.getCheckpointService().getCheckpointList(Empty.getDefaultInstance())
                .getCheckpointIdList()
                .stream()
                .map(CheckpointProtos.CheckpointId::getId)
                .collect(Collectors.toList());
    }

    @Override
    public @Nullable NodeState retrieve(@NotNull String checkpoint) {
        CheckpointProtos.CheckpointId checkpointId = CheckpointProtos.CheckpointId.newBuilder().setId(checkpoint).build();
        NodeStateId nodeStateId = client.getCheckpointService().retrieveCheckpoint(checkpointId);
        return createNodeState(nodeStateId);
    }

    @Override
    public boolean release(@NotNull String checkpoint) {
        CheckpointProtos.CheckpointId checkpointId = CheckpointProtos.CheckpointId.newBuilder().setId(checkpoint).build();
        return client.getCheckpointService().releaseCheckpoint(checkpointId).getValue();
    }

    private SegmentNodeState createNodeState(NodeStateId id) {
        String revision = id.getRevision();
        RecordId recordId = RecordId.fromString(fileStore.getSegmentIdProvider(), revision);
        return fileStore.getReader().readNode(recordId);
    }

    @NotNull
    private CommitProtos.Commit createCommitObject(@NotNull CommitInfo info, SegmentNodeState baseNodeState, SegmentNodeState headNodeState) {
        Commit.Builder commitBuilder = Commit.newBuilder();
        commitBuilder.setCommitInfo(CommitInfoUtil.serialize(info));
        commitBuilder.getBaseNodeStateBuilder().setRevision(baseNodeState.getRevision());
        commitBuilder.getHeadNodeStateBuilder().setRevision(headNodeState.getRevision());
        commitBuilder.setSegmentStoreDir(privateDirName);
        return commitBuilder.build();
    }

    public LeaseProtos.ClusterView getLastClusterView() {
        return lastClusterView;
    }
}
