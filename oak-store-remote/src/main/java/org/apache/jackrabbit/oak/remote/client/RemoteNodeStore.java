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
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.remote.client.persistence.TailingPersistence;
import org.apache.jackrabbit.oak.remote.common.CommitInfoUtil;
import org.apache.jackrabbit.oak.remote.proto.ChangeEventProtos;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CreateCheckpointRequest;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos.Commit;
import org.apache.jackrabbit.oak.remote.proto.LeaseProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.split.SplitPersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeObserver;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.remote.common.PropertySerializer.toProtoProperty;

public class RemoteNodeStore implements NodeStore, Closeable, Observable {

    private static final Logger log = LoggerFactory.getLogger(RemoteNodeStore.class);

    private final RemoteNodeStoreClient client;

    private final BlobStore blobStore;

    private final CompositeObserver compositeObserver;

    private final StreamObserver observerStreamEvent;

    private final RemoteNodeStoreContext context;

    private LeaseProtos.LeaseInfo leaseInfo;

    private volatile LeaseProtos.ClusterView lastClusterView;

    private ScheduledExecutorService leaseRenewProcess = Executors.newScheduledThreadPool(1);

    private FileStore fileStore;

    public static class Builder {

        private RemoteNodeStoreClient client;

        private BlobStore blobStore;

        private AzurePersistence sharedPersistence;

        private SegmentNodeStorePersistence localPersistence;

        public Builder setClient(RemoteNodeStoreClient client) {
            this.client = client;
            return this;
        }

        public Builder setBlobStore(BlobStore blobStore) {
            this.blobStore = blobStore;
            return this;
        }

        public Builder setSharedPersistence(AzurePersistence sharedPersistence) {
            this.sharedPersistence = sharedPersistence;
            return this;
        }

        public Builder setLocalPersistence(SegmentNodeStorePersistence localPersistence) {
            this.localPersistence = localPersistence;
            return this;
        }

        public RemoteNodeStore build() throws IOException, InvalidFileStoreVersionException {
            return new RemoteNodeStore(this);
        }
    }

    private RemoteNodeStore(Builder builder) throws IOException, InvalidFileStoreVersionException {
        this.client = builder.client;
        this.blobStore = builder.blobStore;
        this.compositeObserver = new CompositeObserver();

        SplitPersistence splitPersistence = new SplitPersistence(new TailingPersistence(builder.sharedPersistence, client.getSegmentService()), builder.localPersistence);

        fileStore = FileStoreBuilder.fileStoreBuilder(Files.createTempDir())
                .withBlobStore(blobStore)
                .withCustomPersistence(splitPersistence)
                .build();

        leaseInfo = client.getLeaseService().acquire(Empty.getDefaultInstance());
        lastClusterView = client.getLeaseService().renew(leaseInfo);
        leaseRenewProcess.scheduleAtFixedRate(() -> renewLease(), 2, 2, TimeUnit.SECONDS);
        context = new RemoteNodeStoreContext(fileStore);

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
    public RemoteNodeState getRoot() {
        NodeStateId id = client.getNodeStoreService().getRoot(Empty.getDefaultInstance());
        return createNodeState(id);
    }

    @Override
    public synchronized @NotNull NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook, @NotNull CommitInfo info) throws CommitFailedException {
        RemoteNodeBuilder nodeBuilder = assertRootBuilder(builder);
        NodeState head = nodeBuilder.getNodeState();
        NodeState base = nodeBuilder.getBaseState();

        CommitFailedException ex = null;
        for (int i = 0; i < 5; i++) {
            RemoteNodeState rootState = getRoot();

            if (!RemoteNodeState.fastEquals(rootState, nodeBuilder.getBaseState())) {
                nodeBuilder.reset(rootState);
                head.compareAgainstBaseState(base, new ConflictAnnotatingRebaseDiff(nodeBuilder));
            }
            NodeState afterHooks;
            try {
                afterHooks = commitHook.processCommit(nodeBuilder.getBaseState(), nodeBuilder.getNodeState(), info);
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

            AtomicReference<NodeStateId> id = new AtomicReference<>();
            Object monitor = new Object();
            StreamObserver<CommitProtos.CommitEvent> observer = client.getNodeStoreAsyncService().merge(new StreamObserver<NodeStateId>() {
                @Override
                public void onNext(NodeStateId nodeStateId) {
                    id.set(nodeStateId);
                }

                @Override
                public void onError(Throwable throwable) {
                    log.error("Error occurred", throwable);
                    synchronized (monitor) {
                        monitor.notify();
                    }
                }

                @Override
                public void onCompleted() {
                    synchronized (monitor) {
                        monitor.notify();
                    }
                }
            });
            observer.onNext(CommitProtos.CommitEvent.newBuilder().setCommit(createCommitObject(info, rootState)).build());
            afterHooks.compareAgainstBaseState(rootState, new NodeDiffSerializer(observer));
            observer.onCompleted();
            synchronized (monitor) {
                try {
                    monitor.wait();
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                    throw new IllegalStateException(e);
                }
            }
            if (Strings.isNullOrEmpty(id.get().getRevision())) {
                log.warn("Rebased to outdated root state, retrying");
                ex = new CommitFailedException(CommitFailedException.MERGE, 1, "Can't merge, revision on remote has been updated");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }

                continue;
            }
            NodeState mergedRoot = createNodeState(id.get());
            nodeBuilder.reset(mergedRoot);
            return mergedRoot;
        }
        throw ex;
    }

    @Override
    public @NotNull NodeState rebase(@NotNull NodeBuilder builder) {
        RemoteNodeBuilder nodeBuilder = assertRootBuilder(builder);
        NodeState head = nodeBuilder.getNodeState();
        NodeState base = nodeBuilder.getBaseState();
        RemoteNodeState newBase = getRoot();
        if (!RemoteNodeState.fastEquals(base, newBase)) {
            nodeBuilder.reset(newBase);
            head.compareAgainstBaseState(base, new ConflictAnnotatingRebaseDiff(nodeBuilder));
            head = nodeBuilder.getNodeState();
        }
        return head;
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        RemoteNodeBuilder nodeBuilder = assertRootBuilder(builder);
        NodeState root = getRoot();
        nodeBuilder.reset(root);
        return root;
    }

    private RemoteNodeBuilder assertRootBuilder(NodeBuilder builder) {
        if (!(builder instanceof RemoteNodeBuilder)) {
            throw new IllegalArgumentException("Invalid node builder: " + builder.getClass());
        }
        RemoteNodeBuilder nodeBuilder = (RemoteNodeBuilder) builder;
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

    private RemoteNodeState createNodeState(NodeStateId id) {
        String revision = id.getRevision();
        return new RemoteNodeState(context, revision);
    }

    @NotNull
    private CommitProtos.Commit createCommitObject(@NotNull CommitInfo info, RemoteNodeState root) {
        Commit.Builder commitBuilder = Commit.newBuilder();
        commitBuilder.setCommitInfo(CommitInfoUtil.serialize(info));
        commitBuilder.getRootIdBuilder().setRevision(root.getRevision());
        return commitBuilder.build();
    }

    public LeaseProtos.ClusterView getLastClusterView() {
        return lastClusterView;
    }

    private static class NodeDiffSerializer implements NodeStateDiff {

        private final StreamObserver<CommitProtos.CommitEvent> observer;

        private final String path;

        private CommitProtos.CommitEvent.Builder eventBuilder;

        public NodeDiffSerializer(StreamObserver<CommitProtos.CommitEvent> observer) {
            this.observer = observer;
            this.path = "/";
        }

        public NodeDiffSerializer(NodeDiffSerializer parent, String name) {
            this.observer = parent.observer;
            this.path = PathUtils.concat(parent.path, name);
        }

        private CommitProtos.NodeBuilderChange.Builder newChange() {
            eventBuilder = CommitProtos.CommitEvent.newBuilder();
            return eventBuilder.getChangeBuilder().setNodeBuilderPath(path);
        }

        private void apply() {
            observer.onNext(eventBuilder.build());
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            newChange()
                .getSetPropertyBuilder()
                .setProperty(toProtoProperty(after));
            apply();
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            newChange()
                    .getSetPropertyBuilder()
                    .setProperty(toProtoProperty(after));
            apply();
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            newChange()
                    .getRemovePropertyBuilder()
                    .setName(before.getName());
            apply();
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            newChange()
                    .getAddNodeBuilder()
                    .setChildName(name);
            apply();
            EmptyNodeState.compareAgainstEmptyState(after, new NodeDiffSerializer(this, name));
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            after.compareAgainstBaseState(before, new NodeDiffSerializer(this, name));
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            newChange()
                    .getRemoveNodeBuilder()
                    .setChildName(name);
            apply();
            return true;
        }
    }
}
