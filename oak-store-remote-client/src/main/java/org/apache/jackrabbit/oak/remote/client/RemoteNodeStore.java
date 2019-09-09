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
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.remote.common.CommitInfoUtil;
import org.apache.jackrabbit.oak.remote.common.RevisionableUtils;
import org.apache.jackrabbit.oak.remote.proto.ChangeEventProtos;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CreateCheckpointRequest;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos.Commit;
import org.apache.jackrabbit.oak.remote.proto.LeaseProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.segment.spi.rev.RevRepository;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeObserver;
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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.remote.common.RevisionableUtils.getRevision;

public class RemoteNodeStore implements NodeStore, Closeable, Observable {

    private static final Logger log = LoggerFactory.getLogger(RemoteNodeStore.class);

    private final RemoteNodeStoreClient client;

    private final BlobStore blobStore;

    private final CompositeObserver compositeObserver;

    private final StreamObserver observerStreamEvent;

    private final RevRepository nodeStore;

    private final String privateDirName;

    private LeaseProtos.LeaseInfo leaseInfo;

    private volatile LeaseProtos.ClusterView lastClusterView;

    private ScheduledExecutorService leaseRenewProcess = Executors.newScheduledThreadPool(1);

    public static class Builder {

        private RemoteNodeStoreClient client;

        private BlobStore blobStore;

        private RevRepository nodeStore;

        private String privateDirName;

        public Builder setClient(RemoteNodeStoreClient client) {
            this.client = client;
            return this;
        }

        public Builder setBlobStore(BlobStore blobStore) {
            this.blobStore = blobStore;
            return this;
        }

        public Builder setPrivateDirName(String privateDirName) {
            this.privateDirName = privateDirName;
            return this;
        }

        public Builder setNodeStore(RevRepository nodeStore) {
            this.nodeStore = nodeStore;
            return this;
        }

        public RemoteNodeStore build() throws Exception {
            return new RemoteNodeStore(this);
        }
    }

    private RemoteNodeStore(Builder builder) throws Exception {
        this.client = builder.client;
        this.blobStore = builder.blobStore;
        this.nodeStore = builder.nodeStore;
        this.privateDirName = builder.privateDirName;
        this.compositeObserver = new CompositeObserver();

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
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Closeable addObserver(Observer observer) {
        compositeObserver.addObserver(observer);
        return () -> compositeObserver.removeObserver(observer);
    }

    @Override
    @NotNull
    public NodeState getRoot() {
        NodeStateId id = client.getNodeStoreService().getRoot(Empty.getDefaultInstance());
        return createNodeState(id);
    }

    @Override
    public synchronized @NotNull NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook, @NotNull CommitInfo info) throws CommitFailedException {
        assertRootBuilder(builder);
        NodeState head = builder.getNodeState();
        NodeState base = builder.getBaseState();

        CommitFailedException ex = null;
        for (int i = 0; i < 10; i++) {
            try {
                if (i > 0) {
                    Thread.sleep(500);
                }
            } catch (InterruptedException e1) {
                log.error("Interrupted", e1);
            }

            NodeState rootState = getRoot();

            if (!RevisionableUtils.fastEquals(rootState, builder.getBaseState())) {
                reset(builder, rootState);
                head.compareAgainstBaseState(base, new ConflictAnnotatingRebaseDiff(builder));
            }

            NodeState baseNodeState = builder.getBaseState();
            NodeState headNodeState;
            try {
                headNodeState = commitHook.processCommit(baseNodeState, builder.getNodeState(), info);
            } catch (CommitFailedException e) {
                log.warn("Hooks failed, attempt {}/5", i+1);
                log.info("diff: {}", JsopDiff.diffToJsop(baseNodeState, builder.getNodeState()));
                ex = e;
                continue;
            }

            try {
                nodeStore.flushData();
            } catch (IOException e) {
                log.error("Can't flush", e);
                continue;
            }

            NodeStateId id = client.getNodeStoreService().merge(createCommitObject(info, baseNodeState, headNodeState));

            if (Strings.isNullOrEmpty(id.getRevision())) {
                log.warn("Rebased to an outdated root state, attempt {}/5", i+1);
                ex = new CommitFailedException(CommitFailedException.MERGE, 1, "Can't merge, revision on remote has been updated");
                continue;
            }

            NodeState mergedRoot = createNodeState(id);
            reset(builder, mergedRoot);
            return mergedRoot;
        }
        throw ex;
    }

    @Override
    public @NotNull NodeState rebase(@NotNull NodeBuilder builder) {
        NodeState head = builder.getNodeState();
        NodeState base = builder.getBaseState();
        NodeState newBase = getRoot();
        if (!RevisionableUtils.fastEquals(base, newBase)) {
            reset(builder, newBase);
            head.compareAgainstBaseState(base, new ConflictAnnotatingRebaseDiff(builder));
            head = builder.getNodeState();
        }
        return head;
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        assertRootBuilder(builder);
        NodeState root = getRoot();
        reset(builder, root);
        return root;
    }

    private void reset(@NotNull NodeBuilder builder, NodeState newBase) {
        if (!(builder instanceof MemoryNodeBuilder)) {
            throw new IllegalArgumentException("Invalid node builder: " + builder.getClass());
        }
        ((MemoryNodeBuilder) builder).reset(newBase);
    }

    private void assertRootBuilder(NodeBuilder builder) {
        if (!(builder instanceof MemoryNodeBuilder)) {
            throw new IllegalArgumentException("Invalid node builder: " + builder.getClass());
        }
        MemoryNodeBuilder nodeBuilder = (MemoryNodeBuilder) builder;
        if (!nodeBuilder.isRoot()) {
            throw new IllegalArgumentException("Not a root builder: " + builder.getClass());
        }
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

    private NodeState createNodeState(NodeStateId id) {
        String revision = id.getRevision();
        return nodeStore.getNodeStateByRevision(revision);
    }

    @NotNull
    private CommitProtos.Commit createCommitObject(@NotNull CommitInfo info, NodeState baseNodeState, NodeState headNodeState) {
        Commit.Builder commitBuilder = Commit.newBuilder();
        commitBuilder.setCommitInfo(CommitInfoUtil.serialize(info));
        commitBuilder.getBaseNodeStateBuilder().setRevision(getRevision(baseNodeState));
        commitBuilder.getHeadNodeStateBuilder().setRevision(getRevision(headNodeState));
        commitBuilder.setSegmentStoreDir(privateDirName);
        return commitBuilder.build();
    }

    public LeaseProtos.ClusterView getLastClusterView() {
        return lastClusterView;
    }
}
