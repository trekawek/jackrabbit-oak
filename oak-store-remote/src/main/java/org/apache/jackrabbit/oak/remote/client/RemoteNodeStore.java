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

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.remote.common.CommitInfoUtil;
import org.apache.jackrabbit.oak.remote.proto.ChangeEventProtos;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CreateCheckpointRequest;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos.Commit;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeObserver;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RemoteNodeStore implements NodeStore, Closeable, Observable {

    private final RemoteNodeStoreClient client;

    private final BlobStore blobStore;

    private final RemoteNodeStoreContext context;

    private final ScheduledExecutorService gcExecutor;

    private final CompositeObserver compositeObserver;

    private final StreamObserver observerStreamEvent;

    public RemoteNodeStore(RemoteNodeStoreClient client, BlobStore blobStore) {
        this.client = client;
        this.blobStore = blobStore;
        this.context = new RemoteNodeStoreContext(client, blobStore);
        this.gcExecutor = Executors.newSingleThreadScheduledExecutor();
        this.gcExecutor.scheduleAtFixedRate(context::collectOrphanedReferences, 1, 1, TimeUnit.MINUTES);
        this.compositeObserver = new CompositeObserver();
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

    public void close() {
        this.observerStreamEvent.onCompleted();
        this.gcExecutor.shutdown();
    }

    @Override
    public Closeable addObserver(Observer observer) {
        compositeObserver.addObserver(observer);
        return () -> compositeObserver.removeObserver(observer);
    }

    @Override
    public @NotNull NodeState getRoot() {
        NodeStateId id = client.getNodeStoreService().getRoot(Empty.getDefaultInstance());
        return createNodeState(id);
    }

    @Override
    public @NotNull NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook, @NotNull CommitInfo info) throws CommitFailedException {
        if (!(builder instanceof RemoteNodeBuilder)) {
            throw new IllegalArgumentException("Invalid node builder: " + builder);
        }
        RemoteNodeBuilder nodeBuilder = (RemoteNodeBuilder) builder;
        nodeBuilder.flush();
        Commit.Builder commitBuilder = createCommitObject(info, nodeBuilder);
        NodeStateId id = client.getNodeStoreService().merge(commitBuilder.build());
        return createNodeState(id);
    }

    @Override
    public @NotNull NodeState rebase(@NotNull NodeBuilder builder) {
        if (!(builder instanceof RemoteNodeBuilder)) {
            throw new IllegalArgumentException("Invalid node builder: " + builder);
        }
        RemoteNodeBuilder nodeBuilder = (RemoteNodeBuilder) builder;
        NodeStateId id = client.getNodeStoreService().rebase(nodeBuilder.getNodeBuilderId());
        return createNodeState(id);
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        if (!(builder instanceof RemoteNodeBuilder)) {
            throw new IllegalArgumentException("Invalid node builder: " + builder);
        }
        RemoteNodeBuilder nodeBuilder = (RemoteNodeBuilder) builder;
        NodeStateId id = client.getNodeStoreService().reset(nodeBuilder.getNodeBuilderId());
        return createNodeState(id);
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
        context.addNodeStateId(id);
        return new RemoteNodeState(context, id);
    }

    @NotNull
    private CommitProtos.Commit.Builder createCommitObject(@NotNull CommitInfo info, RemoteNodeBuilder nodeBuilder) {
        Commit.Builder commitBuilder = Commit.newBuilder();
        commitBuilder.setNodeBuilderId(nodeBuilder.getNodeBuilderId());
        commitBuilder.setCommitInfo(CommitInfoUtil.serialize(info));
        return commitBuilder;
    }
}
