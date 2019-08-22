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

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.remote.common.CommitInfoUtil;
import org.apache.jackrabbit.oak.remote.common.PropertyDeserializer;
import org.apache.jackrabbit.oak.remote.proto.ChangeEventProtos.ChangeEvent;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos.Commit;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos.CommitEvent;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeStoreServiceGrpc;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.jackrabbit.oak.remote.server.RevisionableNodeUtils.getNodeStateId;
import static org.apache.jackrabbit.oak.remote.server.RevisionableNodeUtils.getRevision;

public class NodeStoreService extends NodeStoreServiceGrpc.NodeStoreServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(NodeStoreService.class);

    private final SegmentNodeStore nodeStore;

    private final FileStore fileStore;

    private final PropertyDeserializer deserializer;

    public NodeStoreService(SegmentNodeStore nodeStore, FileStore fileStore, BlobStore blobStore) {
        this.nodeStore = nodeStore;
        this.fileStore = fileStore;
        this.deserializer = new PropertyDeserializer(blobId -> new BlobStoreBlob(blobStore, blobId));
    }

    @Override
    public void getRoot(Empty request, StreamObserver<NodeStateId> responseObserver) {
        responseObserver.onNext(getNodeStateId(nodeStore.getRoot()));
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<CommitEvent> merge(StreamObserver<NodeStateId> responseObserver) {
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();
        String currentRootRevision = getRevision(root);

        return new StreamObserver<CommitEvent>() {

            private Commit commit;

            @Override
            public void onNext(CommitEvent commitEvent) {
                switch (commitEvent.getEventCase()) {
                    case COMMIT:
                        commit = commitEvent.getCommit();
                        break;

                    case CHANGE:
                        applyChange(builder, commitEvent.getChange());
                        break;
                }
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
                synchronized (NodeStoreService.this) {
                    if (!currentRootRevision.equals(commit.getRootId().getRevision())) {
                        responseObserver.onNext(NodeStateId.getDefaultInstance());
                        responseObserver.onCompleted();
                        return;
                    }

                    try {
                        NodeState newRoot = nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfoUtil.deserialize(commit.getCommitInfo()));
                        responseObserver.onNext(getNodeStateId(newRoot));
                        responseObserver.onCompleted();
                    } catch (CommitFailedException e) {
                        log.error("Can't commit", e);
                        responseObserver.onError(e);
                    }
                }
            }
        };
    }

    private void applyChange(NodeBuilder root, CommitProtos.NodeBuilderChange change) {
        NodeBuilder nodeBuilder = getNodeBuilder(root, change.getNodeBuilderPath());
        switch (change.getChangeCase()) {
            case ADDNODE:
                nodeBuilder.child(change.getAddNode().getChildName());
                break;

            case REMOVENODE:
                nodeBuilder.getChildNode(change.getRemoveNode().getChildName()).remove();
                break;

            case REMOVEPROPERTY:
                nodeBuilder.removeProperty(change.getRemoveProperty().getName());
                break;

            case SETPROPERTY:
                nodeBuilder.setProperty(deserializer.toOakProperty(change.getSetProperty().getProperty()));
                break;
        }
    }

    private NodeBuilder getNodeBuilder(NodeBuilder root, String nodeBuilderPath) {
        NodeBuilder builder = root;
        for (String el : PathUtils.elements(nodeBuilderPath)) {
            builder = builder.getChildNode(el);
        }
        return builder;
    }

    @Override
    public StreamObserver<Empty> observe(StreamObserver<ChangeEvent> responseObserver) {
        Closeable closeable;
        AtomicBoolean enabled = new AtomicBoolean(true);
        if (nodeStore instanceof Observable) {
            closeable = ((Observable) nodeStore).addObserver((root, info) -> {
                if (!enabled.get()) {
                    return;
                }
                ChangeEvent.Builder builder = ChangeEvent.newBuilder();
                builder.setNodeStateId(getNodeStateId(root));
                builder.setCommitInfo(CommitInfoUtil.serialize(info));
                try {
                    responseObserver.onNext(builder.build());
                } catch (Exception e) {
                    log.error("Can't send state", e);
                    enabled.set(false);
                }
            });
        } else {
            closeable = ()->{};
        }
        return new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty empty) {
            }
            @Override
            public void onError(Throwable throwable) {
            }
            @Override
            public void onCompleted() {
                try {
                    enabled.set(false);
                    closeable.close();
                } catch (IOException e) {
                    log.error("Can't close observer", e);
                }
            }
        };
    }
}
