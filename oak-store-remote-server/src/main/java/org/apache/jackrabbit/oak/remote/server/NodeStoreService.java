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
import org.apache.jackrabbit.oak.remote.common.CommitInfoUtil;
import org.apache.jackrabbit.oak.remote.common.RevisionableUtils;
import org.apache.jackrabbit.oak.remote.proto.ChangeEventProtos.ChangeEvent;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos.Commit;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeStoreServiceGrpc;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.jackrabbit.oak.remote.common.RevisionableUtils.getNodeStateId;

public class NodeStoreService extends NodeStoreServiceGrpc.NodeStoreServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(NodeStoreService.class);

    private final SegmentNodeStore nodeStore;

    private final FileStore fileStore;

    private final PrivateFileStores privateFileStores;

    public NodeStoreService(SegmentNodeStore nodeStore, FileStore fileStore, PrivateFileStores privateFileStores) {
        this.nodeStore = nodeStore;
        this.fileStore = fileStore;
        this.privateFileStores = privateFileStores;
    }

    @Override
    public void getRoot(Empty request, StreamObserver<NodeStateId> responseObserver) {
        responseObserver.onNext(getNodeStateId(nodeStore.getRoot()));
        responseObserver.onCompleted();
    }

    @Override
    public void merge(Commit commit, StreamObserver<NodeStateId> responseObserver) {
        try {
            NodeState newRoot;
            synchronized (this) {
                NodeState currentRoot = nodeStore.getRoot();

                String currentRootRevision = RevisionableUtils.getRevision(currentRoot);
                if (!currentRootRevision.equals(commit.getBaseNodeState().getRevision())) {
                    responseObserver.onNext(NodeStateId.getDefaultInstance());
                    responseObserver.onCompleted();
                    return;
                }

                NodeBuilder builder = currentRoot.builder();
                NodeState newHead = privateFileStores.getNodeState(commit.getSegmentStoreDir(), commit.getHeadNodeState().getRevision());
                newHead.compareAgainstBaseState(currentRoot, new ApplyDiff(builder));

                newRoot = nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfoUtil.deserialize(commit.getCommitInfo()));
            }
            fileStore.flush();
            responseObserver.onNext(getNodeStateId(newRoot));
            responseObserver.onCompleted();
        } catch (CommitFailedException | IOException e) {
            log.error("Can't commit", e);
            responseObserver.onError(e);
        }
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
