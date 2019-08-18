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
import org.apache.jackrabbit.oak.remote.proto.ChangeEventProtos.ChangeEvent;
import org.apache.jackrabbit.oak.remote.proto.CommitProtos.Commit;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderId;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeStoreServiceGrpc;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class NodeStoreService extends NodeStoreServiceGrpc.NodeStoreServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(NodeStoreService.class);

    private final NodeStateRepository nodeStateRepository;

    private final NodeBuilderRepository nodeBuilderRepository;

    private final NodeStore nodeStore;

    public NodeStoreService(NodeStore nodeStore, NodeStateRepository nodeStateRepository, NodeBuilderRepository nodeBuilderRepository) {
        this.nodeStore = nodeStore;
        this.nodeStateRepository = nodeStateRepository;
        this.nodeBuilderRepository = nodeBuilderRepository;
    }

    @Override
    public void getRoot(Empty request, StreamObserver<NodeStateId> responseObserver) {
        NodeState nodeState = nodeStore.getRoot();
        NodeStateId nodeStateId = NodeStateId.newBuilder()
                .setValue(nodeStateRepository.addNewNodeState(nodeState))
                .build();
        responseObserver.onNext(nodeStateId);
        responseObserver.onCompleted();
    }

    @Override
    public void merge(Commit request, StreamObserver<NodeStateId> responseObserver) {
        try {
            NodeBuilder builder = nodeBuilderRepository.getBuilder(request.getNodeBuilderId());
            CommitInfo commitInfo = CommitInfoUtil.deserialize(request.getCommitInfo());
            NodeState nodeState = nodeStore.merge(builder, EmptyHook.INSTANCE, commitInfo);
            NodeStateId nodeStateId = NodeStateId.newBuilder()
                    .setValue(nodeStateRepository.addNewNodeState(nodeState))
                    .build();
            responseObserver.onNext(nodeStateId);
            responseObserver.onCompleted();
        } catch (CommitFailedException | RemoteNodeStoreException e) {
            log.error("Can't merge", e);
            responseObserver.onError(e);
        }
    }

    public void rebase(NodeBuilderId request, StreamObserver<NodeStateId> responseObserver) {
        try {
            NodeBuilder builder = nodeBuilderRepository.getBuilder(request);
            NodeState nodeState = nodeStore.rebase(builder);
            NodeStateId nodeStateId = NodeStateId.newBuilder()
                    .setValue(nodeStateRepository.addNewNodeState(nodeState))
                    .build();
            responseObserver.onNext(nodeStateId);
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            log.error("Can't rebase", e);
            responseObserver.onError(e);
        }
    }

    public void reset(NodeBuilderId request, StreamObserver<NodeStateId> responseObserver) {
        try {
            NodeBuilder builder = nodeBuilderRepository.getBuilder(request);
            NodeState nodeState = nodeStore.reset(builder);
            NodeStateId nodeStateId = NodeStateId.newBuilder()
                    .setValue(nodeStateRepository.addNewNodeState(nodeState))
                    .build();
            responseObserver.onNext(nodeStateId);
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            log.error("Can't reset", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public StreamObserver<Empty> observe(StreamObserver<ChangeEvent> responseObserver) {
        Closeable closeable;
        if (nodeStore instanceof Observable) {
            closeable = ((Observable) nodeStore).addObserver((root, info) -> {
                ChangeEvent.Builder builder = ChangeEvent.newBuilder();
                builder.getNodeStateIdBuilder().setValue(nodeStateRepository.addNewNodeState(root));
                builder.setCommitInfo(CommitInfoUtil.serialize(info));
                responseObserver.onNext(builder.build());
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
                    closeable.close();
                } catch (IOException e) {
                    log.error("Can't close observer", e);
                }
            }
        };
    }
}
