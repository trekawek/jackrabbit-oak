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
import org.apache.jackrabbit.oak.remote.proto.CommitProtos.Commit;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeStoreServiceGrpc;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.HashMap;
import java.util.Map;

public class NodeStoreService extends NodeStoreServiceGrpc.NodeStoreServiceImplBase {

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
            CommitInfo commitInfo = createCommitInfo(request);
            nodeStore.merge(builder, EmptyHook.INSTANCE, commitInfo);
        } catch (CommitFailedException | RemoteNodeStoreException e) {
            responseObserver.onError(e);
        }
    }

    private static CommitInfo createCommitInfo(Commit commit) {
        Map<String, Object> commitInfo = new HashMap<>();
        commitInfo.putAll(commit.getCommitInfoMap());
        return new CommitInfo(commit.getSessionId(), commit.getUserId(), commitInfo, commit.getIsExternal());
    }

}
