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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderId;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStatePath;
import org.apache.jackrabbit.oak.remote.proto.NodeStateServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos.NodeValue;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.remote.common.PropertySerializer.toProtoProperty;

public class NodeStateService extends NodeStateServiceGrpc.NodeStateServiceImplBase {

    private final NodeStateRepository nodeStateRepository;

    private final NodeBuilderRepository nodeBuilderRepository;

    public NodeStateService(NodeStateRepository nodeStateRepository, NodeBuilderRepository nodeBuilderRepository) {
        this.nodeStateRepository = nodeStateRepository;
        this.nodeBuilderRepository = nodeBuilderRepository;
    }

    @Override
    public void getNodeValue(NodeStatePath request, StreamObserver<NodeValue> responseObserver) {
        NodeState nodeState;
        try {
            nodeState = nodeStateRepository.getNodeState(request);
            NodeValue.Builder builder = NodeValue.newBuilder();
            builder.addAllChildName(nodeState.getChildNodeNames());
            for (PropertyState p : nodeState.getProperties()) {
                builder.addProperty(toProtoProperty(p));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            responseObserver.onError(e);
            return;
        }
    }

    @Override
    public void createNodeBuilder(NodeStatePath request, StreamObserver<NodeBuilderId> responseObserver) {
        NodeState nodeState;
        try {
            nodeState = nodeStateRepository.getNodeState(request);
        } catch (RemoteNodeStoreException e) {
            responseObserver.onError(e);
            return;
        }
        long nodeBuilderId = nodeBuilderRepository.addNewNodeState(nodeState.builder());
        responseObserver.onNext(NodeBuilderId.newBuilder().setValue(nodeBuilderId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void release(NodeStateId request, StreamObserver<Empty> responseObserver) {
        nodeStateRepository.release(request.getValue());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
