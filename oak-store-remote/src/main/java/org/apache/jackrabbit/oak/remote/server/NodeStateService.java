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

import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.remote.common.PropertySerializer;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderId;
import org.apache.jackrabbit.oak.remote.proto.NodeStateDiffProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.CompareNodeStateOp;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStatePath;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStatePathPair;
import org.apache.jackrabbit.oak.remote.proto.NodeStateServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos.NodeValue;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.remote.common.PropertySerializer.toProtoProperty;

public class NodeStateService extends NodeStateServiceGrpc.NodeStateServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(NodeStateService.class);

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
            builder.setExists(nodeState.exists());
            builder.setHashCode(nodeState.hashCode());
            builder.addAllChildName(nodeState.getChildNodeNames());
            for (PropertyState p : nodeState.getProperties()) {
                builder.addProperty(toProtoProperty(p));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            log.error("Can't read node", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void createNodeBuilder(NodeStatePath request, StreamObserver<NodeBuilderId> responseObserver) {
        NodeState nodeState;
        try {
            nodeState = nodeStateRepository.getNodeState(request);
        } catch (RemoteNodeStoreException e) {
            log.error("Can't create node builder", e);
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

    @Override
    public void equals(NodeStatePathPair request, StreamObserver<BoolValue> responseObserver) {
        try {
            NodeState nodeState1 = nodeStateRepository.getNodeState(request.getNodeStatePath1());
            NodeState nodeState2 = nodeStateRepository.getNodeState(request.getNodeStatePath2());
            responseObserver.onNext(BoolValue.newBuilder().setValue(nodeState1.equals(nodeState2)).build());
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            log.error("Can't check equality", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void compare(CompareNodeStateOp request, StreamObserver<NodeStateDiffProtos.NodeStateDiff> responseObserver) {
        try {
            NodeState baseNodeState = nodeStateRepository.getNodeState(request.getBaseNodeState());
            NodeState nodeState = nodeStateRepository.getNodeState(request.getNodeState());
            NodeStateDiffProtos.NodeStateDiff.Builder diffBuilder = NodeStateDiffProtos.NodeStateDiff.newBuilder();
            nodeState.compareAgainstBaseState(baseNodeState, new NodeStateDiff() {
                @Override
                public boolean propertyAdded(PropertyState after) {
                    diffBuilder.addEventsBuilder()
                            .getPropertyAddedBuilder()
                            .setAfter(PropertySerializer.toProtoProperty(after));
                    return true;
                }

                @Override
                public boolean propertyChanged(PropertyState before, PropertyState after) {
                    diffBuilder.addEventsBuilder()
                            .getPropertyChangedBuilder()
                            .setBefore(PropertySerializer.toProtoProperty(before))
                            .setAfter(PropertySerializer.toProtoProperty(after));
                    return true;
                }

                @Override
                public boolean propertyDeleted(PropertyState before) {
                    diffBuilder.addEventsBuilder()
                            .getPropertyDeletedBuilder()
                            .setBefore(PropertySerializer.toProtoProperty(before));
                    return true;
                }

                @Override
                public boolean childNodeAdded(String name, NodeState after) {
                    diffBuilder.addEventsBuilder()
                            .getNodeAddedBuilder()
                            .setName(name)
                            .getAfterBuilder().setValue(nodeStateRepository.addNewNodeState(after));
                    return true;
                }

                @Override
                public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                    NodeStateDiffProtos.NodeChanged.Builder builder = diffBuilder.addEventsBuilder()
                            .getNodeChangedBuilder()
                            .setName(name);
                    builder.getBeforeBuilder().setValue(nodeStateRepository.addNewNodeState(before));
                    builder.getAfterBuilder().setValue(nodeStateRepository.addNewNodeState(after));
                    return true;
                }

                @Override
                public boolean childNodeDeleted(String name, NodeState before) {
                    diffBuilder.addEventsBuilder()
                            .getNodeDeletedBuilder()
                            .setName(name)
                            .getBeforeBuilder().setValue(nodeStateRepository.addNewNodeState(before));
                    return true;
                }
            });
            responseObserver.onNext(diffBuilder.build());
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            log.error("Can't compare nodes", e);
            responseObserver.onError(e);
        }

    }
}
