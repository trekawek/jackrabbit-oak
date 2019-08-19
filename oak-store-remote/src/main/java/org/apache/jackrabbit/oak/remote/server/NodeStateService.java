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
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.remote.common.PropertySerializer;
import org.apache.jackrabbit.oak.remote.proto.NodeStateDiffProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.CompareNodeStateOp;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStatePathPair;
import org.apache.jackrabbit.oak.remote.proto.NodeStateServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos.NodeValue;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.RevisionableNodeStore;

import static org.apache.jackrabbit.oak.remote.common.PropertySerializer.toProtoProperty;
import static org.apache.jackrabbit.oak.remote.server.RevisionableNodeUtils.getNodeStateId;

public class NodeStateService extends NodeStateServiceGrpc.NodeStateServiceImplBase {

    private final RevisionableNodeStore nodeStore;

    public NodeStateService(RevisionableNodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    @Override
    public void getNodeValue(NodeStateId request, StreamObserver<NodeValue> responseObserver) {
        NodeState nodeState = nodeStore.getNodeByRevision(request.getRevision());
        NodeValue.Builder builder = NodeValue.newBuilder();
        builder.setExists(nodeState.exists());
        builder.setHashCode(nodeState.hashCode());
        for (ChildNodeEntry e : nodeState.getChildNodeEntries()) {
            builder.addChildBuilder()
                    .setName(e.getName())
                    .setNodeStateId(getNodeStateId(e.getNodeState()));
        }
        for (PropertyState p : nodeState.getProperties()) {
            builder.addProperty(toProtoProperty(p));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void equals(NodeStatePathPair request, StreamObserver<BoolValue> responseObserver) {
        NodeState nodeState1 = nodeStore.getNodeByRevision(request.getNodeState1().getRevision());
        NodeState nodeState2 = nodeStore.getNodeByRevision(request.getNodeState2().getRevision());
        responseObserver.onNext(BoolValue.newBuilder().setValue(nodeState1.equals(nodeState2)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void compare(CompareNodeStateOp request, StreamObserver<NodeStateDiffProtos.NodeStateDiff> responseObserver) {
        NodeState baseNodeState = nodeStore.getNodeByRevision(request.getBaseNodeState().getRevision());
        NodeState nodeState = nodeStore.getNodeByRevision(request.getNodeState().getRevision());
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
                        .setAfter(getNodeStateId(after));
                return true;
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                NodeStateDiffProtos.NodeChanged.Builder builder = diffBuilder.addEventsBuilder()
                        .getNodeChangedBuilder()
                        .setName(name)
                        .setBefore(getNodeStateId(before))
                        .setAfter(getNodeStateId(after));
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                diffBuilder.addEventsBuilder()
                        .getNodeDeletedBuilder()
                        .setName(name)
                        .setBefore(getNodeStateId(before));
                return true;
            }
        });
        responseObserver.onNext(diffBuilder.build());
        responseObserver.onCompleted();
    }
}
