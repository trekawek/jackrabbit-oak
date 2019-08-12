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

import com.google.common.collect.Iterables;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.remote.common.PropertyDeserializer;
import org.apache.jackrabbit.oak.remote.common.PropertySerializer;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderId;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderPath;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderValue;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.NodeDiffProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeDiffProtos.NodeBuilderChanges;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.function.Function;

public class NodeBuilderService extends NodeBuilderServiceGrpc.NodeBuilderServiceImplBase {

    private final NodeStateRepository nodeStateRepository;

    private final NodeBuilderRepository nodeBuilderRepository;

    private final PropertyDeserializer deserializer;

    public NodeBuilderService(Function<String, Blob> blobProvider, NodeStateRepository nodeStateRepository, NodeBuilderRepository nodeBuilderRepository) {
        this.nodeStateRepository = nodeStateRepository;
        this.nodeBuilderRepository = nodeBuilderRepository;
        this.deserializer = new PropertyDeserializer(blobProvider);
    }

    @Override
    public void getNodeValue(NodeBuilderPath request, StreamObserver<NodeBuilderValue> responseObserver) {
        NodeBuilder nodeBuilder;
        try {
            nodeBuilder = nodeBuilderRepository.getBuilder(request);

            NodeBuilderValue.Builder builder = NodeBuilderValue.newBuilder();
            builder
                    .setIsNew(nodeBuilder.isNew())
                    .setIsModified(nodeBuilder.isModified())
                    .setIsReplaced(nodeBuilder.isReplaced())
                    .getNodeValueBuilder()
                    .setExists(nodeBuilder.exists())
                    .addAllChildName(nodeBuilder.getChildNodeNames())
                    .addAllProperty(Iterables.transform(nodeBuilder.getProperties(), propertyState -> {
                        try {
                            return PropertySerializer.toProtoProperty(propertyState);
                        } catch (RemoteNodeStoreException e) {
                            throw new IllegalArgumentException(e);
                        }
                    }));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            responseObserver.onError(e);
            return;
        }
    }

    @Override
    public void createNodeState(NodeBuilderPath request, StreamObserver<NodeStateId> responseObserver) {
        NodeBuilder nodeBuilder;
        try {
            nodeBuilder = nodeBuilderRepository.getBuilder(request);
        } catch (RemoteNodeStoreException e) {
            responseObserver.onError(e);
            return;
        }

        NodeState nodeState = nodeBuilder.getNodeState();
        long nodeStateId = nodeStateRepository.addNewNodeState(nodeState);
        responseObserver.onNext(NodeStateId.newBuilder().setValue(nodeStateId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createBaseNodeState(NodeBuilderPath request, StreamObserver<NodeStateId> responseObserver) {
        NodeBuilder nodeBuilder;
        try {
            nodeBuilder = nodeBuilderRepository.getBuilder(request);
        } catch (RemoteNodeStoreException e) {
            responseObserver.onError(e);
            return;
        }

        NodeState nodeState = nodeBuilder.getBaseState();
        long nodeStateId = nodeStateRepository.addNewNodeState(nodeState);
        responseObserver.onNext(NodeStateId.newBuilder().setValue(nodeStateId).build());
        responseObserver.onCompleted();
    }


    @Override
    public void apply(NodeBuilderChanges request, StreamObserver<Empty> responseObserver) {
        NodeBuilder root;
        try {
            root = nodeBuilderRepository.getBuilder(request.getNodeBuilderId());
            for (NodeDiffProtos.NodeDiff change : request.getChangeList()) {
                applyChange(root, change);
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void release(NodeBuilderId request, StreamObserver<Empty> responseObserver) {
        nodeBuilderRepository.release(request.getValue());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    private void applyChange(NodeBuilder root, NodeDiffProtos.NodeDiff change) throws RemoteNodeStoreException {
        NodeBuilder nodeBuilder = nodeBuilderRepository.getNodeBuilder(root, change.getNodePath());
        switch (change.getChangeCase()) {
            case ADDNODE:
                nodeBuilder.child(change.getAddNode().getChildName());
                break;

            case REMOVENODE:
                nodeBuilder.remove();
                break;

            case REMOVEPROPERTY:
                nodeBuilder.removeProperty(change.getRemoveProperty().getName());
                break;

            case SETCHILDNODE:
                NodeDiffProtos.SetChildNode setChildNodeRequest = change.getSetChildNode();
                NodeState nodeState = nodeStateRepository.getNodeState(setChildNodeRequest.getNodeStatePath());
                nodeBuilder.setChildNode(setChildNodeRequest.getChildName(), nodeState);
                break;

            case SETPROPERTY:
                nodeBuilder.setProperty(deserializer.toOakProperty(change.getSetProperty().getProperty()));
                break;
        }
    }
}
