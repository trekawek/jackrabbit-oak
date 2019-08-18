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
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.remote.common.PropertyDeserializer;
import org.apache.jackrabbit.oak.remote.common.PropertySerializer;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderChangeProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderChangeProtos.NodeBuilderChange;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderChangeProtos.NodeBuilderChangeList;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.MoveOp;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderId;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderPath;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderValue;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.RevisionableNodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.jackrabbit.oak.remote.server.RevisionableNodeUtils.getNodeStateId;

public class NodeBuilderService extends NodeBuilderServiceGrpc.NodeBuilderServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(NodeBuilderService.class);

    private final RevisionableNodeStore nodeStore;

    private final NodeBuilderRepository nodeBuilderRepository;

    private final PropertyDeserializer deserializer;

    public NodeBuilderService(Function<String, Blob> blobProvider, RevisionableNodeStore nodeStore, NodeBuilderRepository nodeBuilderRepository) {
        this.nodeStore = nodeStore;
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
                    .setHashCode(nodeBuilder.hashCode())
                    .setExists(nodeBuilder.exists())
                    .addAllChildName(nodeBuilder.getChildNodeNames())
                    .addAllProperty(Iterables.transform(nodeBuilder.getProperties(), PropertySerializer::toProtoProperty));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            log.error("Can't read node value", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void createNodeState(NodeBuilderPath request, StreamObserver<NodeStateId> responseObserver) {
        NodeBuilder nodeBuilder;
        try {
            nodeBuilder = nodeBuilderRepository.getBuilder(request);
        } catch (RemoteNodeStoreException e) {
            log.error("Can't create node state", e);
            responseObserver.onError(e);
            return;
        }

        NodeState nodeState = nodeBuilder.getNodeState();
        responseObserver.onNext(getNodeStateId(nodeState));
        responseObserver.onCompleted();
    }

    @Override
    public void createBaseNodeState(NodeBuilderPath request, StreamObserver<NodeStateId> responseObserver) {
        NodeBuilder nodeBuilder;
        try {
            nodeBuilder = nodeBuilderRepository.getBuilder(request);
        } catch (RemoteNodeStoreException e) {
            log.error("Can't create base node state", e);
            responseObserver.onError(e);
            return;
        }

        NodeState nodeState = nodeBuilder.getBaseState();
        responseObserver.onNext(getNodeStateId(nodeState));
        responseObserver.onCompleted();
    }

    @Override
    public void move(MoveOp request, StreamObserver<NodeBuilderPath> responseObserver) {
        try {
            NodeBuilder src = nodeBuilderRepository.getBuilder(request.getNodeBuilderPath());
            NodeBuilder newParent = nodeBuilderRepository.getBuilder(request.getNewParent());
            String newName = request.getNewName();
            if (src.moveTo(newParent, newName)) {
                responseObserver.onNext(NodeBuilderPath.newBuilder()
                        .setNodeBuilderId(request.getNewParent().getNodeBuilderId())
                        .setPath(PathUtils.concat(request.getNewParent().getPath(), newName))
                        .build());
            } else {
                responseObserver.onNext(NodeBuilderPath.getDefaultInstance());
            }
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            log.error("Can't move node", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void release(NodeBuilderId request, StreamObserver<Empty> responseObserver) {
        nodeBuilderRepository.release(request.getValue());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    public void apply(NodeBuilderChangeList request, StreamObserver<Empty> responseObserver) {
        Map<NodeBuilderPath, NodeBuilder> builderCache = new HashMap<>();
        try {
            for (NodeBuilderChange c : request.getChangeList()) {
                applyChange(builderCache, c);
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (RemoteNodeStoreException e) {
            log.error("Can't apply changes", e);
            responseObserver.onError(e);
        }
    }

    private void applyChange(Map<NodeBuilderPath, NodeBuilder> builderCache, NodeBuilderChange change) throws RemoteNodeStoreException {
        NodeBuilderPath path = change.getNodeBuilderPath();
        if (!builderCache.containsKey(path)) {
            builderCache.put(path, nodeBuilderRepository.getBuilder(path));
        }
        NodeBuilder nodeBuilder = builderCache.get(path);
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
                NodeBuilderChangeProtos.SetChildNode setChildNodeRequest = change.getSetChildNode();
                if (setChildNodeRequest.hasNodeStateId()) {
                    NodeState nodeState = nodeStore.getNodeByRevision(setChildNodeRequest.getNodeStateId().getRevision());
                    nodeBuilder.setChildNode(setChildNodeRequest.getChildName(), nodeState);
                } else {
                    nodeBuilder.setChildNode(setChildNodeRequest.getChildName());
                }
                break;

            case SETPROPERTY:
                nodeBuilder.setProperty(deserializer.toOakProperty(change.getSetProperty().getProperty()));
                break;
        }
    }
}
