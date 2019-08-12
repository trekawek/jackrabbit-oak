package org.apache.jackrabbit.oak.remote.server;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.remote.common.PropertyDeserializer;
import org.apache.jackrabbit.oak.remote.common.PropertySerializer;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderChanges;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderId;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderPath;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.NodeDiffProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos.NodeValue;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class NodeBuilderService extends NodeBuilderServiceGrpc.NodeBuilderServiceImplBase {

    private final NodeStateRepository nodeStateRepository;

    private final NodeBuilderRepository nodeBuilderRepository;

    private final NodeStore nodeStore;

    public NodeBuilderService(NodeStore nodeStore, NodeStateRepository nodeStateRepository, NodeBuilderRepository nodeBuilderRepository) {
        this.nodeStore = nodeStore;
        this.nodeStateRepository = nodeStateRepository;
        this.nodeBuilderRepository = nodeBuilderRepository;
    }

    @Override
    public void getNodeValue(NodeBuilderPath request, StreamObserver<NodeValue> responseObserver) {
        NodeBuilder nodeBuilder;
        try {
            nodeBuilder = nodeBuilderRepository.getBuilder(request);
            NodeValue.Builder builder = NodeValue.newBuilder();
            builder.addAllChildName(nodeBuilder.getChildNodeNames());
            for (PropertyState propertyState : nodeBuilder.getProperties()) {
                builder.addProperty(PropertySerializer.toProtoProperty(propertyState));
            }
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
                nodeBuilder.getChildNode(change.getRemoveNode().getChildName()).remove();
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
                nodeBuilder.setProperty(PropertyDeserializer.toOakProperty(nodeStore, change.getSetProperty().getProperty()));
                break;
        }
    }
}
