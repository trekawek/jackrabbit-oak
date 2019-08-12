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
import org.apache.jackrabbit.oak.spi.state.NodeStore;

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
