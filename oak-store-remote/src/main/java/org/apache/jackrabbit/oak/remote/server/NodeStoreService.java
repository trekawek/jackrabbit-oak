package org.apache.jackrabbit.oak.remote.server;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderId;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeStoreServiceGrpc;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

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
    public void merge(NodeBuilderId request, StreamObserver<NodeStateId> responseObserver) {
        NodeBuilder builder = nodeBuilderRepository.get(request.getValue());
        if (builder == null) {
            responseObserver.onError(new RemoteNodeStoreException("Invalid node builder id: " + request.getValue()));
            return;
        }
        try {
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        } catch (CommitFailedException e) {
            responseObserver.onError(e);
        }
    }

}
