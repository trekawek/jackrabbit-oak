package org.apache.jackrabbit.oak.remote.server;

import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CheckpointId;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CheckpointInfo;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CheckpointList;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CreateCheckpointRequest;
import org.apache.jackrabbit.oak.remote.proto.CheckpointServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.Map;

public class CheckpointService extends CheckpointServiceGrpc.CheckpointServiceImplBase {

    private final NodeStateRepository nodeStateRepository;

    private final NodeStore nodeStore;

    public CheckpointService(NodeStore nodeStore, NodeStateRepository nodeStateRepository) {
        this.nodeStore = nodeStore;
        this.nodeStateRepository = nodeStateRepository;
    }

    public void createCheckpoint(CreateCheckpointRequest request, StreamObserver<CheckpointId> responseObserver) {
        String checkpoint;
        if (request.hasInfo()) {
            checkpoint = nodeStore.checkpoint(request.getLifetime(), request.getInfo().getCheckpointInfoMap());
        } else {
            checkpoint = nodeStore.checkpoint(request.getLifetime());
        }
        responseObserver.onNext(CheckpointId.newBuilder().setId(checkpoint).build());
        responseObserver.onCompleted();
    }

    public void getCheckpointInfo(CheckpointId request, StreamObserver<CheckpointInfo> responseObserver) {
        Map<String, String> info = nodeStore.checkpointInfo(request.getId());
        responseObserver.onNext(CheckpointInfo.newBuilder().putAllCheckpointInfo(info).build());
        responseObserver.onCompleted();
    }

    public void getCheckpointList(Empty request, StreamObserver<CheckpointList> responseObserver) {
        CheckpointList.Builder builder = CheckpointList.newBuilder();
        for (String checkpointId : nodeStore.checkpoints()) {
            builder.addCheckpointId(CheckpointId.newBuilder().setId(checkpointId).build());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    public void retrieveCheckpoint(CheckpointId request, StreamObserver<NodeStateId> responseObserver) {
        NodeState nodeState = nodeStore.retrieve(request.getId());
        if (nodeState == null) {
            responseObserver.onCompleted();
        } else {
            long nodeStateId = nodeStateRepository.addNewNodeState(nodeState);
            responseObserver.onNext(NodeStateId.newBuilder().setValue(nodeStateId).build());
            responseObserver.onCompleted();
        }
    }

    public void releaseCheckpoint(CheckpointId request, StreamObserver<BoolValue> responseObserver) {
        boolean result = nodeStore.release(request.getId());
        responseObserver.onNext(BoolValue.newBuilder().setValue(result).build());
        responseObserver.onCompleted();
    }
}
