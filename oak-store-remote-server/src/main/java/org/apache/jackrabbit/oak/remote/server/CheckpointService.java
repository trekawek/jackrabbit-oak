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
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CheckpointId;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CheckpointInfo;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CheckpointList;
import org.apache.jackrabbit.oak.remote.proto.CheckpointProtos.CreateCheckpointRequest;
import org.apache.jackrabbit.oak.remote.proto.CheckpointServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.RevisionableNodeStore;

import java.util.Map;

import static org.apache.jackrabbit.oak.remote.server.RevisionableNodeUtils.getNodeStateId;

public class CheckpointService extends CheckpointServiceGrpc.CheckpointServiceImplBase {

    private final RevisionableNodeStore nodeStore;

    public CheckpointService(RevisionableNodeStore nodeStore) {
        this.nodeStore = nodeStore;
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
            responseObserver.onNext(getNodeStateId(nodeState));
            responseObserver.onCompleted();
        }
    }

    public void releaseCheckpoint(CheckpointId request, StreamObserver<BoolValue> responseObserver) {
        boolean result = nodeStore.release(request.getId());
        responseObserver.onNext(BoolValue.newBuilder().setValue(result).build());
        responseObserver.onCompleted();
    }
}
