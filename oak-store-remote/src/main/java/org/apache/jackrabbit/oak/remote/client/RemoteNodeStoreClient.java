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
package org.apache.jackrabbit.oak.remote.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.jackrabbit.oak.remote.proto.CheckpointServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.CheckpointServiceGrpc.CheckpointServiceBlockingStub;
import org.apache.jackrabbit.oak.remote.proto.LeaseServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.LeaseServiceGrpc.LeaseServiceBlockingStub;
import org.apache.jackrabbit.oak.remote.proto.NodeStateServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.NodeStateServiceGrpc.NodeStateServiceBlockingStub;
import org.apache.jackrabbit.oak.remote.proto.NodeStoreServiceGrpc;
import org.apache.jackrabbit.oak.remote.proto.NodeStoreServiceGrpc.NodeStoreServiceBlockingStub;
import org.apache.jackrabbit.oak.remote.server.LeaseService;

import java.util.concurrent.TimeUnit;

public class RemoteNodeStoreClient {

    private final ManagedChannel channel;

    private final CheckpointServiceBlockingStub checkpointService;

    private final NodeStateServiceBlockingStub nodeStateService;

    private final NodeStoreServiceBlockingStub nodeStoreService;

    private final NodeStoreServiceGrpc.NodeStoreServiceStub nodeStoreAsyncService;

    private final LeaseServiceBlockingStub leaseService;

    public RemoteNodeStoreClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public RemoteNodeStoreClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        checkpointService = CheckpointServiceGrpc.newBlockingStub(channel);
        nodeStateService = NodeStateServiceGrpc.newBlockingStub(channel);
        nodeStoreService = NodeStoreServiceGrpc.newBlockingStub(channel);
        nodeStoreAsyncService = NodeStoreServiceGrpc.newStub(channel);
        leaseService = LeaseServiceGrpc.newBlockingStub(channel);
    }

    public CheckpointServiceBlockingStub getCheckpointService() {
        return checkpointService;
    }

    public NodeStateServiceBlockingStub getNodeStateService() {
        return nodeStateService;
    }

    public NodeStoreServiceBlockingStub getNodeStoreService() {
        return nodeStoreService;
    }

    public NodeStoreServiceGrpc.NodeStoreServiceStub getNodeStoreAsyncService() {
        return nodeStoreAsyncService;
    }

    public LeaseServiceBlockingStub getLeaseService() {
        return leaseService;
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

}
