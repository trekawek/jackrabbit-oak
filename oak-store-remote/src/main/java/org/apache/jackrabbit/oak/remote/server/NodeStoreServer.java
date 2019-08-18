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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NodeStoreServer {

    private static final Logger log = LoggerFactory.getLogger(NodeStoreServer.class);

    private final NodeBuilderRepository nodeBuilderRepository;

    private final NodeStateRepository nodeStateRepository;

    private final NodeStore nodeStore;

    private final Server server;

    public NodeStoreServer(int port, NodeStore nodeStore) {
        this(ServerBuilder.forPort(port), nodeStore);
    }

    public NodeStoreServer(ServerBuilder<?> serverBuilder, NodeStore nodeStore) {
        this.nodeStore = nodeStore;
        this.nodeBuilderRepository = new NodeBuilderRepository();
        this.nodeStateRepository = new NodeStateRepository();
        this.server = serverBuilder
                .addService(new CheckpointService(nodeStore, nodeStateRepository))
                .addService(new NodeBuilderService(nodeStore::getBlob, nodeStateRepository, nodeBuilderRepository))
                .addService(new NodeStateService(nodeStateRepository, nodeBuilderRepository))
                .addService(new NodeStoreService(nodeStore, nodeStateRepository, nodeBuilderRepository))
                .build();
    }

    public void start() throws IOException {
        server.start();
        log.info("Server started");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            NodeStoreServer.this.stop();
            System.err.println("*** server shut down");
        }));

    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
