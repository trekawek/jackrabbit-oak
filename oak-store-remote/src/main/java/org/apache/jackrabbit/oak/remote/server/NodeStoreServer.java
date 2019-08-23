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
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.RevisionableNodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NodeStoreServer {

    private static final Logger log = LoggerFactory.getLogger(NodeStoreServer.class);

    private final RevisionableNodeStore nodeStore;

    private final Server server;

    public NodeStoreServer(int port, SegmentNodeStore nodeStore, FileStore fileStore, BlobStore blobStore, SegmentWriteListener segmentWriteListener) {
        this(ServerBuilder.forPort(port), nodeStore, fileStore, blobStore, segmentWriteListener);
    }

    public NodeStoreServer(ServerBuilder<?> serverBuilder, SegmentNodeStore nodeStore, FileStore fileStore, BlobStore blobStore, SegmentWriteListener segmentWriteListener) {
        this.nodeStore = nodeStore;
        this.server = serverBuilder
                .addService(new CheckpointService(nodeStore))
                .addService(new NodeStoreService(nodeStore, fileStore, blobStore))
                .addService(new LeaseService(nodeStore))
                .addService(new SegmentService(segmentWriteListener, fileStore))
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
