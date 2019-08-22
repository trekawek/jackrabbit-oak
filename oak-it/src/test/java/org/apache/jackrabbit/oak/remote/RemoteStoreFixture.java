/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.remote;

import com.google.common.io.Files;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStore;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStoreClient;
import org.apache.jackrabbit.oak.remote.server.NodeStoreServer;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RemoteStoreFixture extends NodeStoreFixture {

    private Map<NodeStore, RemoteNodeStoreInstance> instances = new IdentityHashMap<>();

    private Map<NodeStore, RemoteNodeStoreInstance> clusterInstances = new IdentityHashMap<>();

    private AtomicLong index = new AtomicLong();

    private BlobStore sharedBlobStore;

    private RemoteNodeStoreInstance sharedRemoteServer;

    @Override
    public NodeStore createNodeStore() {
        RemoteNodeStoreInstance instance = null;
        try {
            instance = new RemoteNodeStoreInstance("oak-it-" + index.incrementAndGet(), new MemoryBlobStore(), true);
        } catch (IOException | InvalidFileStoreVersionException e) {
            throw new RuntimeException(e);
        }
        NodeStore ns = instance.getNs();
        instances.put(ns, instance);
        return ns;
    }

    public NodeStore createNodeStore(int clusterNodeId) {
        if (sharedRemoteServer == null) {
            sharedBlobStore = new MemoryBlobStore();
            try {
                sharedRemoteServer = new RemoteNodeStoreInstance("oak-cluster", sharedBlobStore, true);
            } catch (IOException | InvalidFileStoreVersionException e) {
                throw new RuntimeException(e);
            }
        }

        RemoteNodeStoreInstance instance;
        try {
            instance = new RemoteNodeStoreInstance("oak-cluster", sharedBlobStore, false);
        } catch (IOException | InvalidFileStoreVersionException e) {
            throw new RuntimeException(e);
        }
        NodeStore ns = instance.getNs();
        clusterInstances.put(ns, instance);
        return ns;
    }

    public void dispose(NodeStore nodeStore) {
        if (clusterInstances.containsKey(nodeStore)) {
            clusterInstances.remove(nodeStore).close();
            if (clusterInstances.isEmpty()) {
                sharedRemoteServer.close();
                sharedRemoteServer = null;
                sharedBlobStore = null;
            }
        } else if (instances.containsKey(nodeStore)) {
            instances.remove(nodeStore).close();
        }
    }

    private static class RemoteNodeStoreInstance implements Closeable {

        private final File tempDir;

        private final FileStore fs;

        private final NodeStoreServer server;

        private final RemoteNodeStore remoteNs;

        private final BlobStore blobStore;

        public RemoteNodeStoreInstance(String name, BlobStore blobStore, boolean startServer) throws IOException, InvalidFileStoreVersionException {
            if (startServer) {
                tempDir = Files.createTempDir();
                fs = FileStoreBuilder.fileStoreBuilder(tempDir).withBlobStore(blobStore).build();
                SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
                server = createServer(name, ns);
            } else {
                tempDir = null;
                fs = null;
                server = null;
            }
            this.blobStore = blobStore;
            remoteNs = createRemoteNs(name, blobStore);
        }

        public NodeStoreServer createServer(String name, SegmentNodeStore nodeStore) throws IOException {
            InProcessServerBuilder inProcessServerBuilder = InProcessServerBuilder.forName(name);
            NodeStoreServer server = null;//new NodeStoreServer(inProcessServerBuilder, nodeStore, blobStore);
            //server.start();
            return server;
        }

        public RemoteNodeStore createRemoteNs(String name, BlobStore blobStore) {
            InProcessChannelBuilder inProcessChannelBuilder = InProcessChannelBuilder.forName(name);
            RemoteNodeStoreClient client = new RemoteNodeStoreClient(inProcessChannelBuilder);
            //return new RemoteNodeStore(client, blobStore);
            return null;
        }

        public NodeStore getNs() {
            return remoteNs;
        }

        public void close() {
            try {
                remoteNs.close();
                if (server != null) {
                    server.stop();
                }
                if (fs != null) {
                    fs.close();
                }
                if (tempDir != null) {
                    FileUtils.deleteDirectory(tempDir);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

    }
}
