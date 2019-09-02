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

import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStore;
import org.apache.jackrabbit.oak.remote.client.RemoteNodeStoreClient;
import org.apache.jackrabbit.oak.remote.server.NodeStoreServer;
import org.apache.jackrabbit.oak.remote.server.SegmentWriteListener;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.spotify.docker.client.DockerClient.LogsParam.follow;
import static com.spotify.docker.client.DockerClient.LogsParam.stderr;
import static com.spotify.docker.client.DockerClient.LogsParam.stdout;

public class RemoteStoreFixture extends NodeStoreFixture {

    private static final Logger log = LoggerFactory.getLogger(RemoteStoreFixture.class);

    private Map<NodeStore, RemoteNSInstance> instances = new IdentityHashMap<>();

    private Map<NodeStore, RemoteNSClient> clusterInstances = new IdentityHashMap<>();

    private RemoteNSServer sharedInstance;

    private int index;

    private AzureDockerContainer dockerContainer = new AzureDockerContainer();

    private Closer dockerCloser = Closer.create();

    @Override
    public NodeStore createNodeStore() {
        try {
            dockerContainer.startDocker();
        } catch (DockerException | InterruptedException | DockerCertificateException e) {
            throw new IllegalStateException(e);
        }

        RemoteNSInstance instance = new RemoteNSInstance("test-" + index++);
        NodeStore nodeStore = instance.getClient().getNodeStore();
        instances.put(nodeStore, instance);
        return nodeStore;
    }

    @Override
    public NodeStore createNodeStore(int clusterNodeId) {
        try {
            dockerContainer.startDocker();
        } catch (DockerException | InterruptedException | DockerCertificateException e) {
            throw new IllegalStateException(e);
        }
        if (sharedInstance == null) {
            sharedInstance = new RemoteNSServer("test-" + index++);
        }
        RemoteNSClient clientInstance = new RemoteNSClient(sharedInstance);
        NodeStore nodeStore = clientInstance.getNodeStore();
        clusterInstances.put(nodeStore, clientInstance);
        return nodeStore;
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        try {
            if (instances.containsKey(nodeStore)) {
                instances.remove(nodeStore).close();
            }
            if (clusterInstances.containsKey(nodeStore)) {
                clusterInstances.remove(nodeStore).close();
            }
            if (clusterInstances.isEmpty() && sharedInstance != null) {
                sharedInstance.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Can't dispose nodestore", e);
        }

        if (instances.isEmpty() && clusterInstances.isEmpty()) {
            try {
                dockerCloser.close();
            } catch (IOException e) {
                throw new IllegalStateException("Can't stop docker", e);
            }
        }
    }

    private class RemoteNSServer implements Closeable {

        private final Closer closer = Closer.create();

        private final String name;

        private SegmentNodeStore segmentNodeStore;

        private FileStore fileStore;

        private SegmentWriteListener listener;

        private BlobStore blobStore;

        public RemoteNSServer(String name) {
            this.name = name;

            createDataStore();
            createAzureSegmentStore();
            createServer();
        }

        private void createDataStore() {
            FileDataStore fds = new FileDataStore();

            File datastore = Files.createTempDir();
            closer.register(() -> FileUtils.deleteDirectory(datastore));
            fds.setPath(datastore.getPath());
            fds.init(null);
            blobStore = new DataStoreBlobStore(fds);
        }

        private void createAzureSegmentStore() {
            try {
                CloudBlobContainer container = dockerContainer.getContainer(name);
                container.deleteIfExists();
                container.create();

                AzurePersistence persistence = new AzurePersistence(container.getDirectoryReference("oak"));
                File segmentStore = Files.createTempDir();
                closer.register(() -> FileUtils.deleteDirectory(segmentStore));

                listener = new SegmentWriteListener();
                fileStore = FileStoreBuilder.fileStoreBuilder(segmentStore)
                        .withBlobStore(blobStore)
                        .withCustomPersistence(persistence)
                        .withIOMonitor(listener)
                        .build();
                closer.register(fileStore);
                segmentNodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private void createServer() {
            InProcessServerBuilder inProcessServerBuilder = InProcessServerBuilder.forName(name);
            NodeStoreServer server = new NodeStoreServer(inProcessServerBuilder, segmentNodeStore, fileStore, blobStore, listener);
            try {
                server.start();
                closer.register(() -> {
                    server.stop();
                });
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        public void close() throws IOException {
            closer.close();
        }
    }

    private class RemoteNSClient implements Closeable {

        private final Closer closer = Closer.create();

        private final BlobStore blobStore;

        private final String name;

        private RemoteNodeStore remoteNodeStore;

        public RemoteNSClient(RemoteNSServer server) {
            this.blobStore = server.blobStore;
            this.name = server.name;

            createNodeStore();
        }

        private void createNodeStore() {
            InProcessChannelBuilder inProcessChannelBuilder = InProcessChannelBuilder.forName(name);
            RemoteNodeStoreClient client = new RemoteNodeStoreClient(inProcessChannelBuilder);
            try {
                File segmentStore = Files.createTempDir();
                closer.register(() -> FileUtils.deleteDirectory(segmentStore));
                remoteNodeStore = new RemoteNodeStore.Builder()
                        .setBlobStore(blobStore)
                        .setClient(client)
                        .setLocalPersistence(new TarPersistence(segmentStore))
                        .setSharedPersistence(new AzurePersistence(dockerContainer.getContainer(name).getDirectoryReference("oak")))
                        .build();
                closer.register(remoteNodeStore);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        public NodeStore getNodeStore() {
            return remoteNodeStore;
        }

        public void close() throws IOException {
            closer.close();
        }
    }

    private class RemoteNSInstance implements Closeable {

        private final RemoteNSServer server;

        private final RemoteNSClient client;

        private final Closer closer = Closer.create();

        public RemoteNSInstance(String name) {
            server = new RemoteNSServer(name);
            closer.register(server);
            client = new RemoteNSClient(server);
            closer.register(client);
        }

        public RemoteNSClient getClient() {
            return client;
        }

        public void close() throws IOException {
            closer.close();
        }
    }

    private static class AzureDockerContainer implements Closeable {

        private Closer closer;

        private int port;

        public void startDocker() throws DockerException, InterruptedException, DockerCertificateException {
            if (closer != null) {
                return;
            }

            closer = Closer.create();

            DockerClient docker = DefaultDockerClient.fromEnv().build();
            closer.register(docker);
            docker.pull("trekawek/azurite");

            Map<String, List<PortBinding>> portBindings = new HashMap<>();
            PortBinding randomPort = PortBinding.randomPort("0.0.0.0");
            portBindings.put("10000", Arrays.asList(randomPort));
            HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();

            File dataVolume = Files.createTempDir();
            closer.register(() -> FileUtils.deleteDirectory(dataVolume));

            final ContainerConfig containerConfig = ContainerConfig.builder()
                    .hostConfig(hostConfig)
                    .image("trekawek/azurite")
                    .addVolume(dataVolume.getPath() + ":/data")
                    .env("executable=blob")
                    .exposedPorts("10000")
                    .build();

            ContainerCreation creation = docker.createContainer(containerConfig);
            String id = creation.id();
            closer.register(() -> {
                try {
                    docker.removeContainer(id);
                } catch (DockerException | InterruptedException e) {
                    throw new IOException(e);
                }
            });

            docker.startContainer(id);
            closer.register(() -> {
                try {
                    docker.killContainer(id);
                } catch (DockerException | InterruptedException e) {
                    throw new IOException(e);
                }
            });

            LogStream logStream = docker.logs(id, follow(), stdout(), stderr());
            while (logStream.hasNext()) {
                String line = StandardCharsets.UTF_8.decode(logStream.next().content()).toString();
                log.info("{}", line);
                if (line.contains("Azure Blob Storage Emulator listening on port 10000")) {
                    break;
                }
            }

            Map<String, List<PortBinding>> ports = docker.inspectContainer(id).networkSettings().ports();
            port = Integer.valueOf(ports.get("10000/tcp").get(0).hostPort());
        }

        private CloudBlobContainer getContainer(String name) throws URISyntaxException, InvalidKeyException, StorageException {
            if (closer == null) {
                throw new IllegalStateException("Docker is not started");
            }
            CloudStorageAccount cloud = CloudStorageAccount.parse("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:" + port + "/devstoreaccount1;");
            CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(name);
            return container;
        }

        public void close() throws IOException {
            if (closer != null) {
                closer.close();
                closer = null;
            }
        }
    }
}
