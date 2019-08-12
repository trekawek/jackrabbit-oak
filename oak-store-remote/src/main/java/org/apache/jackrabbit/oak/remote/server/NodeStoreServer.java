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

    private final int port;

    private final Server server;

    public NodeStoreServer(int port, NodeStore nodeStore) throws IOException {
        this(ServerBuilder.forPort(port), port, nodeStore);
    }

    public NodeStoreServer(ServerBuilder<?> serverBuilder, int port, NodeStore nodeStore) {
        this.port = port;
        this.nodeStore = nodeStore;
        this.nodeBuilderRepository = new NodeBuilderRepository();
        this.nodeStateRepository = new NodeStateRepository();
        this.server = serverBuilder
                .addService(new CheckpointService(nodeStore, nodeStateRepository))
                .addService(new NodeBuilderService(nodeStore, nodeStateRepository, nodeBuilderRepository))
                .addService(new NodeStateService(nodeStateRepository, nodeBuilderRepository))
                .addService(new NodeStoreService(nodeStore, nodeStateRepository, nodeBuilderRepository))
                .build();
    }

    public void start() throws IOException {
        server.start();
        log.info("Server started, listening on " + port);
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
