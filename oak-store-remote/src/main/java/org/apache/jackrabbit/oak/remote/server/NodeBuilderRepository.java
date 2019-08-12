package org.apache.jackrabbit.oak.remote.server;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import java.util.HashMap;
import java.util.Map;

public class NodeBuilderRepository {

    private final Map<Long, NodeBuilder> nodeBuilderMap = new HashMap<>();

    private long lastId = 0;

    public synchronized long addNewNodeState(NodeBuilder nodeBuilder) {
        nodeBuilderMap.put(++lastId, nodeBuilder);
        return lastId;
    }

    public synchronized void release(long value) {
        nodeBuilderMap.remove(value);
    }

    public synchronized NodeBuilder get(long value) {
        return nodeBuilderMap.get(value);
    }

    public NodeBuilder getBuilder(NodeBuilderProtos.NodeBuilderId nodeBuilderId) throws RemoteNodeStoreException {
        NodeBuilder root = get(nodeBuilderId.getValue());
        if (root == null) {
            throw new RemoteNodeStoreException("Invalid node builder id: " + nodeBuilderId.getValue());
        }
        return root;
    }

    public NodeBuilder getBuilder(NodeBuilderProtos.NodeBuilderPath path) throws RemoteNodeStoreException {
        return getNodeBuilder(getBuilder(path.getNodeBuilderId()), path.getPath());
    }

    public static NodeBuilder getNodeBuilder(NodeBuilder root, String path) {
        NodeBuilder builder = root;
        for (String element : PathUtils.elements(path)) {
            builder = builder.getChildNode(element);
        }
        return builder;
    }
}
