package org.apache.jackrabbit.oak.remote.server;

import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import java.util.HashMap;
import java.util.Map;

public class NodeStateRepository {

    private final Map<Long, NodeState> nodeStateMap = new HashMap<>();

    private long lastId = 0;

    public synchronized long addNewNodeState(NodeState nodeState) {
        nodeStateMap.put(++lastId, nodeState);
        return lastId;
    }

    public synchronized void release(long value) {
        nodeStateMap.remove(value);
    }

    public synchronized NodeState get(long value) {
        return nodeStateMap.get(value);
    }

    public NodeState getNodeState(NodeStateProtos.NodeStateId nodeStateId) throws RemoteNodeStoreException {
        NodeState root = get(nodeStateId.getValue());
        if (root == null) {
            throw new RemoteNodeStoreException("Invalid node state id: " + nodeStateId.getValue());
        }
        return root;
    }

    public NodeState getNodeState(NodeStateProtos.NodeStatePath path) throws RemoteNodeStoreException {
        return NodeStateUtils.getNode(getNodeState(path.getNodeStateId()), path.getPath());
    }

}
