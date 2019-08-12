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
