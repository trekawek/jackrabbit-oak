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
