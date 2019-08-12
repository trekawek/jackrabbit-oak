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

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStatePath;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos.NodeValue;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

public class RemoteNodeState extends AbstractNodeState {

    private final RemoteNodeStoreContext context;

    private final NodeStateId id;

    private final String path;

    private volatile NodeValue nodeValue;

    public RemoteNodeState(RemoteNodeStoreContext context, NodeStateId id) {
        this.context = context;
        this.id = id;
        this.path = "/";
    }

    private RemoteNodeState(RemoteNodeState parent, String name) {
        this.context = parent.context;
        this.id = parent.id;
        this.path = PathUtils.concat(parent.path, name);
    }

    @Override
    public boolean exists() {
        return getNodeValue().getExists();
    }

    @Override
    public @NotNull Iterable<? extends PropertyState> getProperties() {
        return Iterables.transform(getNodeValue().getPropertyList(), context.getPropertyDeserializer()::unsafeToOakProperty);
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return getNodeValue().getChildNameList().contains(name);
    }

    @Override
    public @NotNull NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        return new RemoteNodeState(this, name);
    }

    @Override
    public @NotNull Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return Iterables.transform(getNodeValue().getChildNameList(), name -> new MemoryChildNodeEntry(name, getChildNode(name)));
    }

    @Override
    public @NotNull NodeBuilder builder() {
        NodeStatePath request = NodeStatePath.newBuilder()
                .setNodeStateId(id)
                .setPath(path)
                .build();
        NodeBuilderProtos.NodeBuilderId builderId = context.getClient().getNodeStateService().createNodeBuilder(request);
        context.addNodeBuilderId(builderId);
        return new RemoteNodeBuilder(context, builderId);
    }

    private NodeValue getNodeValue() {
        if (nodeValue == null) {
            synchronized (this) {
                if (nodeValue == null) {
                    NodeStatePath request = NodeStatePath.newBuilder()
                            .setNodeStateId(id)
                            .setPath(path)
                            .build();
                    nodeValue = context.getClient().getNodeStateService().getNodeValue(request);
                }
            }
        }
        return nodeValue;
    }

    public String getPath() {
        return path;
    }

    public NodeStateId getNodeStateId() {
        return id;
    }
}
