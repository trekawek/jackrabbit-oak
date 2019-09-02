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
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeStateDiffProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeStateDiffProtos.NodeStateDiffEvent;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos.NodeValue;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;

import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RemoteNodeState extends AbstractNodeState {

    private final RemoteNodeStoreContext context;

    private final NodeStateId id;

    private volatile NodeValue nodeValue;

    public RemoteNodeState(RemoteNodeStoreContext context, NodeStateId id) {
        this.context = context;
        this.id = id;
    }

    @Override
    public boolean exists() {
        return getNodeValue().getExists();
    }

    @Override
    public @NotNull Iterable<? extends PropertyState> getProperties() {
        return Iterables.transform(getNodeValue().getPropertyList(), context.getPropertyDeserializer()::toOakProperty);
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return getNodeValue().getChildList().stream()
                .map(NodeValueProtos.ChildNode::getName)
                .anyMatch(Predicate.isEqual(name));
    }

    @Override
    public @NotNull NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        return getNodeValue().getChildList().stream()
                .filter(c -> name.equals(c.getName()))
                .findFirst()
                .map(NodeValueProtos.ChildNode::getNodeStateId)
                .map((id -> (NodeState) new RemoteNodeState(context, id)))
                .orElse(EmptyNodeState.MISSING_NODE);
    }

    @Override
    public @NotNull Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return getNodeValue().getChildList().stream()
                .map(c -> new MemoryChildNodeEntry(c.getName(), new RemoteNodeState(context, c.getNodeStateId())))
                .collect(Collectors.toList());
    }

    @Override
    public @NotNull NodeBuilder builder() {
        NodeBuilderProtos.NodeBuilderId builderId = context.getClient().getNodeStateService().createNodeBuilder(id);
        context.addNodeBuilderId(builderId);
        return new RemoteNodeBuilder(context, builderId);
    }

    @Override
    public int hashCode() {
        return getNodeValue().getHashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof RemoteNodeState) {
            RemoteNodeState remoteThat = (RemoteNodeState) that;
            if (id.equals(remoteThat.id)) {
                return true;
            } else if (context == remoteThat.context) {
                NodeStateProtos.NodeStatePathPair pair = NodeStateProtos.NodeStatePathPair.newBuilder()
                        .setNodeState1(id)
                        .setNodeState2(remoteThat.id)
                        .build();
                return context.getClient().getNodeStateService().equals(pair).getValue();
            }
        }
        return super.equals(that);
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base instanceof RemoteNodeState) {
            RemoteNodeState remoteBase = (RemoteNodeState) base;
            NodeStateProtos.CompareNodeStateOp compareOp = NodeStateProtos.CompareNodeStateOp.newBuilder()
                    .setNodeState(id)
                    .setBaseNodeState(remoteBase.id)
                    .build();
            NodeStateDiffProtos.NodeStateDiff diffResult = context.getClient().getNodeStateService().compare(compareOp);
            boolean cont = true;
            for (NodeStateDiffEvent e : diffResult.getEventsList()) {
                if (!cont) {
                    break;
                }
                switch (e.getEventValueCase()) {
                    case PROPERTYADDED: {
                        NodeStateDiffProtos.PropertyAdded ev = e.getPropertyAdded();
                        cont = diff.propertyAdded(context.getPropertyDeserializer().toOakProperty(ev.getAfter()));
                    }
                    break;

                    case PROPERTYCHANGED: {
                        NodeStateDiffProtos.PropertyChanged ev = e.getPropertyChanged();
                        cont = diff.propertyChanged(context.getPropertyDeserializer().toOakProperty(ev.getBefore()), context.getPropertyDeserializer().toOakProperty(ev.getAfter()));
                    }
                    break;

                    case PROPERTYDELETED: {
                        NodeStateDiffProtos.PropertyDeleted ev = e.getPropertyDeleted();
                        cont = diff.propertyDeleted(context.getPropertyDeserializer().toOakProperty(ev.getBefore()));
                    }
                    break;

                    case NODEADDED: {
                        NodeStateDiffProtos.NodeAdded ev = e.getNodeAdded();
                        cont = diff.childNodeAdded(ev.getName(), new RemoteNodeState(context, ev.getAfter()));
                    }
                    break;

                    case NODECHANGED: {
                        NodeStateDiffProtos.NodeChanged ev = e.getNodeChanged();
                        cont = diff.childNodeChanged(ev.getName(), new RemoteNodeState(context, ev.getBefore()), new RemoteNodeState(context, ev.getAfter()));
                    }
                    break;

                    case NODEDELETED: {
                        NodeStateDiffProtos.NodeDeleted ev = e.getNodeDeleted();
                        cont = diff.childNodeDeleted(ev.getName(), new RemoteNodeState(context, ev.getBefore()));
                    }
                    break;
                }
            }
            return cont;
        } else {
            return super.compareAgainstBaseState(base, diff);
        }
    }

    private NodeValue getNodeValue() {
        if (nodeValue == null) {
            synchronized (this) {
                if (nodeValue == null) {
                    nodeValue = context.getClient().getNodeStateService().getNodeValue(id);
                }
            }
        }
        return nodeValue;
    }

    public NodeStateId getNodeStateId() {
        return id;
    }

}
