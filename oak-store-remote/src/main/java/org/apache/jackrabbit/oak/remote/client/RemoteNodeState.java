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

import com.google.common.collect.FluentIterable;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;

public class RemoteNodeState extends AbstractNodeState {

    private final RemoteNodeStoreContext context;

    private final RemoteNodeState parent;

    private final String name;

    private volatile NodeState nodeValue;

    private volatile String recordId;

    public RemoteNodeState(RemoteNodeStoreContext context, String recordId) {
        this.context = context;
        this.parent = null;
        this.name = null;
        this.nodeValue = null;
        this.recordId = recordId;
    }

    private RemoteNodeState(RemoteNodeState parent, String name, SegmentNodeState nodeValue) {
        this.context = parent.context;
        this.parent = parent;
        this.name = name;
        this.nodeValue = nodeValue;
        this.recordId = null;
    }

    public static boolean fastEquals(NodeState node1, NodeState node2) {
        if (node1 instanceof RemoteNodeState && node2 instanceof RemoteNodeState) {
            RemoteNodeState remoteNode1 = (RemoteNodeState) node1;
            RemoteNodeState remoteNode2 = (RemoteNodeState) node2;
            return SegmentNodeState.fastEquals(remoteNode1.getNodeValue(), remoteNode2.getNodeValue());
        } else if (node1 instanceof EmptyNodeState && node2 instanceof EmptyNodeState) {
            return node1.equals(node2);
        } else {
            return false;
        }
    }

    @Override
    public boolean exists() {
        return getNodeValue().exists();
    }

    @Override
    public @NotNull Iterable<? extends PropertyState> getProperties() {
        return getNodeValue().getProperties();
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return getNodeValue().hasChildNode(name);
    }

    @Override
    @NotNull
    public RemoteNodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        return new RemoteNodeState(this, name, null);
    }

    @Override
    @NotNull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return FluentIterable.from(getNodeValue().getChildNodeEntries())
                .transform(e -> new MemoryChildNodeEntry(e.getName(), new RemoteNodeState(this, name, (SegmentNodeState) e.getNodeState())));
    }

    @Override
    public @NotNull NodeBuilder builder() {
        return new RemoteNodeBuilder(context, this);
    }

    @Override
    public int hashCode() {
        return getNodeValue().hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof RemoteNodeState) {
            RemoteNodeState remoteThat = (RemoteNodeState) that;
            return getNodeValue().equals(remoteThat.getNodeValue());
        } else {
            return super.equals(that);
        }
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base instanceof RemoteNodeState) {
            return this.getNodeValue().compareAgainstBaseState(((RemoteNodeState) base).getNodeValue(), diff);
        } else {
            return super.compareAgainstBaseState(base, diff);
        }
    }

    private NodeState getNodeValue() {
        if (nodeValue == null) {
            synchronized (this) {
                if (nodeValue == null) {
                    if (recordId != null) {
                        nodeValue = context.loadNode(recordId);
                    } else if (parent != null) {
                        nodeValue = parent.getNodeValue().getChildNode(name);
                    } else {
                        throw new IllegalStateException("Either recordId or parent shouldn't be empty");
                    }
                }
            }
        }
        return nodeValue;
    }

    public String getRevision() {
        if (recordId != null) {
            return recordId;
        } else if (getNodeValue() instanceof SegmentNodeState) {
            return ((SegmentNodeState) getNodeValue()).getRecordId().toString();
        } else {
            throw new IllegalStateException();
        }
    }
}