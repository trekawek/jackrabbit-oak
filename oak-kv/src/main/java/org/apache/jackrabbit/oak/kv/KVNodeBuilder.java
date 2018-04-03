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

package org.apache.jackrabbit.oak.kv;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Store;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class KVNodeBuilder extends MemoryNodeBuilder {

    private final Store store;

    private final BlobStore blobStore;

    KVNodeBuilder(Store store, BlobStore blobStore, NodeState base) {
        super(base);
        this.store = store;
        this.blobStore = blobStore;
    }

    private KVNodeBuilder(KVNodeBuilder parent, String name) {
        super(parent, name);
        this.store = parent.store;
        this.blobStore = parent.blobStore;
    }

    @Override
    protected MemoryNodeBuilder createChildBuilder(String name) {
        return new KVNodeBuilder(this, name);
    }

    boolean isRootBuilder() {
        return super.isRoot();
    }

    @Override
    public NodeState getNodeState() {
        NodeState nodeState = super.getNodeState();

        if (nodeState instanceof ModifiedNodeState) {
            KVNodeState persistedState = writeNodeStateUnchecked(nodeState);
            set(persistedState);
            return persistedState;
        }

        return nodeState;
    }

    private KVNodeState writeNodeStateUnchecked(NodeState nodeState) {
        try {
            return writeNodeState(nodeState);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private KVNodeState writeNodeState(NodeState nodeState) throws IOException {
        if (nodeState instanceof KVNodeState) {
            return (KVNodeState) nodeState;
        }
        return newNodeState(writeNode(nodeState));
    }

    private KVNodeState newNodeState(ID id) throws IOException {
        return new KVNodeState(store, blobStore, id, store.getNode(id));
    }

    private ID writeNode(NodeState nodeState) throws IOException {
        Map<String, ID> children = new HashMap<>();

        for (ChildNodeEntry entry : nodeState.getChildNodeEntries()) {
            children.put(entry.getName(), writeNode(entry.getNodeState()));
        }

        Map<String, Value> properties = new HashMap<>();

        for (PropertyState propertyState : nodeState.getProperties()) {
            properties.put(propertyState.getName(), newValue(propertyState));
        }

        return store.putNode(properties, children);
    }

    private Value newValue(PropertyState ps) throws IOException {
        if (ps.getType() == Type.STRING) {
            return Value.newStringValue(ps.getValue(Type.STRING));
        }
        if (ps.getType() == Type.BINARY) {
            return Value.newBinaryValue(writeBlob(ps.getValue(Type.BINARY)));
        }
        if (ps.getType() == Type.LONG) {
            return Value.newLongValue(ps.getValue(Type.LONG));
        }
        if (ps.getType() == Type.DOUBLE) {
            return Value.newDoubleValue(ps.getValue(Type.DOUBLE));
        }
        if (ps.getType() == Type.DATE) {
            return Value.newDateValue(ps.getValue(Type.DATE));
        }
        if (ps.getType() == Type.BOOLEAN) {
            return Value.newBooleanValue(ps.getValue(Type.BOOLEAN));
        }
        if (ps.getType() == Type.NAME) {
            return Value.newNameValue(ps.getValue(Type.NAME));
        }
        if (ps.getType() == Type.PATH) {
            return Value.newPathValue(ps.getValue(Type.PATH));
        }
        if (ps.getType() == Type.REFERENCE) {
            return Value.newReferenceValue(ps.getValue(Type.REFERENCE));
        }
        if (ps.getType() == Type.WEAKREFERENCE) {
            return Value.newWeakReferenceValue(ps.getValue(Type.WEAKREFERENCE));
        }
        if (ps.getType() == Type.URI) {
            return Value.newURIValue(ps.getValue(Type.URI));
        }
        if (ps.getType() == Type.DECIMAL) {
            return Value.newDecimalValue(ps.getValue(Type.DECIMAL));
        }
        if (ps.getType() == Type.STRINGS) {
            return Value.newStringArray(ps.getValue(Type.STRINGS));
        }
        if (ps.getType() == Type.BINARIES) {
            return Value.newBinaryArray(writeBlobs(ps.getValue(Type.BINARIES)));
        }
        if (ps.getType() == Type.LONGS) {
            return Value.newLongArray(ps.getValue(Type.LONGS));
        }
        if (ps.getType() == Type.DOUBLES) {
            return Value.newDoubleArray(ps.getValue(Type.DOUBLES));
        }
        if (ps.getType() == Type.DATES) {
            return Value.newDateArray(ps.getValue(Type.DATES));
        }
        if (ps.getType() == Type.BOOLEANS) {
            return Value.newBooleanArray(ps.getValue(Type.BOOLEANS));
        }
        if (ps.getType() == Type.NAMES) {
            return Value.newNameArray(ps.getValue(Type.NAMES));
        }
        if (ps.getType() == Type.PATHS) {
            return Value.newPathArray(ps.getValue(Type.PATHS));
        }
        if (ps.getType() == Type.REFERENCES) {
            return Value.newReferenceArray(ps.getValue(Type.REFERENCES));
        }
        if (ps.getType() == Type.WEAKREFERENCES) {
            return Value.newWeakReferenceArray(ps.getValue(Type.WEAKREFERENCES));
        }
        if (ps.getType() == Type.URIS) {
            return Value.newURIArray(ps.getValue(Type.URIS));
        }
        if (ps.getType() == Type.DECIMALS) {
            return Value.newDecimalArray(ps.getValue(Type.DECIMALS));
        }
        throw new IllegalArgumentException("ps");
    }

    private Iterable<String> writeBlobs(Iterable<Blob> it) throws IOException {
        List<String> result = new ArrayList<>();

        for (Blob blob : it) {
            result.add(writeBlob(blob));
        }

        return result;
    }

    private String writeBlob(Blob blob) throws IOException {
        return blobStore.getReference(blobStore.writeBlob(blob.getNewStream()));
    }

}
