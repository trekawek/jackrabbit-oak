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
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Store;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVNodeStore implements NodeStore, Observable {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Store store;

    private final BlobStore blobStore;

    public KVNodeStore(Store store, BlobStore blobStore) {
        this.store = store;
        this.blobStore = blobStore;
    }

    @Override
    public Closeable addObserver(Observer observer) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public NodeState getRoot() {
        ID rootID;

        lock.readLock().lock();
        try {
            rootID = store.getTag("root");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }

        if (rootID == null) {
            lock.writeLock().lock();
            try {
                rootID = store.getTag("root");
                if (rootID == null) {
                    rootID = store.putNode(Collections.emptyMap(), Collections.emptyMap());
                }
                store.putTag("root", rootID);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                lock.writeLock().unlock();
            }
        }

        Node rootNode;
        try {
            rootNode = store.getNode(rootID);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new KVNodeState(store, blobStore, rootID, rootNode);
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        if (builder instanceof KVNodeBuilder) {
            return merge((KVNodeBuilder) builder, commitHook, info);
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState merge(KVNodeBuilder builder, CommitHook commitHook, CommitInfo commitInfo) throws CommitFailedException {
        if (builder.isRootBuilder()) {
            try {
                return merge(builder, builder.getBaseState(), builder.getNodeState(), commitHook, commitInfo);
            } catch (IOException e) {
                throw new CommitFailedException(CommitFailedException.OAK, -1, "I/O error", e);
            }
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState merge(KVNodeBuilder builder, NodeState baseState, NodeState headState, CommitHook commitHook, CommitInfo commitInfo) throws IOException, CommitFailedException {
        lock.writeLock().lock();
        try {
            ID upstreamID = store.getTag("root");

            if (upstreamID == null) {
                throw new IllegalStateException("invalid upstream state");
            }

            ID baseID = null;

            if (baseState instanceof KVNodeState) {
                baseID = ((KVNodeState) baseState).getID();
            }

            if (baseID == null) {
                throw new IllegalStateException("invalid base state");
            }

            KVNodeState mergedState;

            if (baseID.equals(upstreamID)) {
                mergedState = (KVNodeState) commitHook.processCommit(baseState, headState, commitInfo);
            } else {
                NodeBuilder upstreamBuilder = new KVNodeState(store, blobStore, upstreamID, store.getNode(upstreamID)).builder();
                headState.compareAgainstBaseState(baseState, new ConflictAnnotatingRebaseDiff(upstreamBuilder));
                mergedState = (KVNodeState) commitHook.processCommit(upstreamBuilder.getBaseState(), upstreamBuilder.getNodeState(), commitInfo);
            }

            store.putTag("root", mergedState.getID());
            builder.reset(mergedState);
            return mergedState;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public NodeState rebase(NodeBuilder builder) {
        if (builder instanceof KVNodeBuilder) {
            return rebase((KVNodeBuilder) builder);
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState rebase(KVNodeBuilder builder) {
        if (builder.isRootBuilder()) {
            try {
                return rebase(builder, builder.getBaseState(), builder.getNodeState());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState rebase(KVNodeBuilder builder, NodeState baseState, NodeState headState) throws IOException {
        ID upstreamID = store.getTag("root");

        if (upstreamID == null) {
            return headState;
        }

        ID baseID = null;

        if (baseState instanceof KVNodeState) {
            baseID = ((KVNodeState) baseState).getID();
        }

        if (baseID == null) {
            throw new IllegalStateException("invalid base state");
        }

        if (baseID.equals(upstreamID)) {
            return headState;
        }

        builder.reset(new KVNodeState(store, blobStore, upstreamID, store.getNode(upstreamID)));
        headState.compareAgainstBaseState(baseState, new ConflictAnnotatingRebaseDiff(builder));
        return builder.getNodeState();
    }

    @Override
    public NodeState reset(NodeBuilder builder) {
        if (builder instanceof KVNodeBuilder) {
            return reset((KVNodeBuilder) builder);
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState reset(KVNodeBuilder builder) {
        if (builder.isRootBuilder()) {
            NodeState root = getRoot();
            builder.reset(root);
            return root;
        }
        throw new IllegalArgumentException("builder");
    }

    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        return new KVBlob(blobStore, blobStore.writeBlob(inputStream));
    }

    @Override
    public Blob getBlob(String reference) {
        String blobId = blobStore.getBlobId(reference);

        if (blobId == null) {
            return null;
        }

        return new KVBlob(blobStore, blobId);
    }

    @Override
    public String checkpoint(long lifetime, Map<String, String> properties) {
        try {
            return checkpoint(UUID.randomUUID().toString(), lifetime, properties);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections.emptyMap());
    }

    private String checkpoint(String reference, long lifetime, Map<String, String> properties) throws IOException {
        ID rootID = store.getTag("root");

        if (rootID == null) {
            return reference;
        }

        ID propertiesID = createProperties(properties);
        ID checkpointID = createCheckpoint(lifetime, propertiesID, rootID);
        ID checkpointsID = createOrUpdateCheckpoints(reference, checkpointID);
        store.putTag("checkpoints", checkpointsID);

        return reference;
    }

    private ID createProperties(Map<String, String> values) throws IOException {
        Map<String, Value> properties = new HashMap<>();

        for (Map.Entry<String, String> entry : values.entrySet()) {
            properties.put(entry.getKey(), Value.newStringValue(entry.getValue()));
        }

        return store.putNode(properties, Collections.emptyMap());
    }

    private ID createCheckpoint(long lifetime, ID propertiesID, ID rootID) throws IOException {
        Map<String, Value> properties = new HashMap<>();
        properties.put("lifetime", Value.newLongValue(lifetime));

        Map<String, ID> children = new HashMap<>();
        children.put("properties", propertiesID);
        children.put("root", rootID);

        return store.putNode(properties, children);
    }

    private ID createOrUpdateCheckpoints(String reference, ID checkpoint) throws IOException {
        ID id = store.getTag("checkpoints");

        if (id == null) {
            return createCheckpoints(reference, checkpoint);
        }

        return updateCheckpoints(store.getNode(id), reference, checkpoint);
    }

    private ID createCheckpoints(String reference, ID checkpoint) throws IOException {
        return store.putNode(Collections.emptyMap(), Collections.singletonMap(reference, checkpoint));
    }

    private ID updateCheckpoints(Node node, String reference, ID checkpoint) throws IOException {
        Map<String, ID> children = new HashMap<>(node.getChildren());
        children.put(reference, checkpoint);
        return store.putNode(node.getProperties(), children);
    }

    @Override
    public Map<String, String> checkpointInfo(String reference) {
        try {
            return readCheckpointInfo(reference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> readCheckpointInfo(String reference) throws IOException {
        ID checkpointsID = store.getTag("checkpoints");

        if (checkpointsID == null) {
            return Collections.emptyMap();
        }

        Node checkpoints = store.getNode(checkpointsID);

        if (checkpoints == null) {
            throw new IOException("invalid checkpoints ID");
        }

        ID checkpointID = checkpoints.getChildren().get(reference);

        if (checkpointID == null) {
            return Collections.emptyMap();
        }

        Node checkpoint = store.getNode(checkpointID);

        if (checkpoint == null) {
            throw new IOException("invalid checkpoint ID");
        }

        ID propertiesID = checkpoint.getChildren().get("properties");

        if (propertiesID == null) {
            return Collections.emptyMap();
        }

        Node properties = store.getNode(propertiesID);

        if (properties == null) {
            throw new IOException("invalid properties ID");
        }

        Map<String, String> values = new HashMap<>();

        for (Map.Entry<String, Value> entry : properties.getProperties().entrySet()) {
            values.put(entry.getKey(), (String) entry.getValue().getValue());
        }

        return values;
    }

    @Override
    public Iterable<String> checkpoints() {
        try {
            return readCheckpoints();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterable<String> readCheckpoints() throws IOException {
        ID checkpointsID = store.getTag("checkpoints");

        if (checkpointsID == null) {
            return Collections.emptySet();
        }

        Node checkpoints = store.getNode(checkpointsID);

        if (checkpoints == null) {
            throw new IOException("invalid checkpoints ID");
        }

        return checkpoints.getChildren().keySet();
    }

    @Override
    public NodeState retrieve(String reference) {
        try {
            return readRoot(reference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private NodeState readRoot(String reference) throws IOException {
        ID checkpointsID = store.getTag("checkpoints");

        if (checkpointsID == null) {
            return null;
        }

        Node checkpoints = store.getNode(checkpointsID);

        if (checkpoints == null) {
            throw new IOException("invalid checkpoints ID");
        }

        ID checkpointID = checkpoints.getChildren().get(reference);

        if (checkpointID == null) {
            return null;
        }

        Node checkpoint = store.getNode(checkpointID);

        if (checkpoint == null) {
            throw new IOException("invalid checkpoint ID");
        }

        ID rootID = checkpoint.getChildren().get("root");

        if (rootID == null) {
            throw new IOException("missing root ID");
        }

        Node root = store.getNode(rootID);

        if (root == null) {
            throw new IOException("invalid root ID");
        }

        return new KVNodeState(store, blobStore, rootID, root);
    }

    @Override
    public boolean release(String reference) {
        try {
            return deleteCheckpoint(reference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean deleteCheckpoint(String reference) throws IOException {
        ID checkpointsID = store.getTag("checkpoints");

        if (checkpointsID == null) {
            return true;
        }

        Node checkpoints = store.getNode(checkpointsID);

        if (checkpoints == null) {
            throw new IOException("invalid checkpoints ID");
        }

        if (checkpoints.getChildren().containsKey(reference)) {
            return deleteCheckpoint(checkpoints, reference);
        }

        return true;
    }

    private boolean deleteCheckpoint(Node checkpoints, String reference) throws IOException {
        Map<String, ID> children = new HashMap<>(checkpoints.getChildren());
        children.remove(reference);
        ID checkpointsID = store.putNode(checkpoints.getProperties(), children);
        store.putTag("checkpoints", checkpointsID);
        return true;
    }

}
