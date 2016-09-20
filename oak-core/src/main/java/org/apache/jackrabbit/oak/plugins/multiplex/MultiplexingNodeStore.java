/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.multiplex;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A <tt>NodeStore</tt> implementation that multiplexes multiple <tt>MemoryNodeStore</tt> instances
 *
 */
public class MultiplexingNodeStore implements NodeStore, Observable {
    
    // TODO - define concurrency model
    //
    // This implementation operates on multiple mounted stores and is generally expected to be 
    // thread safe. From a publication point of view this is achieved. It is up for debate
    // whether we need to make operations atomic, or rely on the internal consistency of the
    // mounted repositories. It's possible that there is some unfortunate interleaving of
    // operations which would ultimately require us to have some sort of global ordering.
    
    private static final char CHECKPOINT_MARKER = '|';

    private static final Splitter CHECKPOINT_SPLITTER = Splitter.on(CHECKPOINT_MARKER);

    private static final Joiner CHECKPOINT_JOINER = Joiner.on(CHECKPOINT_MARKER);
    
    private final MountedNodeStore globalStore;
    private final List<MountedNodeStore> nonDefaultStores;

    private final MultiplexingContext ctx;
    
    private final List<Observer> observers = new CopyOnWriteArrayList<>();
    
    private MultiplexingNodeStore(MountInfoProvider mip, NodeStore globalStore, List<MountedNodeStore> nonDefaultStore) {

        this.globalStore = new MountedNodeStore(mip.getDefaultMount(), globalStore);
        this.nonDefaultStores = ImmutableList.copyOf(nonDefaultStore);
        
        this.ctx = new MultiplexingContext(this, mip, this.globalStore, nonDefaultStores);
    }

    @Override
    public NodeState getRoot() {
        
        // the multiplexed root state exposes the node states as they are
        // at this certain point in time, so we eagerly retrieve them from all stores
        
        // register the initial builder as affected as it's already instantiated
        Map<MountedNodeStore, NodeState> nodeStates = newHashMap();
        MountedNodeStore owningStore = ctx.getOwningStore("/");
        NodeState rootState = owningStore.getNodeStore().getRoot();
        nodeStates.put(owningStore, rootState);

        for (MountedNodeStore nodeStore : nonDefaultStores) {
            nodeStates.put(nodeStore, nodeStore.getNodeStore().getRoot());
        }

        return new MultiplexingNodeState("/", ctx, Collections.<String>emptyList(), nodeStates);
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        checkArgument(builder instanceof MultiplexingNodeBuilder);
        
        MultiplexingNodeBuilder nodeBuilder = (MultiplexingNodeBuilder) builder;

        // since we maintain a mapping of _root_ NodeBuilder instances for all mounted stores
        // we need to check ourselves against merging a non-root node
        checkArgument(nodeBuilder.getPath().equals("/"));

        NodeState processed = commitHook.processCommit(getRoot(), rebase(nodeBuilder), info);
        processed.compareAgainstBaseState(builder.getNodeState(), new ApplyDiff(nodeBuilder));

        for (Map.Entry<MountedNodeStore, NodeBuilder> e : nodeBuilder.getRootBuilders().entrySet() ) {
            NodeStore nodeStore = e.getKey().getNodeStore();
            NodeBuilder rootBuilder = e.getValue();
            nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, info);
        }
        
        NodeState newRoot = getRoot();
        for (Observer observer : observers) {
            observer.contentChanged(newRoot, info);
        }
        return newRoot;
   }

    @Override
    public NodeState rebase(NodeBuilder builder) {
        checkArgument(builder instanceof MultiplexingNodeBuilder);
        MultiplexingNodeBuilder nodeBuilder = (MultiplexingNodeBuilder) builder;
        for (Map.Entry<MountedNodeStore, NodeBuilder> e : nodeBuilder.getRootBuilders().entrySet() ) {
            NodeStore nodeStore = e.getKey().getNodeStore();
            NodeBuilder affectedBuilder = e.getValue();
            nodeStore.rebase(affectedBuilder);
        }
        return nodeBuilder.getNodeState();
    }

    @Override
    public NodeState reset(NodeBuilder builder) {
        checkArgument(builder instanceof MultiplexingNodeBuilder);
        
        MultiplexingNodeBuilder nodeBuilder = (MultiplexingNodeBuilder) builder;
        
        for ( Map.Entry<MountedNodeStore, NodeBuilder> affectedBuilderEntry : nodeBuilder.getRootBuilders().entrySet() ) {
            
            NodeStore nodeStore = affectedBuilderEntry.getKey().getNodeStore();
            NodeBuilder affectedBuilder = affectedBuilderEntry.getValue();
            
            nodeStore.reset(affectedBuilder);
        }
        
        // TODO - is this correct or do we need a specific path?
        return getRoot();
    }

    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        
        // since there is no way to infer a path for a blob, we create all blobs in the root store
        return globalStore.getNodeStore().createBlob(inputStream);
    }

    @Override
    public Blob getBlob(String reference) {
        // blobs are searched in all stores
        Blob found = globalStore.getNodeStore().getBlob(reference);
        if ( found != null ) {
            return found;
        }
        
        for ( MountedNodeStore nodeStore : nonDefaultStores ) {
            found = nodeStore.getNodeStore().getBlob(reference);
            if ( found != null ) {
                return found;
            }
        }
        
        return null;
    }

    @Override
    public String checkpoint(long lifetime, Map<String, String> properties) {
        
        // This implementation does a best-effort attempt to join all the returned checkpoints
        // In case of failure it bails out
        List<String> checkpoints = Lists.newArrayList();
        addCheckpoint(globalStore, lifetime, properties, checkpoints);
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            addCheckpoint(mountedNodeStore, lifetime, properties, checkpoints);
        }
        
        return CHECKPOINT_JOINER.join(checkpoints);
    }
    
    private void addCheckpoint(MountedNodeStore store, long lifetime, Map<String, String> properties, List<String> accumulator) {
        
        NodeStore nodeStore = store.getNodeStore();
        String checkpoint = nodeStore.checkpoint(lifetime, properties);
        Preconditions.checkArgument(checkpoint.indexOf(CHECKPOINT_MARKER) == -1, 
                "Checkpoint %s created by NodeStore %s mounted at %s contains the invalid entry %s. Unable to add checkpoint.", 
                checkpoint, nodeStore, store.getMount().getName(), CHECKPOINT_MARKER );
        
        accumulator.add(checkpoint);
    }

    @Override
    public String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections. <String, String> emptyMap());
    }

    @Override
    public Map<String, String> checkpointInfo(String checkpoint) {
        
        // TODO - proper validation of checkpoints size compared to mounts
        Iterable<String> checkpoints = CHECKPOINT_SPLITTER.split(checkpoint);
        
        // since checkpoints are by design kept in sync between the stores
        // it's enough to query one. The root one is the most convenient
        return globalStore.getNodeStore().checkpointInfo(checkpoints.iterator().next());
    }

    @Override
    public NodeState retrieve(String checkpoint) {
        // TODO - proper validation of checkpoints size compared to mounts
        List<String> checkpoints = CHECKPOINT_SPLITTER.splitToList(checkpoint);
        Map<MountedNodeStore, NodeState> nodeStates = newHashMap();

        // global store is always first
        NodeState globalStoreNodeState = globalStore.getNodeStore().retrieve(checkpoints.get(0));
        if (globalStoreNodeState == null) {
            return null;
        }
        nodeStates.put(globalStore, globalStoreNodeState);

        int i = 1;
        for (MountedNodeStore ns : nonDefaultStores) {
            NodeState root = ns.getNodeStore().retrieve(checkpoints.get(i++));
            nodeStates.put(ns, root);
        }
        
        return new MultiplexingNodeState("/", ctx, checkpoints, nodeStates);
    }

    @Override
    public boolean release(String checkpoint) {
        
        boolean result = true;
        // TODO - proper validation of checkpoints size compared to mounts
        List<String> checkpoints = CHECKPOINT_SPLITTER.splitToList(checkpoint);

        result &= globalStore.getNodeStore().release(checkpoints.get(0));
        
        for ( int i = 0 ; i < nonDefaultStores.size(); i++ ) {
            result &= nonDefaultStores.get(i).getNodeStore().release(checkpoints.get(i + 1));
        }
        
        return result;
    }
    
    public Closeable addObserver(final Observer observer) {
        
        observer.contentChanged(getRoot(), null);
        
        observers.add(observer);
        
        return new Closeable() {
            @Override
            public void close() throws IOException {
                observers.remove(observer);
            }
        };
    }
    
    public static class Builder {
        
        private final MountInfoProvider mip;
        private final NodeStore globalStore;
        
        private final List<MountedNodeStore> nonDefaultStores = Lists.newArrayList();

        public Builder(MountInfoProvider mip, NodeStore globalStore) {
            this.mip = checkNotNull(mip, "mountInfoProvider");
            this.globalStore = checkNotNull(globalStore, "globalStore");
        }
        
        public Builder addMount(String mountName, NodeStore store) {
            
            checkNotNull(store, "store");
            checkNotNull(mountName, "mountName");

            Mount mount = checkNotNull(mip.getMountByName(mountName), "No mount with name %s found in %s", mountName, mip);
            
            nonDefaultStores.add(new MountedNodeStore(mount, store));
            
            return this;
        }
        
        public MultiplexingNodeStore build() {
            
            int buildMountCount = nonDefaultStores.size();
            int mipMountCount = mip.getNonDefaultMounts().size();
            checkArgument(buildMountCount == mipMountCount, 
                    "Inconsistent mount configuration. Builder received %s mounts, but MountInfoProvider knows about %s.",
                    buildMountCount, mipMountCount);
            
            return new MultiplexingNodeStore(mip, globalStore, nonDefaultStores);
        }
    }
}
