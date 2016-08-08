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
package org.apache.jackrabbit.oak.plugins.document.mount;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MountedDocumentNodeState extends AbstractDocumentNodeState {

    private final NodeState nodeState;

    private final String path;

    private final RevisionVector lastRev;

    private final RevisionVector rootRev;

    private final boolean fromExternalChange;

    private final NodeStateDiffer differ;

    private MountedDocumentNodeState(NodeState nodeState, String path, RevisionVector lastRev, RevisionVector rootRev, boolean fromExternalChange, NodeStateDiffer differ) {
        this.nodeState = nodeState;
        this.path = path;
        this.lastRev = lastRev;
        this.rootRev = rootRev;
        this.fromExternalChange = fromExternalChange;
        this.differ = differ;
    }

    public static AbstractDocumentNodeState wrap(NodeState nodeState, String path, RevisionVector lastRev, RevisionVector rootRev, NodeStateDiffer differ) {
        return new MountedDocumentNodeState(nodeState, path, lastRev, rootRev, false, differ);
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public RevisionVector getLastRevision() {
        return lastRev;
    }

    @Override
    public RevisionVector getRootRevision() {
        return rootRev;
    }

    @Override
    public boolean isFromExternalChange() {
        return fromExternalChange;
    }

    @Override
    public AbstractDocumentNodeState withRootRevision(@Nonnull RevisionVector root, boolean externalChange) {
        return new MountedDocumentNodeState(nodeState, path, lastRev, root, externalChange, differ);
    }

    @Override
    public boolean hasNoChildren() {
        //Passing max as 1 so as to minimize any overhead.
        return nodeState.getChildNodeCount(1) == 0;
    }

    @Override
    protected NodeStateDiffer getNodeStateDiffer() {
        return differ;
    }

    @Override
    public boolean exists() {
        return nodeState.exists();
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return nodeState.getProperties();
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return nodeState.hasChildNode(name);
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        return decorateChild(nodeState.getChildNode(name), name);
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return Iterables.transform(nodeState.getChildNodeEntries(), new Function<ChildNodeEntry, ChildNodeEntry>() {
            @Nullable
            @Override
            public ChildNodeEntry apply(@Nullable ChildNodeEntry input) {
                return new MemoryChildNodeEntry(input.getName(), decorateChild(input.getNodeState(), input.getName()));
            }
        });
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    //Following method should be overridden as default implementation in AbstractNodeState
    //is not optimized

    @Override
    public PropertyState getProperty(@Nonnull String name) {
        return nodeState.getProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return nodeState.getPropertyCount();
    }

    @Override
    public boolean hasProperty(@Nonnull String name) {
        return nodeState.hasProperty(name);
    }

    @Override
    public boolean getBoolean(@Nonnull String name) {
        return nodeState.getBoolean(name);
    }

    @Override
    public long getLong(String name) {
        return nodeState.getLong(name);
    }

    @Override
    public String getString(String name) {
        return nodeState.getString(name);
    }

    @Nonnull
    @Override
    public Iterable<String> getStrings(@Nonnull String name) {
        return nodeState.getStrings(name);
    }

    @Override
    public String getName(@Nonnull String name) {
        return nodeState.getName(name);
    }

    @Nonnull
    @Override
    public Iterable<String> getNames(@Nonnull String name) {
        return nodeState.getNames(name);
    }

    @Override
    public long getChildNodeCount(long max) {
        return nodeState.getChildNodeCount(max);
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return nodeState.getChildNodeNames();
    }

    private NodeState decorateChild(NodeState child, String name) {
        String childPath = PathUtils.concat(path, name);
        return new MountedDocumentNodeState(child, childPath, lastRev, rootRev, fromExternalChange, differ);
    }
}
