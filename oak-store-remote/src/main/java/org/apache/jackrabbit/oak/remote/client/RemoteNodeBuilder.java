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
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.remote.common.PropertySerializer;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderValue;
import org.apache.jackrabbit.oak.remote.proto.NodeDiffProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeDiffProtos.NodeDiff;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos.NodeValue;
import org.apache.jackrabbit.oak.remote.server.RemoteNodeStoreException;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.function.Predicate;

public class RemoteNodeBuilder implements NodeBuilder  {

    private RemoteNodeStoreContext context;

    private final NodeBuilderProtos.NodeBuilderId builderId;

    private final String path;

    private NodeBuilderValue nodeBuilderValue;

    public RemoteNodeBuilder(RemoteNodeStoreContext context, NodeBuilderProtos.NodeBuilderId builderId) {
        this.context = context;
        this.builderId = builderId;
        this.path = "/";
    }

    private RemoteNodeBuilder(RemoteNodeBuilder parentBuilder, String name) {
        this.context = parentBuilder.context;
        this.builderId = parentBuilder.builderId;
        this.path = PathUtils.concat(parentBuilder.path, name);
    }

    public NodeBuilderProtos.NodeBuilderId getNodeBuilderId() {
        return builderId;
    }

    private NodeBuilderProtos.NodeBuilderPath getNodeBuilderPath() {
        return NodeBuilderProtos.NodeBuilderPath.newBuilder()
                .setPath(path)
                .setNodeBuilderId(builderId)
                .build();
    }

    private NodeBuilderValue getNodeBuilderValue() {
        if (getChangeQueue().flush()) {
            nodeBuilderValue = null;
        }
        if (nodeBuilderValue == null) {
            nodeBuilderValue = context.getClient().getNodeBuilderService().getNodeValue(getNodeBuilderPath());
        }
        return nodeBuilderValue;
    }

    private NodeValue getNodeValue() {
        return getNodeBuilderValue().getNodeValue();
    }

    private NodeBuilderChangeQueue getChangeQueue() {
        return context.getNodeBuilderChangeQueue(builderId);
    }

    void flush() {
        getChangeQueue().flush();
    }

    @Override
    public @NotNull NodeState getNodeState() {
        NodeStateProtos.NodeStateId id = context.getClient().getNodeBuilderService().createNodeState(getNodeBuilderPath());
        context.addNodeStateId(id);
        return new RemoteNodeState(context, id);
    }

    @Override
    public @NotNull NodeState getBaseState() {
        NodeStateProtos.NodeStateId id = context.getClient().getNodeBuilderService().createBaseNodeState(getNodeBuilderPath());
        context.addNodeStateId(id);
        return new RemoteNodeState(context, id);
    }

    @Override
    public boolean exists() {
        return getNodeValue().getExists();
    }

    @Override
    public boolean isNew() {
        return getNodeBuilderValue().getIsNew();
    }

    @Override
    public boolean isModified() {
        return getNodeBuilderValue().getIsModified();
    }

    @Override
    public boolean isReplaced() {
        return getNodeBuilderValue().getIsReplaced();
    }

    @Override
    public boolean isNew(String name) {
        return hasProperty(name) && !getBaseState().hasProperty(name);
    }

    @Override
    public boolean isReplaced(String name) {
        PropertyState baseProperty = getBaseState().getProperty(name);
        if (baseProperty != null) {
            PropertyState currentProperty = getProperty(name);
            if (currentProperty == null || !currentProperty.equals(baseProperty)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long getChildNodeCount(long max) {
        return Math.min(getNodeValue().getChildNameCount(), max);
    }

    @Override
    public @NotNull Iterable<String> getChildNodeNames() {
        return getNodeValue().getChildNameList();
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return getNodeValue().getChildNameList().contains(name);
    }

    @Override
    public @NotNull NodeBuilder getChildNode(@NotNull String name) throws IllegalArgumentException {
        return new RemoteNodeBuilder(this, name);
    }

    @Override
    public long getPropertyCount() {
        return getNodeValue().getPropertyCount();
    }

    @Override
    public @NotNull Iterable<? extends PropertyState> getProperties() {
        return Iterables.transform(getNodeValue().getPropertyList(), context.getPropertyDeserializer()::unsafeToOakProperty);
    }

    @Override
    public boolean hasProperty(String name) {
        return getNodeValue().getPropertyList().stream()
                .map(NodeValueProtos.Property::getName)
                .anyMatch(Predicate.isEqual(name));
    }

    @Override
    public @Nullable PropertyState getProperty(String name) {
        return getNodeValue().getPropertyList().stream()
                .filter(p -> name.equals(p.getName()))
                .findFirst()
                .map(context.getPropertyDeserializer()::unsafeToOakProperty)
                .orElse(null);
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        PropertyState property = getProperty(name);
        return property != null
                && property.getType() == Type.BOOLEAN
                && property.getValue(Type.BOOLEAN);
    }

    @Override
    public @Nullable String getString(String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.STRING) {
            return property.getValue(Type.STRING);
        } else {
            return null;
        }
    }

    @Override
    public @Nullable String getName(@NotNull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.NAME) {
            return property.getValue(Type.NAME);
        } else {
            return null;
        }
    }

    @Override
    public @NotNull Iterable<String> getNames(@NotNull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.NAMES) {
            return property.getValue(Type.NAMES);
        } else {
            return Collections.emptyList();
        }
    }

    // muting methods

    @Override
    public @NotNull NodeBuilder child(@NotNull String name) throws IllegalArgumentException {
        NodeDiff.Builder diffBuilder = NodeDiff.newBuilder();
        diffBuilder.getAddNodeBuilder().setChildName(name);
        getChangeQueue().add(diffBuilder.build());
        return new RemoteNodeBuilder(this, name);
    }

    @Override
    public @NotNull NodeBuilder setChildNode(@NotNull String name) throws IllegalArgumentException {
        NodeDiff.Builder diffBuilder = NodeDiff.newBuilder();
        diffBuilder.getSetChildNodeBuilder().setChildName(name);
        getChangeQueue().add(diffBuilder.build());
        return new RemoteNodeBuilder(this, name);
    }

    @Override
    public @NotNull NodeBuilder setChildNode(@NotNull String name, @NotNull NodeState nodeState) throws IllegalArgumentException {
        if (!(nodeState instanceof RemoteNodeState)) {
            throw new IllegalArgumentException("The node state " + nodeState + " doesn't come from this node store");
        }

        RemoteNodeState remoteNodeState = (RemoteNodeState) nodeState;
        NodeDiff.Builder diffBuilder = NodeDiff.newBuilder();
        diffBuilder.getSetChildNodeBuilder()
                .setChildName(name)
                .getNodeStatePathBuilder()
                    .setPath(remoteNodeState.getPath())
                    .setNodeStateId(remoteNodeState.getNodeStateId());

        getChangeQueue().add(diffBuilder.build());
        return new RemoteNodeBuilder(this, name);
    }

    @Override
    public boolean moveTo(@NotNull NodeBuilder newParent, @NotNull String newName) throws IllegalArgumentException {
        if (!(newParent instanceof RemoteNodeBuilder)) {
            throw new IllegalArgumentException("The node builder " + newParent + " doesn't come from this node store");
        }
        RemoteNodeBuilder remoteNewParent = ((RemoteNodeBuilder) newParent);
        if (newParent.hasChildNode(newName)) {
            return false;
        }
        if ((remoteNewParent.getNodeBuilderId().equals(this.getNodeBuilderId()) && PathUtils.isAncestor(this.path, remoteNewParent.path))) {
            return false;
        }
        NodeDiff.Builder diffBuilder = NodeDiff.newBuilder();
        diffBuilder.getMoveBuilder()
                .setChildName(newName)
                .setNewParent(remoteNewParent.getNodeBuilderPath());
        getChangeQueue().add(diffBuilder.build());
        return true;
    }

    @Override
    public @NotNull NodeBuilder setProperty(@NotNull PropertyState property) throws IllegalArgumentException {
        NodeDiff.Builder diffBuilder = NodeDiff.newBuilder();
        try {
            diffBuilder.getSetPropertyBuilder().setProperty(PropertySerializer.toProtoProperty(property));
        } catch (RemoteNodeStoreException e) {
            throw new IllegalArgumentException(e);
        }
        getChangeQueue().add(diffBuilder.build());
        return this;
    }

    @Override
    public @NotNull <T> NodeBuilder setProperty(String name, @NotNull T value) throws IllegalArgumentException {
        return setProperty(PropertyStates.createProperty(name, value));
    }

    @Override
    public @NotNull <T> NodeBuilder setProperty(String name, @NotNull T value, Type<T> type) throws IllegalArgumentException {
        return setProperty(PropertyStates.createProperty(name, value, type));
    }

    @Override
    public @NotNull NodeBuilder removeProperty(String name) {
        NodeDiff.Builder diffBuilder = NodeDiff.newBuilder();
        diffBuilder.getRemovePropertyBuilder().setName(name);
        getChangeQueue().add(diffBuilder.build());
        return this;
    }

    @Override
    public boolean remove() {
        if (!exists()) {
            return false;
        }
        NodeDiff.Builder diffBuilder = NodeDiff.newBuilder();
        diffBuilder.setRemoveNode(NodeDiffProtos.RemoveNode.getDefaultInstance());
        getChangeQueue().add(diffBuilder.build());
        return true;
    }

    // other methods

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        String blobId = context.getBlobStore().writeBlob(stream);
        return new BlobStoreBlob(context.getBlobStore(), blobId);
    }
}
