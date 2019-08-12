package org.apache.jackrabbit.oak.remote.client;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class RemoteNodeState implements NodeState {

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        return false;
    }

    @Override
    public @Nullable PropertyState getProperty(@NotNull String name) {
        return null;
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        return false;
    }

    @Override
    public long getLong(String name) {
        return 0;
    }

    @Override
    public @Nullable String getString(String name) {
        return null;
    }

    @Override
    public @NotNull Iterable<String> getStrings(@NotNull String name) {
        return null;
    }

    @Override
    public @Nullable String getName(@NotNull String name) {
        return null;
    }

    @Override
    public @NotNull Iterable<String> getNames(@NotNull String name) {
        return null;
    }

    @Override
    public long getPropertyCount() {
        return 0;
    }

    @Override
    public @NotNull Iterable<? extends PropertyState> getProperties() {
        return null;
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return false;
    }

    @Override
    public @NotNull NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        return null;
    }

    @Override
    public long getChildNodeCount(long max) {
        return 0;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return null;
    }

    @Override
    public @NotNull Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return null;
    }

    @Override
    public @NotNull NodeBuilder builder() {
        return null;
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        return false;
    }
}
