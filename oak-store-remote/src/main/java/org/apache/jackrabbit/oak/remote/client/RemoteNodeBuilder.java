package org.apache.jackrabbit.oak.remote.client;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;

public class RemoveNodeBuilder implements NodeBuilder  {
    @Override
    public @NotNull NodeState getNodeState() {
        return null;
    }

    @Override
    public @NotNull NodeState getBaseState() {
        return null;
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public boolean isNew() {
        return false;
    }

    @Override
    public boolean isNew(String name) {
        return false;
    }

    @Override
    public boolean isModified() {
        return false;
    }

    @Override
    public boolean isReplaced() {
        return false;
    }

    @Override
    public boolean isReplaced(String name) {
        return false;
    }

    @Override
    public long getChildNodeCount(long max) {
        return 0;
    }

    @Override
    public @NotNull Iterable<String> getChildNodeNames() {
        return null;
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return false;
    }

    @Override
    public @NotNull NodeBuilder child(@NotNull String name) throws IllegalArgumentException {
        return null;
    }

    @Override
    public @NotNull NodeBuilder getChildNode(@NotNull String name) throws IllegalArgumentException {
        return null;
    }

    @Override
    public @NotNull NodeBuilder setChildNode(@NotNull String name) throws IllegalArgumentException {
        return null;
    }

    @Override
    public @NotNull NodeBuilder setChildNode(@NotNull String name, @NotNull NodeState nodeState) throws IllegalArgumentException {
        return null;
    }

    @Override
    public boolean remove() {
        return false;
    }

    @Override
    public boolean moveTo(@NotNull NodeBuilder newParent, @NotNull String newName) throws IllegalArgumentException {
        return false;
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
    public boolean hasProperty(String name) {
        return false;
    }

    @Override
    public @Nullable PropertyState getProperty(String name) {
        return null;
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        return false;
    }

    @Override
    public @Nullable String getString(String name) {
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
    public @NotNull NodeBuilder setProperty(@NotNull PropertyState property) throws IllegalArgumentException {
        return null;
    }

    @Override
    public @NotNull <T> NodeBuilder setProperty(String name, @NotNull T value) throws IllegalArgumentException {
        return null;
    }

    @Override
    public @NotNull <T> NodeBuilder setProperty(String name, @NotNull T value, Type<T> type) throws IllegalArgumentException {
        return null;
    }

    @Override
    public @NotNull NodeBuilder removeProperty(String name) {
        return null;
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return null;
    }
}
