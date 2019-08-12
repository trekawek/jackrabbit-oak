package org.apache.jackrabbit.oak.remote.client;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class RemoteNodeStore implements NodeStore {

    @Override
    public @NotNull NodeState getRoot() {
        return null;
    }

    @Override
    public @NotNull NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook, @NotNull CommitInfo info) throws CommitFailedException {
        return null;
    }

    @Override
    public @NotNull NodeState rebase(@NotNull NodeBuilder builder) {
        return null;
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        return null;
    }

    @Override
    public @NotNull Blob createBlob(InputStream inputStream) throws IOException {
        return null;
    }

    @Override
    public @Nullable Blob getBlob(@NotNull String reference) {
        return null;
    }

    @Override
    public @NotNull String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
        return null;
    }

    @Override
    public @NotNull String checkpoint(long lifetime) {
        return null;
    }

    @Override
    public @NotNull Map<String, String> checkpointInfo(@NotNull String checkpoint) {
        return null;
    }

    @Override
    public @NotNull Iterable<String> checkpoints() {
        return null;
    }

    @Override
    public @Nullable NodeState retrieve(@NotNull String checkpoint) {
        return null;
    }

    @Override
    public boolean release(@NotNull String checkpoint) {
        return false;
    }
}
