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
package org.apache.jackrabbit.oak.remote.client.persistence;

import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

public class TailingArchiveManager implements SegmentArchiveManager {

    private final SegmentArchiveManager delegate;

    private final CloudBlobDirectory directory;

    private final String lastArchive;

    public TailingArchiveManager(SegmentArchiveManager delegate, CloudBlobDirectory directory) {
        this.delegate = delegate;
        this.directory = directory;
        try {
            List<String> archives = listArchives();
            if (archives.isEmpty()) {
                throw new IllegalStateException("The archive list is empty, can't tail the last one");
            }
            Collections.sort(archives);
            lastArchive = archives.get(archives.size() - 1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public @NotNull List<String> listArchives() throws IOException {
        return delegate.listArchives();
    }

    @Override
    public @Nullable SegmentArchiveReader open(@NotNull String archiveName) throws IOException {
        if (archiveName.equals(lastArchive)) {
            return new ArchiveTailingReader(delegate, archiveName, directory);
        } else {
            return delegate.open(archiveName);
        }
    }

    @Override
    public @Nullable SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        if (archiveName.equals(lastArchive)) {
            return new ArchiveTailingReader(delegate, archiveName, directory);
        } else {
            return delegate.forceOpen(archiveName);
        }
    }

    @Override
    public @NotNull SegmentArchiveWriter create(@NotNull String archiveName) throws IOException {
        return delegate.create(archiveName);
    }

    @Override
    public boolean delete(@NotNull String archiveName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean renameTo(@NotNull String from, @NotNull String to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyFile(@NotNull String from, @NotNull String to) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(@NotNull String archiveName) {
        return delegate.exists(archiveName);
    }

    @Override
    public void recoverEntries(@NotNull String archiveName, @NotNull LinkedHashMap<UUID, byte[]> entries) throws IOException {
        throw new UnsupportedOperationException();
    }
}
