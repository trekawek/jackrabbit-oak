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
package org.apache.jackrabbit.oak.remote.common.persistence;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.oak.remote.proto.SegmentProtos;
import org.apache.jackrabbit.oak.remote.proto.SegmentServiceGrpc.SegmentServiceBlockingStub;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

public class TailingArchiveManager implements SegmentArchiveManager {

    private final SegmentArchiveManager delegate;

    private final TailingReader reader;

    public TailingArchiveManager(SegmentArchiveManager delegate, SegmentServiceBlockingStub segmentService, CloudBlobContainer container, List<String> directoryNames) throws IOException {
        this.delegate = delegate;
        this.reader = new TailingReader(container, directoryNames, segmentService);
    }

    @Override
    public @NotNull List<String> listArchives() {
        return Arrays.asList("data00000a.tar");
    }

    @Override
    @Nullable
    public SegmentArchiveReader open(@NotNull String archiveName) {
        return forceOpen(archiveName);
    }

    @Override
    @Nullable
    public SegmentArchiveReader forceOpen(String archiveName) {
        if ("data00000a.tar".equals(archiveName)) {
            return reader;
        } else {
            return null;
        }
    }

    @Override
    public @NotNull SegmentArchiveWriter create(@NotNull String archiveName) {
        throw new UnsupportedOperationException();
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
    public void copyFile(@NotNull String from, @NotNull String to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(@NotNull String archiveName) {
        return delegate.exists(archiveName);
    }

    @Override
    public void recoverEntries(@NotNull String archiveName, @NotNull LinkedHashMap<UUID, byte[]> entries) {
        throw new UnsupportedOperationException();
    }

    public void onNewSegment(SegmentProtos.SegmentBlob segmentBlob) {
        reader.onNewSegment(segmentBlob);
    }
}
