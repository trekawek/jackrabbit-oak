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
 *
 */

package org.apache.jackrabbit.oak.segment.persistentcache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * michid document
 */
public class CachingSegmentArchiveReader implements SegmentArchiveReader {

    @NotNull
    private final DiskCache diskCache;

    @NotNull
    private final SegmentArchiveReader delegate;

    public CachingSegmentArchiveReader(
            @NotNull DiskCache diskCache,
            @NotNull SegmentArchiveReader delegate) {
        this.diskCache = diskCache;
        this.delegate = delegate;
    }

    @Override
    @Nullable
    public ByteBuffer readSegment(long msb, long lsb) throws IOException {
        ByteBuffer buffer = diskCache.readSegment(msb, lsb);
        if (buffer == null) {
            buffer = delegate.readSegment(msb, lsb);
            diskCache.writeSegment(msb, lsb, buffer);
        }
        return buffer;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        if (diskCache.containsSegment(msb, lsb)) {
            return true;
        } else {
            return delegate.containsSegment(msb, lsb);
        }
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        return delegate.listSegments();
    }

    @Override
    @Nullable
    public ByteBuffer getGraph() throws IOException {
        return delegate.getGraph();
    }

    @Override
    public boolean hasGraph() {
        return delegate.hasGraph();
    }

    @Override
    @NotNull
    public ByteBuffer getBinaryReferences() throws IOException {
        return delegate.getBinaryReferences();
    }

    @Override
    public long length() {
        return delegate.length();
    }

    @Override
    @NotNull
    public String getName() {
        return delegate.getName();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public int getEntrySize(int size) {
        return delegate.getEntrySize(size);
    }
}
