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

package org.apache.jackrabbit.oak.segment.file.proc;

import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class TarEntryNode extends AbstractNodeState {

    private final SegmentArchiveManager manager;

    private final String name;

    private final long msb;

    private final long lsb;

    TarEntryNode(SegmentArchiveManager manager, String name, long msb, long lsb) {
        this.manager = manager;
        this.name = name;
        this.msb = msb;
        this.lsb = lsb;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        try (SegmentArchiveReader reader = manager.open(name)) {
            SegmentArchiveEntry entry = readEntry(reader);
            if (entry == null) {
                return Collections.emptyList();
            }
            return Arrays.asList(
                createProperty("generation", entry.getGeneration(), Type.LONG),
                createProperty("fullGeneration", entry.getFullGeneration(), Type.LONG),
                createProperty("compacted", entry.isCompacted(), Type.BOOLEAN),
                createProperty("length", entry.getLength(), Type.LONG),
                createProperty("lsb", lsb, Type.LONG),
                createProperty("msb", msb, Type.LONG),
                createProperty("data", newBlob(msb, lsb), Type.BINARY)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Blob newBlob(long msb, long lsb) {
        return new Blob() {

            @Nonnull
            @Override
            public InputStream getNewStream() {
                try (SegmentArchiveReader reader = manager.open(name)) {
                    if (reader == null) {
                        throw new IllegalStateException("reader not found");
                    }
                    ByteBuffer buffer = reader.readSegment(msb, lsb);
                    if (buffer == null) {
                        throw new IllegalStateException("segment not found");
                    }
                    byte[] destination = new byte[buffer.remaining()];
                    buffer.get(destination);
                    return new ByteArrayInputStream(destination);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public long length() {
                try (SegmentArchiveReader reader = manager.open(name)) {
                    SegmentArchiveEntry entry = readEntry(reader);
                    if (entry == null) {
                        return -1;
                    }
                    return entry.getLength();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @CheckForNull
            @Override
            public String getReference() {
                return null;
            }

            @CheckForNull
            @Override
            public String getContentIdentity() {
                return null;
            }

        };
    }

    private SegmentArchiveEntry readEntry(SegmentArchiveReader reader) {
        if (reader == null) {
            return null;
        }
        for (SegmentArchiveEntry e : reader.listSegments()) {
            if (e.getMsb() == msb && e.getLsb() == lsb) {
                return e;
            }
        }
        return null;
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return false;
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        return EmptyNodeState.MISSING_NODE;
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return emptyList();
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        throw new UnsupportedOperationException();
    }

}
