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

import static java.util.Collections.emptySet;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.io.InputStream;
import java.util.Arrays;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class SegmentNode extends AbstractNodeState {

    private final Proc.Backend backend;

    private final String segmentId;

    SegmentNode(Proc.Backend backend, String segmentId) {
        this.backend = backend;
        this.segmentId = segmentId;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return backend.getSegment(segmentId).map(this::getProperties).orElse(emptySet());
    }

    private Iterable<PropertyState> getProperties(Segment segment) {
        return Arrays.asList(
            createProperty("generation", (long) segment.getGeneration(), Type.LONG),
            createProperty("fullGeneration", (long) segment.getFullGeneration(), Type.LONG),
            createProperty("compacted", segment.isCompacted(), Type.BOOLEAN),
            createProperty("length", (long) segment.getLength(), Type.LONG),
            createProperty("data", newBlob(), Type.BINARY),
            createProperty("id", segmentId, Type.STRING),
            createProperty("version", (long) segment.getVersion(), Type.LONG),
            createProperty("isDataSegment", segment.isDataSegment(), Type.BOOLEAN),
            createProperty("info", segment.getInfo().orElse(""), Type.STRING)
        );
    }

    private Blob newBlob() {
        return new Blob() {

            @Nonnull
            @Override
            public InputStream getNewStream() {
                return backend.getSegmentData(segmentId)
                    .orElseThrow(() -> new IllegalStateException("segment not found"));
            }

            @Override
            public long length() {
                return backend.getSegment(segmentId)
                    .map(Segment::getLength)
                    .orElseThrow(() -> new IllegalStateException("segment not found"));
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

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return NodeUtils.hasChildNode(getChildNodeEntries(), name);
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        return NodeUtils.getChildNode(getChildNodeEntries(), name);
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return Arrays.asList(
            new MemoryChildNodeEntry("references", new SegmentReferencesNode(backend, segmentId)),
            new MemoryChildNodeEntry("records", new SegmentRecordsNode(backend, segmentId))
        );
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        throw new UnsupportedOperationException();
    }

}
