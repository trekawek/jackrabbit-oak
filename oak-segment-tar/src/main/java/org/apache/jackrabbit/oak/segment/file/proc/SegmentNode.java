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

import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.io.InputStream;
import java.util.Arrays;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;

class SegmentNode extends AbstractNode {

    private final Proc.Backend backend;

    private final String segmentId;

    SegmentNode(Proc.Backend backend, String segmentId) {
        this.backend = backend;
        this.segmentId = segmentId;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return backend.getSegment(segmentId)
                .map(this::getProperties)
                .orElse(ImmutableList.of(
                        newIdProperty(segmentId),
                        newExistsProperty(false)));
    }

    private Iterable<PropertyState> getProperties(Segment segment) {
        return Arrays.asList(
            createProperty("generation", (long) segment.getGeneration(), Type.LONG),
            createProperty("fullGeneration", (long) segment.getFullGeneration(), Type.LONG),
            createProperty("compacted", segment.isCompacted(), Type.BOOLEAN),
            createProperty("length", (long) segment.getLength(), Type.LONG),
            createProperty("data", newBlob(), Type.BINARY),
            newIdProperty(segmentId),
            createProperty("version", (long) segment.getVersion(), Type.LONG),
            createProperty("isDataSegment", segment.isDataSegment(), Type.BOOLEAN),
            createProperty("info", segment.getInfo().orElse(""), Type.STRING),
            newExistsProperty(true)
        );
    }

    private static PropertyState newIdProperty(String segmentId) {
        return createProperty("id", segmentId, Type.STRING);
    }

    private static PropertyState newExistsProperty(boolean exists) {
        return createProperty("exists", exists, Type.BOOLEAN);
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

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return Arrays.asList(
            new MemoryChildNodeEntry("references", new ReferencesNode(backend, segmentId)),
            new MemoryChildNodeEntry("records", new RecordsNode(backend, segmentId))
        );
    }

}
