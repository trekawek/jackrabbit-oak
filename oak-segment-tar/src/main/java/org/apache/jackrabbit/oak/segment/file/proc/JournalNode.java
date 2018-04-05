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

import static java.lang.String.valueOf;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.io.IOException;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.file.JournalEntry;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class JournalNode extends AbstractNodeState {

    private final JournalFile journal;

    private final SegmentReader segmentReader;

    private final SegmentIdProvider segmentIdProvider;

    JournalNode(JournalFile journal, SegmentReader segmentReader, SegmentIdProvider segmentIdProvider) {
        this.journal = journal;
        this.segmentReader = segmentReader;
        this.segmentIdProvider = segmentIdProvider;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return emptyList();
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        try (JournalReader reader = new JournalReader(journal)) {
            return Iterators.any(reader, e -> name(e).equals(name));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private static String name(JournalEntry entry) {
        return valueOf(requireNonNull(entry).getTimestamp());
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        try (JournalReader reader = new JournalReader(journal)) {
            return Optional.ofNullable(Iterators.find(reader, e -> name(e).equals(name)))
                .map(this::newJournalEntryNode)
                .orElse(MISSING_NODE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private NodeState newJournalEntryNode(@Nonnull JournalEntry journalEntry) {
        return new JournalEntryNode(journalEntry, segmentReader, segmentIdProvider);
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return () -> {
            try (JournalReader reader = new JournalReader(journal)) {
                return Iterators.transform(reader, this::newChildNodeEntry);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Nonnull
    private ChildNodeEntry newChildNodeEntry(@Nonnull JournalEntry entry) {
        return new MemoryChildNodeEntry(name(entry), newJournalEntryNode(entry));
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        throw new UnsupportedOperationException();
    }

}
