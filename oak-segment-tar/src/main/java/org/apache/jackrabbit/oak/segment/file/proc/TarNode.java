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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class TarNode extends AbstractNodeState {

    private final SegmentArchiveManager manager;

    private final String name;

    TarNode(SegmentArchiveManager manager, String name) {
        this.manager = manager;
        this.name = name;
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
        try (SegmentArchiveReader reader = manager.open(this.name)) {
            return Optional.ofNullable(reader)
                .map(SegmentArchiveReader::listSegments)
                .map(e -> e.stream().anyMatch(matchingName(name)))
                .orElse(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        try (SegmentArchiveReader reader = manager.open(this.name)) {
            return Optional.ofNullable(reader)
                .map(SegmentArchiveReader::listSegments)
                .map(toChildNode(name))
                .orElse(EmptyNodeState.MISSING_NODE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Function<List<SegmentArchiveEntry>, NodeState> toChildNode(String name) {
        return entries -> entries.stream()
            .filter(matchingName(name))
            .map(this::newTarNode)
            .findFirst()
            .orElse(EmptyNodeState.MISSING_NODE);
    }

    private static String name(SegmentArchiveEntry e) {
        return new UUID(e.getMsb(), e.getLsb()).toString();
    }

    private static Predicate<SegmentArchiveEntry> matchingName(String name) {
        return e -> name(e).equals(name);
    }

    private NodeState newTarNode(SegmentArchiveEntry e) {
        return new TarEntryNode(manager, name, e.getMsb(), e.getLsb());
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        try (SegmentArchiveReader reader = manager.open(this.name)) {
            return Optional.ofNullable(reader)
                .map(SegmentArchiveReader::listSegments)
                .map(this::toChildNodeEntries)
                .orElse(Collections.emptyList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterable<ChildNodeEntry> toChildNodeEntries(List<SegmentArchiveEntry> entries) {
        return () -> entries.stream().map(this::toChildNodeEntry).iterator();
    }

    private ChildNodeEntry toChildNodeEntry(SegmentArchiveEntry entry) {
        return new MemoryChildNodeEntry(name(entry), newTarNode(entry));
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        throw new UnsupportedOperationException();
    }

}
