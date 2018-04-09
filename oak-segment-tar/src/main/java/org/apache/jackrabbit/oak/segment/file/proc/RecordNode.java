/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.file.proc;

import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.util.Arrays;
import java.util.Collections;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Record;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class RecordNode extends AbstractNodeState {

    private final Record record;

    RecordNode(Record record) {
        this.record = record;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return Arrays.asList(
            createProperty("number", (long) record.getNumber(), Type.LONG),
            createProperty("offset", (long) record.getOffset(), Type.LONG),
            createProperty("type", record.getType(), Type.STRING)
        );
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
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        throw new UnsupportedOperationException();
    }

}
