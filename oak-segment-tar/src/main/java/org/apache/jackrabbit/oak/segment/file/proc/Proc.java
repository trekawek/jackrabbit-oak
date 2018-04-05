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

import static java.util.Objects.requireNonNull;

import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class Proc {

    private SegmentNodeStorePersistence persistence;

    private SegmentReader segmentReader;

    private SegmentIdProvider segmentIdProvider;

    public static Proc builder() {
        return new Proc();
    }

    public Proc withPersistence(SegmentNodeStorePersistence persistence) {
        this.persistence = requireNonNull(persistence);
        return this;
    }

    public Proc withSegmentReader(SegmentReader reader) {
        this.segmentReader = requireNonNull(reader);
        return this;
    }

    public Proc withSegmentIdProvider(SegmentIdProvider segmentIdProvider) {
        this.segmentIdProvider = requireNonNull(segmentIdProvider);
        return this;
    }

    public NodeState build() throws IOException {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setChildNode("store", new StoreNode(newSegmentArchiveManager(persistence)));
        builder.setChildNode("journal", new JournalNode(persistence.getJournalFile(), segmentReader, segmentIdProvider));
        return builder.getNodeState();
    }

    private static SegmentArchiveManager newSegmentArchiveManager(SegmentNodeStorePersistence persistence) throws IOException {
        return persistence.createArchiveManager(true, new IOMonitorAdapter(), new FileStoreMonitorAdapter());
    }

}
