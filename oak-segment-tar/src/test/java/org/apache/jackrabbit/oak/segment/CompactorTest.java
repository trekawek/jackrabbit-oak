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
package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.segment.SegmentWriterBuilder.segmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

public class CompactorTest {

    private MemoryStore memoryStore;

    @Before
    public void openSegmentStore() throws IOException {
        memoryStore = new MemoryStore();
    }

    @Test
    public void testCompactor() throws Exception {
        NodeStore store = SegmentNodeStoreBuilders.builder(memoryStore).build();
        init(store);

        SegmentWriter writer = segmentWriterBuilder("c").withGeneration(1).build(memoryStore);
        Compactor compactor = new Compactor(memoryStore.getReader(), writer,
                memoryStore.getBlobStore(), Suppliers.ofInstance(false), defaultGCOptions());
        addTestContent(store, 0);

        NodeState initial = store.getRoot();
        SegmentNodeState after = compactor.compact(initial, store.getRoot(),
                initial);
        assertEquals(store.getRoot(), after);

        addTestContent(store, 1);
        after = compactor.compact(initial, store.getRoot(), initial);
        assertEquals(store.getRoot(), after);
    }

    @Test
    public void testCancel() throws Throwable {

        // Create a Compactor that will cancel itself as soon as possible. The
        // early cancellation is the reason why the returned SegmentNodeState
        // doesn't have the child named "b".

        NodeStore store = SegmentNodeStoreBuilders.builder(memoryStore).build();
        SegmentWriter writer = segmentWriterBuilder("c").withGeneration(1).build(memoryStore);
        Compactor compactor = new Compactor(memoryStore.getReader(), writer,
                memoryStore.getBlobStore(), Suppliers.ofInstance(true), defaultGCOptions());
        SegmentNodeState sns = compactor.compact(store.getRoot(),
                addChild(store.getRoot(), "b"), store.getRoot());
        assertFalse(sns.hasChildNode("b"));
    }

    private NodeState addChild(NodeState current, String name) {
        NodeBuilder builder = current.builder();
        builder.child(name);
        return builder.getNodeState();
    }

    private static void init(NodeStore store) {
        new Oak(store).with(new OpenSecurityProvider())
                .createContentRepository();
    }

    private static void addTestContent(NodeStore store, int index)
            throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("test" + index);
        builder.child("child" + index);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}
