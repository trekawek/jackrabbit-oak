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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.io.input.NullInputStream;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Commit;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class ProcTest {

    @Test
    public void procNodeShouldExposeStore() {
        assertTrue(
            Proc.of(mock(Backend.class))
                .hasChildNode("store")
        );
    }

    @Test
    public void storeNodeShouldExist() {
        assertTrue(
            Proc.of(mock(Backend.class))
                .getChildNode("store")
                .exists()
        );
    }

    @Test
    public void storeNodeShouldExposeAllTarNames() {
        Set<String> names = Sets.newHashSet("t1", "t2", "t3");
        Backend backend = mock(Backend.class);
        when(backend.getTarNames()).thenReturn(names);
        assertEquals(names, Sets.newHashSet(
            Proc.of(backend)
                .getChildNode("store")
                .getChildNodeNames()
        ));
    }

    @Test
    public void storeNodeShouldExposeTarName() {
        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        assertTrue(
            Proc.of(backend)
                .getChildNode("store")
                .hasChildNode("t")
        );
    }

    @Test
    public void tarNodeShouldExist() {
        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        assertTrue(
            Proc.of(backend)
                .getChildNode("store")
                .getChildNode("t")
                .exists()
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void storeNodeShouldNotBeBuildable() {
        Proc.of(mock(Backend.class))
            .getChildNode("store")
            .builder();
    }

    @Test
    public void tarNodeShouldExposeSegmentId() {
        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        assertTrue(
            Proc.of(backend)
                .getChildNode("store")
                .getChildNode("t")
                .hasChildNode("s")
        );
    }

    @Test
    public void tarNodeShouldExposeAllSegmentIds() {
        Set<String> names = Sets.newHashSet("s1", "s2", "s3");
        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.getSegmentIds("t")).thenReturn(names);
        assertEquals(names, Sets.newHashSet(
            Proc.of(backend)
                .getChildNode("store")
                .getChildNode("t")
                .getChildNodeNames()
        ));
    }

    @Test
    public void segmentNodeShouldExist() {
        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        assertTrue(
            Proc.of(backend)
                .getChildNode("store")
                .getChildNode("t")
                .getChildNode("s")
                .exists()
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void tarNodeShouldNotBeBuildable() {
        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .builder();
    }

    @Test
    public void segmentNodeShouldHaveGenerationProperty() {
        Segment segment = mock(Segment.class);
        when(segment.getGeneration()).thenReturn(1);

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("t", "s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("generation");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).intValue());
    }

    @Test
    public void segmentNodeShouldHaveFullGenerationProperty() {
        Segment segment = mock(Segment.class);
        when(segment.getFullGeneration()).thenReturn(1);

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("t", "s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("fullGeneration");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).intValue());
    }

    @Test
    public void segmentNodeShouldHaveCompactedProperty() {
        Segment segment = mock(Segment.class);
        when(segment.isCompacted()).thenReturn(true);

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("t", "s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("compacted");

        assertEquals(Type.BOOLEAN, property.getType());
        assertTrue(property.getValue(Type.BOOLEAN));
    }

    @Test
    public void segmentNodeShouldHaveLengthProperty() {
        Segment segment = mock(Segment.class);
        when(segment.getLength()).thenReturn(1);

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("t", "s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("length");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).intValue());
    }

    @Test
    public void segmentNodeShouldHaveDataProperty() {
        InputStream stream = new NullInputStream(1);

        Segment segment = mock(Segment.class);
        when(segment.getLength()).thenReturn(1);

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("t", "s")).thenReturn(Optional.of(segment));
        when(backend.getSegmentData("t", "s")).thenReturn(Optional.of(stream));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("data");

        assertEquals(Type.BINARY, property.getType());
        assertSame(stream, property.getValue(Type.BINARY).getNewStream());
        assertEquals(1, property.getValue(Type.BINARY).length());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void segmentNodeShouldNotBeBuildable() {
        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);

        Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .builder();
    }

    @Test
    public void procShouldExposeJournal() {
        assertTrue(
            Proc.of(mock(Backend.class))
                .hasChildNode("journal")
        );
    }

    @Test
    public void journalNodeShouldExposeCommitHandle() {
        Backend backend = mock(Backend.class);
        when(backend.commitExists("h")).thenReturn(true);

        assertTrue(
            Proc.of(backend)
                .getChildNode("journal")
                .hasChildNode("h")
        );
    }

    @Test
    public void journalNodeShouldExposeAllCommitHandles() {
        Set<String> names = Sets.newHashSet("h1", "h2", "h3");

        Backend backend = mock(Backend.class);
        when(backend.getCommitHandles()).thenReturn(names);

        assertEquals(names, Sets.newHashSet(
            Proc.of(backend)
                .getChildNode("journal")
                .getChildNodeNames()
        ));
    }

    @Test
    public void commitNodeShouldExist() {
        Backend backend = mock(Backend.class);
        when(backend.commitExists("h")).thenReturn(true);

        assertTrue(
            Proc.of(backend)
                .getChildNode("journal")
                .getChildNode("h")
                .exists()
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void journalNodeShouldNotBeBuildable() {
        Proc.of(mock(Backend.class))
            .getChildNode("journal")
            .builder();
    }

    @Test
    public void commitNodeShouldHaveTimestampProperty() {
        Commit commit = mock(Commit.class);
        when(commit.getTimestamp()).thenReturn(1L);

        Backend backend = mock(Backend.class);
        when(backend.commitExists("h")).thenReturn(true);
        when(backend.getCommit("h")).thenReturn(Optional.of(commit));

        PropertyState property = Proc.of(backend)
            .getChildNode("journal")
            .getChildNode("h")
            .getProperty("timestamp");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1L, property.getValue(Type.LONG).longValue());
    }

    @Test
    public void commitNodeShouldExposeRoot() {
        Commit commit = mock(Commit.class);
        when(commit.getRoot()).thenReturn(Optional.of(EmptyNodeState.EMPTY_NODE));

        Backend backend = mock(Backend.class);
        when(backend.commitExists("h")).thenReturn(true);
        when(backend.getCommit("h")).thenReturn(Optional.of(commit));

        NodeState commitNode = Proc.of(backend)
            .getChildNode("journal")
            .getChildNode("h");

        assertTrue(commitNode.hasChildNode("root"));
        assertTrue(Sets.newHashSet(commitNode.getChildNodeNames()).contains("root"));
    }

    @Test
    public void rootNodeShouldExist() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("root", true);
        NodeState root = builder.getNodeState();

        Commit commit = mock(Commit.class);
        when(commit.getRoot()).thenReturn(Optional.of(root));

        Backend backend = mock(Backend.class);
        when(backend.commitExists("h")).thenReturn(true);
        when(backend.getCommit("h")).thenReturn(Optional.of(commit));

        assertSame(root,
            Proc.of(backend)
                .getChildNode("journal")
                .getChildNode("h")
                .getChildNode("root")
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void commitNodeShouldNotBeBuildable() {
        Commit commit = mock(Commit.class);
        when(commit.getRoot()).thenReturn(Optional.of(EmptyNodeState.EMPTY_NODE));

        Backend backend = mock(Backend.class);
        when(backend.commitExists("h")).thenReturn(true);
        when(backend.getCommit("h")).thenReturn(Optional.of(commit));

        Proc.of(backend)
            .getChildNode("journal")
            .getChildNode("h")
            .builder();
    }

}
