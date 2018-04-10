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

import java.util.Optional;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class CommitNodeTest {

    @Test
    public void shouldExist() {
        Proc.Backend backend = mock(Proc.Backend.class);
        when(backend.commitExists("h")).thenReturn(true);

        assertTrue(
            Proc.of(backend)
                .getChildNode("journal")
                .getChildNode("h")
                .exists()
        );
    }

    @Test
    public void shouldHaveTimestampProperty() {
        Proc.Backend.Commit commit = mock(Proc.Backend.Commit.class);
        when(commit.getTimestamp()).thenReturn(1L);
        when(commit.getRevision()).thenReturn("");

        Proc.Backend backend = mock(Proc.Backend.class);
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
    public void shouldExposeRoot() {
        Proc.Backend.Commit commit = mock(Proc.Backend.Commit.class);
        when(commit.getRoot()).thenReturn(Optional.of(EmptyNodeState.EMPTY_NODE));

        Proc.Backend backend = mock(Proc.Backend.class);
        when(backend.commitExists("h")).thenReturn(true);
        when(backend.getCommit("h")).thenReturn(Optional.of(commit));

        NodeState commitNode = Proc.of(backend)
            .getChildNode("journal")
            .getChildNode("h");

        assertTrue(commitNode.hasChildNode("root"));
        assertTrue(Sets.newHashSet(commitNode.getChildNodeNames()).contains("root"));
    }

    @Test
    public void shouldHaveRootChildNode() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("root", true);
        NodeState root = builder.getNodeState();

        Proc.Backend.Commit commit = mock(Proc.Backend.Commit.class);
        when(commit.getRoot()).thenReturn(Optional.of(root));

        Proc.Backend backend = mock(Proc.Backend.class);
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
    public void shouldNotBeBuildable() {
        Proc.Backend.Commit commit = mock(Proc.Backend.Commit.class);
        when(commit.getRoot()).thenReturn(Optional.of(EmptyNodeState.EMPTY_NODE));

        Proc.Backend backend = mock(Proc.Backend.class);
        when(backend.commitExists("h")).thenReturn(true);
        when(backend.getCommit("h")).thenReturn(Optional.of(commit));

        Proc.of(backend)
            .getChildNode("journal")
            .getChildNode("h")
            .builder();
    }

}
