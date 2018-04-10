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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class ReferencesNodeTest {

    @Test
    public void shouldExposeReference() {
        List<String> references = Collections.singletonList("u");

        Segment segment = mock(Segment.class);
        when(segment.getInfo()).thenReturn(Optional.empty());

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegmentReferences("s")).thenReturn(Optional.of(references));
        when(backend.getSegment("u")).thenReturn(Optional.of(segment));

        NodeState r = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getChildNode("references")
            .getChildNode("0");

        assertEquals("u", r.getProperty("id").getValue(Type.STRING));
    }

    @Test
    public void shouldExposeAllReferences() {
        List<String> references = Arrays.asList(
            "u", "v", "w"
        );

        Segment segment = mock(Segment.class);
        when(segment.getInfo()).thenReturn(Optional.empty());

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegmentReferences("s")).thenReturn(Optional.of(references));
        when(backend.getSegment(any())).thenReturn(Optional.of(segment));

        NodeState rr = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getChildNode("references");

        for (int i = 0; i < references.size(); i++) {
            NodeState r = rr.getChildNode(Integer.toString(i));
            assertEquals(references.get(i), r.getProperty("id").getValue(Type.STRING));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotBeBuildable() {
        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);

        Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getChildNode("references")
            .builder();
    }

}
