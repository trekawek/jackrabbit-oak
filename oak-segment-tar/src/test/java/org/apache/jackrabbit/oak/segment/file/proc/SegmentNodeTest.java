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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.Optional;

import org.apache.commons.io.input.NullInputStream;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class SegmentNodeTest {

    @Test
    public void shouldHaveGenerationProperty() {
        Segment segment = mock(Segment.class);
        when(segment.getGeneration()).thenReturn(1);
        when(segment.getInfo()).thenReturn(Optional.empty());

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("generation");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).intValue());
    }

    @Test
    public void shouldHaveFullGenerationProperty() {
        Segment segment = mock(Segment.class);
        when(segment.getFullGeneration()).thenReturn(1);
        when(segment.getInfo()).thenReturn(Optional.empty());

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("fullGeneration");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).intValue());
    }

    @Test
    public void shouldHaveCompactedProperty() {
        Segment segment = mock(Segment.class);
        when(segment.isCompacted()).thenReturn(true);
        when(segment.getInfo()).thenReturn(Optional.empty());

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("compacted");

        assertEquals(Type.BOOLEAN, property.getType());
        assertTrue(property.getValue(Type.BOOLEAN));
    }

    @Test
    public void shouldHaveLengthProperty() {
        Segment segment = mock(Segment.class);
        when(segment.getLength()).thenReturn(1);
        when(segment.getInfo()).thenReturn(Optional.empty());

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("length");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).intValue());
    }

    @Test
    public void shouldHaveDataProperty() {
        InputStream stream = new NullInputStream(1);

        Segment segment = mock(Segment.class);
        when(segment.getLength()).thenReturn(1);
        when(segment.getInfo()).thenReturn(Optional.empty());

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));
        when(backend.getSegmentData("s")).thenReturn(Optional.of(stream));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("data");

        assertEquals(Type.BINARY, property.getType());
        assertSame(stream, property.getValue(Type.BINARY).getNewStream());
        assertEquals(1, property.getValue(Type.BINARY).length());
    }

    @Test
    public void shouldHaveIdProperty() {
        Segment segment = mock(Segment.class);
        when(segment.getLength()).thenReturn(1);
        when(segment.getInfo()).thenReturn(Optional.empty());

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("id");

        assertEquals(Type.STRING, property.getType());
        assertEquals("s", property.getValue(Type.STRING));
    }

    @Test
    public void shouldHaveVersionProperty() {
        Segment segment = mock(Segment.class);
        when(segment.getVersion()).thenReturn(1);
        when(segment.getInfo()).thenReturn(Optional.empty());

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("version");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).longValue());
    }

    @Test
    public void shouldHaveIsDataSegmentProperty() {
        Segment segment = mock(Segment.class);
        when(segment.isDataSegment()).thenReturn(true);
        when(segment.getInfo()).thenReturn(Optional.empty());

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("isDataSegment");

        assertEquals(Type.BOOLEAN, property.getType());
        assertTrue(property.getValue(Type.BOOLEAN));
    }

    @Test
    public void shouldHaveInfoProperty() {
        Segment segment = mock(Segment.class);
        when(segment.getInfo()).thenReturn(Optional.of("info"));

        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        PropertyState property = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getProperty("info");

        assertEquals(Type.STRING, property.getType());
        assertEquals("info", property.getValue(Type.STRING));
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
            .builder();
    }

    @Test
    public void shouldExposeReferences() {
        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);

        NodeState segment = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s");

        assertTrue(segment.hasChildNode("references"));
        assertNotNull(segment.getChildNode("references"));
        assertTrue(segment.getChildNode("references").exists());
    }

    @Test
    public void shouldExposeRecordsNode() {
        Backend backend = mock(Backend.class);
        when(backend.tarExists("t")).thenReturn(true);
        when(backend.segmentExists("t", "s")).thenReturn(true);

        NodeState n = Proc.of(backend)
            .getChildNode("store")
            .getChildNode("t")
            .getChildNode("s")
            .getChildNode("records");

        assertTrue(n.exists());
    }

    @Test
    public void shouldExist() {
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

}
