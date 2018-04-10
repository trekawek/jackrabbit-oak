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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Record;
import org.junit.Test;

public class RecordNodeTest {

    @Test
    public void shouldExist() {
        assertTrue(new RecordNode(mock(Record.class)).exists());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotBeBuildable() {
        new RecordNode(mock(Record.class)).builder();
    }

    @Test
    public void shouldHaveNumberProperty() {
        Record record = mock(Record.class);
        when(record.getNumber()).thenReturn(1);
        when(record.getType()).thenReturn("t");
        when(record.getSegmentId()).thenReturn("s");

        PropertyState p = new RecordNode(record).getProperty("number");

        assertEquals(Type.LONG, p.getType());
        assertEquals(1, p.getValue(Type.LONG).intValue());
    }

    @Test
    public void shouldHaveOffsetProperty() {
        Record record = mock(Record.class);
        when(record.getOffset()).thenReturn(2);
        when(record.getType()).thenReturn("t");
        when(record.getSegmentId()).thenReturn("s");

        PropertyState p = new RecordNode(record).getProperty("offset");

        assertEquals(Type.LONG, p.getType());
        assertEquals(2, p.getValue(Type.LONG).intValue());
    }

    @Test
    public void shouldHaveTypeProperty() {
        Record record = mock(Record.class);
        when(record.getType()).thenReturn("t");
        when(record.getSegmentId()).thenReturn("s");

        PropertyState p = new RecordNode(record).getProperty("type");

        assertEquals(Type.STRING, p.getType());
        assertEquals("t", p.getValue(Type.STRING));
    }

    @Test
    public void shouldHaveSegmentIdProperty() {
        Record record = mock(Record.class);
        when(record.getType()).thenReturn("t");
        when(record.getSegmentId()).thenReturn("s");

        PropertyState p = new RecordNode(record).getProperty("segmentId");

        assertEquals(Type.STRING, p.getType());
        assertEquals("s", p.getValue(Type.STRING));
    }

    @Test
    public void shouldHaveAddressProperty() {
        Record record = mock(Record.class);
        when(record.getAddress()).thenReturn(1);
        when(record.getType()).thenReturn("t");
        when(record.getSegmentId()).thenReturn("s");

        PropertyState p = new RecordNode(record).getProperty("address");

        assertEquals(Type.LONG, p.getType());
        assertEquals(1, p.getValue(Type.LONG).intValue());
    }

}
