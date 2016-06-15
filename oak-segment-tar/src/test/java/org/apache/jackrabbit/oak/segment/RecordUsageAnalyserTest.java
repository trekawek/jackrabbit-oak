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

import static com.google.common.base.Strings.repeat;
import static java.util.Collections.nCopies;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.ListRecord.LEVEL_SIZE;
import static org.apache.jackrabbit.oak.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.Segment.SMALL_LIMIT;
import static org.apache.jackrabbit.oak.segment.SegmentWriterBuilder.segmentWriterBuilder;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class RecordUsageAnalyserTest {
    private SegmentWriter writer;
    private RecordUsageAnalyser analyser;

    @Before
    public void setup() throws IOException {
        MemoryStore store = new MemoryStore();
        writer = segmentWriterBuilder("").build(store);
        analyser = new RecordUsageAnalyser(store.getReader());
    }

    @Test
    public void emptyNode() throws IOException {
        SegmentNodeState node = writer.writeNode(EMPTY_NODE);
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 0, 4, 3);
    }

    @Test
    public void nodeWithInt() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("one", 1);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 6, 8, 6);
    }

    @Test
    public void nodeWithString() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("two", "222");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 8, 8, 6);
    }

    @Test
    public void nodeWithMultipleProperties() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("one", "11");
        builder.setProperty("two", "22");
        builder.setProperty("three", "33");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 18, 23, 10, 6);
    }

    @Test
    public void nodeWithMediumString() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("medium", repeat("a", SMALL_LIMIT + 1));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 138, 8, 6);
    }

    @Test
    public void nodeWithLargeString() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("large", repeat("b", MEDIUM_LIMIT + 1));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 15, 16530, 8, 6);
    }

    @Test
    public void nodeWithSameString() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("two", "two");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 4, 8, 6);
    }

    @Test
    public void nodeWithInts() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("multi", ImmutableList.of(1L, 2L, 3L, 4L), LONGS);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 12, 21, 8, 6);
    }

    @Test
    public void nodeWithManyInts() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("multi", nCopies(LEVEL_SIZE + 1, 1L), LONGS);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 771, 15, 8, 6);
    }

    @Test
    public void nodeWithManyIntsAndOne() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("multi", nCopies(LEVEL_SIZE + 2, 1L), LONGS);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 777, 15, 8, 6);
    }

    @Test
    public void nodeWithStrings() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("multi", ImmutableList.of("one", "one", "two", "two", "three"), STRINGS);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 15, 27, 8, 6);
    }

    @Test
    public void nodeWithBlob() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("blob", createRandomBlob(4));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 10, 8, 6);
    }

    @Test
    public void nodeWithMediumBlob() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("mediumBlob", createRandomBlob(SMALL_LIMIT + 1));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 142, 8, 6);
    }

    @Test
    public void nodeWithLargeBlob() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("largeBlob", createRandomBlob(MEDIUM_LIMIT + 1));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 15, 16534, 8, 6);
    }

    @Test
    public void nodeWithPrimaryType() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("jcr:primaryType", "type", NAME);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 5, 7, 3);
    }

    @Test
    public void nodeWithMixinTypes() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("jcr:mixinTypes", ImmutableList.of("type1", "type2"), NAMES);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 12, 10, 3);
    }

    @Test
    public void singleChild() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("child");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 6, 11, 9);
    }

    @Test
    public void multiChild() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("child1");
        builder.setChildNode("child2");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 24, 0, 14, 8, 12);
    }

    @Test
    public void manyChild() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        for (int k = 0; k < MapRecord.BUCKETS_PER_LEVEL + 1; k++) {
            builder.setChildNode("child" + k);
        }

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 457, 0, 254, 8, 105);
    }

    @Test
    public void changedChild() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("child1");
        builder.setChildNode("child2");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 24, 0, 14, 8, 12);

        builder = node.builder();
        builder.child("child1").setProperty("p", "q");

        node = (SegmentNodeState) builder.getNodeState();

        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 41, 0, 18, 16, 24);
    }

    @Test
    public void counts() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("child1");
        builder.setChildNode("child2");
        builder.setProperty("prop", ImmutableList.of("a", "b"), STRINGS);
        builder.setProperty("mediumString", repeat("m", SMALL_LIMIT));
        builder.setProperty("longString", repeat("l", MEDIUM_LIMIT));
        builder.setProperty("smallBlob", createRandomBlob(4));
        builder.setProperty("mediumBlob", createRandomBlob(SMALL_LIMIT));
        builder.setProperty("longBlob", createRandomBlob(MEDIUM_LIMIT));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertCounts(analyser, 1, 5, 6, 1, 1, 1, 0, 10, 1, 1, 2, 3);
    }

    private static Blob createRandomBlob(int size) {
        byte[] bytes = new byte[size];
        new Random().nextBytes(bytes);
        return new ArrayBasedBlob(bytes);
    }

    private static void assertSizes(RecordUsageAnalyser analyser,
            long maps, long lists, long values, long templates, long nodes) {
        assertEquals("maps sizes mismatch", maps, analyser.getMapSize());
        assertEquals("lists sizes mismatch", lists, analyser.getListSize());
        assertEquals("value sizes mismatch", values, analyser.getValueSize());
        assertEquals("template sizes mismatch", templates, analyser.getTemplateSize());
        assertEquals("nodes sizes mismatch", nodes, analyser.getNodeSize());
    }

    private static void assertCounts(RecordUsageAnalyser analyser,
            long mapCount, long listCount, long propertyCount,
            long smallBlobCount, long mediumBlobCount, long longBlobCount, long externalBlobCount,
            long smallStringCount, long mediumStringCount, long longStringCount,
            long templateCount, long nodeCount) {
        assertEquals("map count mismatch", mapCount, analyser.getMapCount());
        assertEquals("list count mismatch", listCount, analyser.getListCount());
        assertEquals("property count mismatch", propertyCount, analyser.getPropertyCount());
        assertEquals("small blob count mismatch", smallBlobCount, analyser.getSmallBlobCount());
        assertEquals("medium blob mismatch", mediumBlobCount, analyser.getMediumBlobCount());
        assertEquals("long blob count mismatch", longBlobCount, analyser.getLongBlobCount());
        assertEquals("external blob count mismatch", externalBlobCount, analyser.getExternalBlobCount());
        assertEquals("small string count mismatch", smallStringCount, analyser.getSmallStringCount());
        assertEquals("medium string count mismatch", mediumStringCount, analyser.getMediumStringCount());
        assertEquals("long string count mismatch", longStringCount, analyser.getLongStringCount());
        assertEquals("template count mismatch", templateCount, analyser.getTemplateCount());
        assertEquals("node count mismatch", nodeCount, analyser.getNodeCount());

    }

}
