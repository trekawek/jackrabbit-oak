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
 */
package org.apache.jackrabbit.oak.segment.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DynamoSegmentArchiveReader implements SegmentArchiveReader {

    private final Item archive;

    private final String archiveName;

    private final Table segmentTable;

    private final IOMonitor ioMonitor;

    private final long length;

    private final Map<UUID, SegmentArchiveEntry> index = new LinkedHashMap<>();

    private Boolean hasGraph;

    DynamoSegmentArchiveReader(Item archive, Table segmentTable, IOMonitor ioMonitor) throws IOException {
        this.archive = archive;
        this.archiveName = archive.getString("archiveName");
        this.segmentTable = segmentTable;
        this.ioMonitor = ioMonitor;
        long length = 0;

        QuerySpec query = new QuerySpec()
                .withAttributesToGet("msb", "lsb", "length", "generation", "fullGeneration", "isCompacted")
                .withKeyConditionExpression("archiveName = :a")
                .withValueMap(new ValueMap().withString(":a", archiveName));
        try {
            for (Item i : segmentTable.query(query)) {
                SegmentArchiveEntry indexEntry = new DynamoArchiveEntry(i);
                index.put(new UUID(indexEntry.getMsb(), indexEntry.getLsb()), indexEntry);
                length += indexEntry.getLength();
            }
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
        this.length = length;
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        SegmentArchiveEntry indexEntry = index.get(new UUID(msb, lsb));
        if (indexEntry == null) {
            return null;
        }

        Buffer buffer = null;
        ioMonitor.beforeSegmentRead(pathAsFile(), msb, lsb, indexEntry.getLength());
        Stopwatch stopwatch = Stopwatch.createStarted();

        QuerySpec query = new QuerySpec()
                .withAttributesToGet("data")
                .withKeyConditionExpression("archiveName = :a AND msb = :msb AND lsb = :lsb")
                .withValueMap(new ValueMap()
                        .withString(":a", archiveName)
                        .withLong("msb", msb)
                        .withList("lsb", lsb));
        try {
            Iterator<Item> it = segmentTable.query(query).iterator();
            if (it.hasNext()) {
                buffer = Buffer.wrap(it.next().getBinary("data"));
            }
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }

        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        ioMonitor.afterSegmentRead(pathAsFile(), msb, lsb, indexEntry.getLength(), elapsed);
        return buffer;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return index.containsKey(new UUID(msb, lsb));
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        return new ArrayList<>(index.values());
    }

    @Override
    public Buffer getGraph() throws IOException {
        Buffer graph = null;
        try {
            if (archive.hasAttribute("graph")) {
                graph = Buffer.wrap(archive.getBinary("graph"));
            }
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
        hasGraph = graph != null;
        return graph;
    }

    @Override
    public boolean hasGraph() {
        if (hasGraph == null) {
            try {
                getGraph();
            } catch (IOException ignore) { }
        }
        return hasGraph;
    }

    @Override
    public Buffer getBinaryReferences() throws IOException {
        if (archive.hasAttribute("binaryReferences")) {
            return Buffer.wrap(archive.getBinary("binaryReferences"));
        } else {
            return null;
        }
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public String getName() {
        return archiveName;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public int getEntrySize(int size) {
        return size;
    }

    private File pathAsFile() {
        return new File(archiveName);
    }

}
