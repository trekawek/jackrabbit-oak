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
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.segment.spi.asyncwrite.SegmentWriteAction;
import org.apache.jackrabbit.oak.segment.spi.asyncwrite.SegmentWriteQueue;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DynamoSegmentArchiveWriter implements SegmentArchiveWriter {

    private final String archiveName;

    private final Item archive;

    private final Table archiveTable;

    private final Table segmentTable;

    private final IOMonitor ioMonitor;

    private final FileStoreMonitor monitor;

    private final Optional<SegmentWriteQueue<DynamoArchiveEntry>> queue;

    private Map<UUID, DynamoArchiveEntry> index = Collections.synchronizedMap(new LinkedHashMap<>());

    private int entries;

    private long totalLength;

    private volatile boolean created = false;

    public DynamoSegmentArchiveWriter(String archiveName, Table archiveTable, Table segmentTable, IOMonitor ioMonitor, FileStoreMonitor monitor) throws IOException {
        this.archiveName = archiveName;
        this.archiveTable = archiveTable;
        this.segmentTable = segmentTable;
        this.ioMonitor = ioMonitor;
        this.monitor = monitor;
        this.queue = SegmentWriteQueue.THREADS > 0 ? Optional.of(new SegmentWriteQueue<DynamoArchiveEntry>(this::doWriteEntry)) : Optional.empty();

        try {
            archive = new Item()
                    .withString("archiveName", archiveName)
                    .withBoolean("closed", false);
            archiveTable.putItem(archive);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeSegment(long msb, long lsb, byte[] data, int offset, int size, int generation, int fullGeneration, boolean isCompacted) throws IOException {
        created = true;
        Item segment = new Item()
                .withString("archiveName", archiveName)
                .withString("uuid", new UUID(msb, lsb).toString())
                .withInt("generation", generation)
                .withInt("fullGeneration", fullGeneration)
                .withBoolean("isCompacted", isCompacted)
                .withInt("position", entries++)
                .withInt("length", size);
        DynamoArchiveEntry entry = new DynamoArchiveEntry(segment);
        if (queue.isPresent()) {
            queue.get().addToQueue(entry, data, offset, size);
        } else {
            doWriteEntry(entry, data, offset, size);
        }
        index.put(new UUID(msb, lsb), entry);

        totalLength += size;
        monitor.written(size);
    }

    private void doWriteEntry(DynamoArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException {
        long msb = indexEntry.getMsb();
        long lsb = indexEntry.getLsb();
        ioMonitor.beforeSegmentWrite(pathAsFile(), msb, lsb, size);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            Item item = new Item()
                    .withBinary("data", ByteBuffer.wrap(data, offset, size));
            for (Map.Entry<String, Object> e : indexEntry.getItem().attributes()) {
                item.with(e.getKey(), e.getValue());
            }
            segmentTable.putItem(item);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
        ioMonitor.afterSegmentWrite(pathAsFile(), msb, lsb, size, stopwatch.elapsed(TimeUnit.NANOSECONDS));
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        UUID uuid = new UUID(msb, lsb);
        Optional<SegmentWriteAction<DynamoArchiveEntry>> segment = queue.map(q -> q.read(uuid));
        if (segment.isPresent()) {
            return segment.get().toBuffer();
        }

        QuerySpec query = new QuerySpec()
                .withAttributesToGet("data")
                .withHashKey("uuid", uuid.toString());
        try {
            Iterator<Item> it = segmentTable.getIndex("uuidIndex").query(query).iterator();
            if (it.hasNext()) {
                return Buffer.wrap(it.next().getBinary("data"));
            } else {
                return null;
            }
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        UUID uuid = new UUID(msb, lsb);
        Optional<SegmentWriteAction> segment = queue.map(q -> q.read(uuid));
        if (segment.isPresent()) {
            return true;
        }
        return index.containsKey(new UUID(msb, lsb));
    }

    @Override
    public void writeGraph(byte[] data) throws IOException {
        try {
            archiveTable.updateItem(new UpdateItemSpec()
                    .withPrimaryKey("archiveName", archiveName)
                    .withUpdateExpression("set graph = :d")
                    .withValueMap(new ValueMap().withBinary(":d", data)));
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeBinaryReferences(byte[] data) throws IOException {
        try {
            archiveTable.updateItem(new UpdateItemSpec()
                    .withPrimaryKey("archiveName", archiveName)
                    .withUpdateExpression("set binaryReferences = :d")
                    .withValueMap(new ValueMap().withBinary(":d", data)));
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long getLength() {
        return totalLength;
    }

    @Override
    public int getEntryCount() {
        return index.size();
    }

    @Override
    public void close() throws IOException {
        if (queue.isPresent()) { // required to handle IOException
            SegmentWriteQueue q = queue.get();
            q.flush();
            q.close();
        }
        try {
            archiveTable.updateItem(new UpdateItemSpec()
                    .withPrimaryKey("archiveName", archiveName)
                    .withUpdateExpression("set closed = :c")
                    .withValueMap(new ValueMap().withBoolean(":c", true)));
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean isCreated() {
        return created || !queueIsEmpty();
    }

    @Override
    public void flush() throws IOException {
        if (queue.isPresent()) { // required to handle IOException
            queue.get().flush();
        }
    }

    private boolean queueIsEmpty() {
        return queue.map(SegmentWriteQueue::isEmpty).orElse(true);
    }

    @Override
    public String getName() {
        return archiveName;
    }

    private File pathAsFile() {
        return new File(archiveName);
    }
}
