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

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;

public class DynamoArchiveManager implements SegmentArchiveManager {

    private static final Logger log = LoggerFactory.getLogger(DynamoSegmentArchiveReader.class);

    private final DynamoDB dynamoDB;

    private final Table archiveTable;

    private final Table segmentTable;

    private final IOMonitor ioMonitor;

    private final FileStoreMonitor monitor;

    public DynamoArchiveManager(DynamoDB dynamoDB, String archiveTableName, String segmentTableName, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor) throws IOException {
        this.dynamoDB = dynamoDB;
        if (DynamoUtils.tableExists(dynamoDB, archiveTableName)) {
            archiveTable = dynamoDB.getTable(archiveTableName);
        } else {
            archiveTable = createArchiveTable(archiveTableName);
        }
        if (DynamoUtils.tableExists(dynamoDB, segmentTableName)) {
            segmentTable = dynamoDB.getTable(segmentTableName);
        } else {
            segmentTable = createSegmentTable(segmentTableName);
        }
        this.ioMonitor = ioMonitor;
        this.monitor = fileStoreMonitor;
    }

    @Override
    public List<String> listArchives() throws IOException {
        try {
            return StreamSupport.stream(archiveTable.scan().spliterator(), false)
                    .map(i -> i.getString("archiveName"))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public SegmentArchiveReader open(String archiveName) throws IOException {
        try {
            Item item = archiveTable.getItem("archiveName", archiveName);
            if (item == null) {
                throw new IOException("Archive not found: " + archiveName);
            }
            if (!item.hasAttribute("closed") || !item.getBoolean("closed")) {
                throw new IOException("The archive " + archiveName + " hasn't been closed correctly.");
            }
            return new DynamoSegmentArchiveReader(item, segmentTable, ioMonitor);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public @Nullable SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        try {
            Item item = archiveTable.getItem("archiveName", archiveName);
            if (item == null) {
                throw new IOException("Archive not found: " + archiveName);
            }
            return new DynamoSegmentArchiveReader(item, segmentTable, ioMonitor);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public SegmentArchiveWriter create(String archiveName) throws IOException {
        try {
            return new DynamoSegmentArchiveWriter(archiveName, archiveTable, segmentTable, ioMonitor, monitor);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean delete(String archiveName) {
        try {
            archiveTable.deleteItem("archiveName", archiveName);
            segmentTable.deleteItem("archiveName", archiveName);
            return true;
        } catch (AmazonDynamoDBException e) {
            log.error("Can't delete archive {}", archiveName, e);
            return false;
        }
    }

    @Override
    public boolean renameTo(String from, String to) {
        try {
            archiveTable.updateItem(new UpdateItemSpec()
                    .withPrimaryKey("archiveName", from)
                    .withUpdateExpression("set archiveName = :n")
                    .withValueMap(new ValueMap().withString(":n", to)));

            QuerySpec query = new QuerySpec()
                    .withAttributesToGet("uuid")
                    .withKeyConditionExpression("archiveName = :a")
                    .withValueMap(new ValueMap().withString(":a", from));
            for (Item i : segmentTable.query(query)) {
                segmentTable.updateItem(new UpdateItemSpec()
                        .withPrimaryKey("uuid", i.getString("uuid"))
                        .withUpdateExpression("set archiveName = :n")
                        .withValueMap(new ValueMap().withString(":n", to)));
            }
            return true;
        } catch (AmazonDynamoDBException e) {
            log.error("Can't rename archive {} to {}", from, to, e);
            return false;
        }
    }

    @Override
    public void copyFile(String from, String to) throws IOException {
        try {
            Item archiveItem = archiveTable.getItem("archiveName", from);
            archiveItem.withString("archiveName", to);
            archiveTable.putItem(archiveItem);

            QuerySpec query = new QuerySpec()
                    .withKeyConditionExpression("archiveName = :a")
                    .withValueMap(new ValueMap().withString(":a", from));
            for (Item i : segmentTable.query(query)) {
                i.withString("archiveName", to);
                segmentTable.putItem(i);
            }
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean exists(String archiveName) {
        try {
            return archiveTable.getItem("archiveName", archiveName) != null;
        } catch (AmazonDynamoDBException e) {
            log.error("Can't check the existence of {}", archiveName, e);
            return false;
        }
    }

    @Override
    public void recoverEntries(String archiveName, LinkedHashMap<UUID, byte[]> entries) throws IOException {
        List<RecoveredEntry> entryList = new ArrayList<>();

        QuerySpec query = new QuerySpec()
                .withKeyConditionExpression("archiveName = :a")
                .withValueMap(new ValueMap().withString(":a", archiveName));
        for (Item i : segmentTable.query(query)) {
            int position = i.getInt("position");
            UUID uuid = UUID.fromString(i.getString("uuid"));
            entryList.add(new RecoveredEntry(position, uuid, i.getBinary("data")));
        }
        Collections.sort(entryList);

        int i = 0;
        for (RecoveredEntry e : entryList) {
            if (e.position != i) {
                log.warn("Missing entry {}.??? when recovering {}. No more segments will be read.", String.format("%04X", i), archiveName);
                break;
            }
            log.info("Recovering segment {}/{}-{}", archiveName, e.position, e.uuid.toString());
            entries.put(e.uuid, e.data);
            i++;
        }
    }

    private Table createArchiveTable(String tableName) throws IOException {
        try {
            Table table = dynamoDB.createTable(tableName,
                    asList(new KeySchemaElement("archiveName", KeyType.HASH)),
                    asList(new AttributeDefinition("archiveName", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));
            table.waitForActive();
            return table;
        } catch (InterruptedException | AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    private Table createSegmentTable(String tableName) throws IOException {
        try {
            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withTableName(tableName)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(10L)
                            .withWriteCapacityUnits(10L))
                    .withAttributeDefinitions(asList(
                            new AttributeDefinition("archiveName", ScalarAttributeType.S),
                            new AttributeDefinition("position", ScalarAttributeType.N),
                            new AttributeDefinition("uuid", ScalarAttributeType.S)))
                    .withKeySchema(asList(
                            new KeySchemaElement("archiveName", KeyType.HASH),
                            new KeySchemaElement("position", KeyType.RANGE)))
                    .withGlobalSecondaryIndexes(asList(
                            new GlobalSecondaryIndex()
                                    .withIndexName("uuidIndex")
                                    .withProvisionedThroughput(new ProvisionedThroughput()
                                            .withReadCapacityUnits(10L)
                                            .withWriteCapacityUnits(10L))
                                    .withProjection(
                                            new Projection()
                                                    .withProjectionType(ProjectionType.ALL))
                                    .withKeySchema(Arrays.asList(
                                            new KeySchemaElement("uuid", KeyType.HASH)
                                    ))
                    ));
            Table table = dynamoDB.createTable(createTableRequest);
            table.waitForActive();
            return table;
        } catch (InterruptedException | AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    private static class RecoveredEntry implements Comparable<RecoveredEntry> {

        private final byte[] data;

        private final UUID uuid;

        private final int position;

        public RecoveredEntry(int position, UUID uuid, byte[] data) {
            this.data = data;
            this.uuid = uuid;
            this.position = position;
        }

        @Override
        public int compareTo(RecoveredEntry o) {
            return Integer.compare(this.position, o.position);
        }
    }

}
