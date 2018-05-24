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
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class DynamoJournalFile implements JournalFile {

    private final DynamoDB dynamoDB;

    private final Table table;

    private final String tableName;

    public DynamoJournalFile(DynamoDB dynamoDB, String tableName) {
        this.dynamoDB = dynamoDB;
        this.table = dynamoDB.getTable(tableName);
        this.tableName = tableName;
    }

    @Override
    public JournalFileReader openJournalReader() {
        return new AzureJournalReader();
    }

    @Override
    public JournalFileWriter openJournalWriter() throws IOException {
        return new AzureJournalWriter();
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public boolean exists() {
        return DynamoUtils.tableExists(dynamoDB, tableName);
    }

    private void createTable() throws IOException {
        try {
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("i", KeyType.RANGE)),
                    Arrays.asList(new AttributeDefinition("i", ScalarAttributeType.N)),
                    new ProvisionedThroughput(10L, 10L));
            table.waitForActive();
        } catch (InterruptedException | AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    private class AzureJournalReader implements JournalFileReader {

        private IteratorSupport<Item, QueryOutcome> queryResult;

        private AzureJournalReader() {
        }

        @Override
        public String readLine() throws IOException {
            try {
                if (queryResult == null) {
                    if (!exists()) {
                        return null;
                    }
                    QuerySpec spec = new QuerySpec().withScanIndexForward(false);
                    queryResult = table.query(spec).iterator();
                }
                return queryResult.next().getString("v");
            } catch (AmazonDynamoDBException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close() {
        }
    }

    private class AzureJournalWriter implements JournalFileWriter {

        private final AtomicLong id;

        public AzureJournalWriter() throws IOException {
            try {
                QuerySpec spec = new QuerySpec().withScanIndexForward(false);
                IteratorSupport<Item, QueryOutcome> queryResult = table.query(spec).iterator();
                if (queryResult.hasNext()) {
                    id = new AtomicLong(queryResult.next().getLong("i"));
                } else {
                    id = new AtomicLong();
                }
            } catch (AmazonDynamoDBException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void truncate() throws IOException {
            try {
                table.delete();
            } catch (AmazonDynamoDBException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void writeLine(String line) throws IOException {
            if (!exists()) {
                createTable();
            }
            try {
                Item item = new Item();
                item.withLong("i", id.addAndGet(1));
                item.withString("v", line);
                table.putItem(item);
            } catch (AmazonDynamoDBException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close() {
        }
    }

}