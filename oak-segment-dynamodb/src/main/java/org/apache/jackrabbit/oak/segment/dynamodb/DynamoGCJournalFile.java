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
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DynamoGCJournalFile implements GCJournalFile {

    private final DynamoDB dynamoDB;

    private final Table table;

    private final String tableName;

    private long id;

    public DynamoGCJournalFile(DynamoDB dynamoDB, String tableName) throws IOException {
        this.dynamoDB = dynamoDB;
        this.table = dynamoDB.getTable(tableName);
        this.tableName = tableName;

        if (DynamoUtils.tableExists(dynamoDB, tableName)) {
            try {
                QuerySpec spec = new QuerySpec()
                        .withHashKey("key", 1)
                        .withScanIndexForward(false);
                IteratorSupport<Item, QueryOutcome> queryResult = table.query(spec).iterator();
                if (queryResult.hasNext()) {
                    id = queryResult.next().getLong("i");
                }
            } catch (AmazonDynamoDBException e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public void writeLine(String line) throws IOException {
        try {
            if (!DynamoUtils.tableExists(dynamoDB, tableName)) {
                createTable();
            }
            Item item = new Item();
            item.withLong("key", 1);
            item.withLong("i", ++id);
            item.withString("v", line);
            table.putItem(item);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<String> readLines() throws IOException {
        try {
            if (!DynamoUtils.tableExists(dynamoDB, tableName)) {
                return Collections.emptyList();
            }
            List<String> lines = new ArrayList<>();
            for (Item item : table.query(new QuerySpec().withHashKey("key", 1))) {
                lines.add(item.getString("v"));
            }
            return lines;
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    private void createTable() throws IOException {
        try {
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(
                            new KeySchemaElement("key", KeyType.HASH),
                            new KeySchemaElement("i", KeyType.RANGE)),
                    Arrays.asList(
                            new AttributeDefinition("key", ScalarAttributeType.N),
                            new AttributeDefinition("i", ScalarAttributeType.N)
                    ),
                    new ProvisionedThroughput(10L, 10L));
            table.waitForActive();
        } catch (InterruptedException | AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }
}
