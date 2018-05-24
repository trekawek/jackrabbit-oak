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
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class DynamoManifestFile implements ManifestFile {

    private final DynamoDB dynamoDB;

    private final String tableName;

    private final Table table;

    public DynamoManifestFile(DynamoDB dynamoDB, String tableName) {
        this.dynamoDB = dynamoDB;
        this.tableName = tableName;
        this.table = dynamoDB.getTable(tableName);
    }

    @Override
    public boolean exists() {
        return DynamoUtils.tableExists(dynamoDB, tableName);
    }

    @Override
    public Properties load() throws IOException {
        try {
            Properties properties = new Properties();
            for (Item e : table.scan()) {
                properties.put(e.getString("k"), e.getString("v"));
            }
            return properties;
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void save(Properties properties) throws IOException {
        if (exists()) {
            table.delete();
        }
        createTable();
        try {
            for (String key : properties.stringPropertyNames()) {
                Item item = new Item();
                item.withString("k", key);
                item.withString("v", properties.getProperty(key));
                table.putItem(item);
            }
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    private void createTable() throws IOException {
        try {
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("k", KeyType.HASH)),
                    Arrays.asList(new AttributeDefinition("k", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));
            table.waitForActive();
        } catch (InterruptedException | AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }
}
