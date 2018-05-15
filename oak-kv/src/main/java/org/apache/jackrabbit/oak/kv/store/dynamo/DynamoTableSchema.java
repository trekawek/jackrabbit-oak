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
package org.apache.jackrabbit.oak.kv.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import java.io.IOException;
import java.util.Arrays;

public final class DynamoTableSchema {

    private DynamoTableSchema() {
    }

    public static void createSchema(DynamoDB dynamoDB) throws InterruptedException, IOException {
        try {
            if (!tableExists(dynamoDB, "tags")) {
                createTagsTable(dynamoDB);
            }
            if (!tableExists(dynamoDB, "nodes")) {
                createNodesTable(dynamoDB);
            }
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    private static void createTagsTable(DynamoDB dynamoDB) throws InterruptedException {
        Table table = dynamoDB.createTable("tags",
                Arrays.asList(
                        new KeySchemaElement("tag", KeyType.HASH)),
                Arrays.asList(
                        new AttributeDefinition("tag", ScalarAttributeType.S)),
                new ProvisionedThroughput(10L, 10L));
        table.waitForActive();
    }

    private static void createNodesTable(DynamoDB dynamoDB) throws InterruptedException {
        Table table = dynamoDB.createTable("nodes",
                Arrays.asList(
                        new KeySchemaElement("uuid", KeyType.HASH)),
                Arrays.asList(
                        new AttributeDefinition("uuid", ScalarAttributeType.S)),
                new ProvisionedThroughput(10L, 10L));
        table.waitForActive();
    }


    private static boolean tableExists(DynamoDB dynamoDB, String tableName) {
        try {
            dynamoDB.getTable(tableName).describe();
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }
}
