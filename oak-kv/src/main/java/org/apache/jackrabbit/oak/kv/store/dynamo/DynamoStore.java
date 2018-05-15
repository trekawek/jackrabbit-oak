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
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Store;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.apache.jackrabbit.oak.kv.store.leveldb.Converters;
import org.apache.jackrabbit.oak.kv.store.leveldb.LevelDBID;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class DynamoStore implements Store {

    private final Table tagsTable;

    private final Table nodesTable;

    public DynamoStore(DynamoDB dynamoDB) throws InterruptedException, IOException {
        DynamoTableSchema.createSchema(dynamoDB);
        tagsTable = dynamoDB.getTable("tags");
        nodesTable = dynamoDB.getTable("nodes");
    }

    @Override
    public ID getTag(String tag) throws IOException {
        checkNotNull(tag);
        Item item;
        try {
            item = tagsTable.getItem("tag", tag);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
        if (item == null) {
            return null;
        } else {
            return new LevelDBID(UUID.fromString(item.getString("uuid")));
        }
    }

    @Override
    public void putTag(String tag, ID id) throws IOException {
        checkNotNull(tag);
        Item item = new Item()
                .withString("tag", tag)
                .withString("uuid", id.toString());

        try {
            tagsTable.putItem(item);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void deleteTag(String tag) throws IOException {
        checkNotNull(tag);

        try {
            tagsTable.deleteItem("tag", tag);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Node getNode(ID id) throws IOException {
        Item item;
        try {
            item = nodesTable.getItem("uuid", id.toString());
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
        return Converters.readNode(item.getBinary("data"));
    }

    @Override
    public ID putNode(Map<String, Value> properties, Map<String, ID> children) throws IOException {
        ID id = new LevelDBID(UUID.randomUUID());
        Item item = new Item()
                .withString("uuid", id.toString())
                .withBinary("data", Converters.write(properties, children));
        try {
            nodesTable.putItem(item);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
        return id;
    }
}
