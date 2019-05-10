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

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.CreateDynamoDBTableOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class DynamoRepositoryLock implements RepositoryLock {

    private final AmazonDynamoDBLockClient client;

    private final LockItem lock;

    public DynamoRepositoryLock(AmazonDynamoDB amazonDynamoDB, String tableName) throws IOException {
        try {
            DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
            if (!DynamoUtils.tableExists(dynamoDB, tableName)) {
                AmazonDynamoDBLockClient.createLockTableInDynamoDB(CreateDynamoDBTableOptions.builder(
                        amazonDynamoDB,
                        new ProvisionedThroughput(10L, 10L),
                        tableName
                ).build());
                dynamoDB.getTable(tableName).waitForActive();
            }
            client = new AmazonDynamoDBLockClient(
                    AmazonDynamoDBLockClientOptions.builder(amazonDynamoDB, tableName)
                            .withTimeUnit(TimeUnit.SECONDS)
                            .withLeaseDuration(10L)
                            .withHeartbeatPeriod(3L)
                            .withCreateHeartbeatBackgroundThread(true)
                            .build());
            Optional<LockItem> lockItem = client.tryAcquireLock(AcquireLockOptions.builder("key").build());
            lock = lockItem.orElseThrow(() -> new IOException("Can't acquire lock"));
        } catch (AmazonDynamoDBException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void unlock() throws IOException {
        try {
            client.releaseLock(lock);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }
}
