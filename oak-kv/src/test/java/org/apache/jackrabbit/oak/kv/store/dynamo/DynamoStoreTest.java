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
import org.apache.jackrabbit.oak.kv.store.AbstractStoreTest;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.apache.jackrabbit.oak.kv.store.redis.RedisRule;
import org.apache.jackrabbit.oak.kv.store.redis.RedisStore;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;

public class DynamoStoreTest extends AbstractStoreTest {

    @ClassRule
    public static DynamoRule dynamoRule = new DynamoRule();

    @Before
    public void setup() throws IOException, InterruptedException {
        store = new DynamoStore(new DynamoDB(dynamoRule.getDynamoDB()));
    }
}
