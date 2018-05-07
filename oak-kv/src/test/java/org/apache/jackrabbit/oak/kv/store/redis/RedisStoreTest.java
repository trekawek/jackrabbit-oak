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
package org.apache.jackrabbit.oak.kv.store.redis;

import org.apache.jackrabbit.oak.kv.store.AbstractStoreTest;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;

public class RedisStoreTest extends AbstractStoreTest {

    @ClassRule
    public static RedisRule redisRule = new RedisRule();

    @Before
    public void setup() {
        Jedis jedis = redisRule.getJedis();
        jedis.flushAll();
        store = new RedisStore(jedis);
    }

    @After
    public void teardown() throws IOException {
        ((Closeable) store).close();
    }

    @Test
    public void testLongProperty() throws IOException {
        Map<String, Value> properties = new HashMap<>();
        List<String> property = new ArrayList<>();
        for (int i = 0; i < 123; i++) {
            property.add("value" + i);
        }
        properties.put("prop", Value.newStringArray(property));

        Node node = store.getNode(store.putNode(properties, emptyMap()));
        assertEquals(properties, node.getProperties());
    }
}
