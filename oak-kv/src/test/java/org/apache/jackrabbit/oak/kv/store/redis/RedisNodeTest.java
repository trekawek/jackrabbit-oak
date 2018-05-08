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

import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;

public class RedisNodeTest {

    @ClassRule
    public static RedisRule redisRule = new RedisRule();

    private Jedis jedis;

    private RedisStore store;

    @Before
    public void setup() {
        jedis = redisRule.getJedis();
        jedis.flushAll();
        store = new RedisStore(jedis);
    }

    @After
    public void teardown() throws IOException {
        store.close();
    }

    @Test
    public void testManyProperties() throws IOException {
        Map<String, Value> properties = new HashMap<>();

        for (int i = 0; i < 123; i++) { // not a batch size multiplier
            properties.put("key" + i, Value.newStringValue(UUID.randomUUID().toString()));
        }

        Node node = store.getNode(store.putNode(properties, emptyMap()));
        assertEquals(properties, node.getProperties());
    }

    @Test
    public void testManyChildren() throws IOException {
        Map<String, ID> children = new HashMap<>();

        for (int i = 0; i < 123; i++) { // not a batch size multiplier
            children.put("child" + i, new RedisID(i));
        }

        Node node = store.getNode(store.putNode(emptyMap(), children));
        assertEquals(children, node.getChildren());
    }
}
