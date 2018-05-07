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
package org.apache.jackrabbit.oak.kv.store.redis.iterators;

import org.apache.jackrabbit.oak.kv.store.redis.RedisRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestScanIterator {

    @ClassRule
    public static RedisRule redisRule = new RedisRule();

    private Jedis jedis;

    @Before
    public void setup() {
        jedis = redisRule.getJedis();
        jedis.flushAll();
    }

    @Test
    public void testEmptyIterator() {
        ScanParams params = new ScanParams().match("xyz*");
        ScanIterator<String> it = new ScanIterator<>(s -> jedis.scan(s, params));
        assertFalse(it.hasNext());
    }

    @Test
    public void testSingleBatch() {
        testIterator(10);
    }

    @Test
    public void testMultiBatches() {
        testIterator(123);
    }

    private void testIterator(int count) {
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < count; i++) {
            String key = "xyz" + i;
            jedis.set(key, "abc");
            keys.add(key);
        }
        ScanParams params = new ScanParams().match("xyz*");
        ScanIterator<String> it = new ScanIterator<>(s -> jedis.scan(s, params));
        for (int i = 0; i < count; i++) {
            it.hasNext();
            assertTrue(it.hasNext());
            assertTrue(keys.remove(it.next()));
        }
        assertTrue(keys.isEmpty());
    }
}
