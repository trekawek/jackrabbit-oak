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
import org.apache.jackrabbit.oak.kv.store.redis.iterators.ScanIterator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.collect.Iterators.transform;
import static java.util.Collections.unmodifiableMap;

/**
 * The node structure within Redis is as follows:
 *
 * <pre>
 * property list:  NNNNNNNN0     => [ "prop1", "prop2", ... ]
 * children list:  NNNNNNNN1     => { "child1" => "MMMMMMMM", "child2" => "OOOOOOO" }
 * property value: NNNNNNNN2PPPP => [ T, A, "value1" [, "value2", ...] ]
 *
 * NNNNNNNN - 8-byte node id
 *     PPPP - 4-byte property index (0x00 for prop1, 0x01 for prop2, etc.)
 *        T - value type byte: (Value.ordinal())
 *        A - arity byte: 0x00 or singular, 0x01 for array
 * </pre>
 */
public class RedisNode implements Node {

    private final JedisPool jedisPool;

    private final RedisID id;

    private Map<String, Value> properties;

    private Map<String, ID> children;

    RedisNode(JedisPool jedisPool, RedisID id) {
        this.jedisPool = jedisPool;
        this.id = id;
    }

    @Override
    public Map<String, Value> getProperties() {
        if (properties == null) {
            try (Jedis jedis = jedisPool.getResource()) {
                Map<String, Value> map = new HashMap<>();
                Iterator<Map.Entry<String, Value>> it = getPropertyIterator(jedis, id);
                while (it.hasNext()) {
                    Map.Entry<String, Value> e = it.next();
                    map.put(e.getKey(), e.getValue());
                }
                properties = unmodifiableMap(map);
            }
        }
        return properties;
    }

    @Override
    public Map<String, ID> getChildren() {
        if (children == null) {
            try (Jedis jedis = jedisPool.getResource()) {
                Map<String, ID> map = new HashMap<>();
                Iterator<Map.Entry<String, RedisID>> it = getChildrenIterator(jedis, id);
                while (it.hasNext()) {
                    Map.Entry<String, RedisID> child = it.next();
                    map.put(child.getKey(), child.getValue());
                }
                children = unmodifiableMap(map);
            }
        }
        return children;
    }

    private static Iterator<Map.Entry<String, Value>> getPropertyIterator(Jedis jedis, RedisID id) {
        return new PropertyIterator(jedis, id);
    }

    private static Iterator<Map.Entry<String, RedisID>> getChildrenIterator(Jedis jedis, RedisID id) {
        return transform(
                new ScanIterator<>(cursor -> jedis.hscan(id.getChildrenHashKey(), cursor.getBytes())),
                e -> new AbstractMap.SimpleEntry<>(new String(e.getKey()), new RedisID(e.getValue()))
        );
    }
}
