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

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.collect.Iterators.transform;

/**
 * <pre>
 * property list: NNNNNNNN0 => [ "property_name_1", "property_name_2", ... ]
 * children list: NNNNNNNN1 => { "child1" => "MMMMMMMM", "child2" => "OOOOOOO" }
 * property value: NNNNNNNN2PPPP => [ T, A, "value1" [, "value2", ...] ]
 *
 * NNNNNNNN, MMMMMMMM, OOOOOOOO - 8-byte node id
 * PPPP - 4-byte property id
 * T - value type byte: (Value.ordinal())
 * A - arity byte: 0x00 or singular, 0x01 for array
 * </pre>
 */
public class RedisNode implements Node {

    private final Jedis jedis;

    private final RedisID id;

    RedisNode(Jedis jedis, RedisID id) {
        this.jedis = jedis;
        this.id = id;
    }

    private Iterator<Map.Entry<String, Value>> getPropertyIterator() {
        return new PropertyIterator(jedis, id);
    }

    private Iterator<Map.Entry<String, RedisID>> getChildrenIterator() {
        return transform(
                new ScanIterator<>(cursor -> jedis.hscan(id.getChildrenHashKey(), cursor.getBytes())),
                e -> new AbstractMap.SimpleEntry<>(new String(e.getKey()), new RedisID(e.getValue()))
        );
    }

    @Override
    public Map<String, Value> getProperties() {
        Map<String, Value> properties = new HashMap<>();
        Iterator<Map.Entry<String, Value>> it = getPropertyIterator();
        while (it.hasNext()) {
            Map.Entry<String, Value> e = it.next();
            properties.put(e.getKey(), e.getValue());
        }
        return properties;
    }

    @Override
    public Map<String, ID> getChildren() {
        Map<String, ID> children = new HashMap<>();
        Iterator<Map.Entry<String, RedisID>> it = getChildrenIterator();
        while (it.hasNext()) {
            Map.Entry<String, RedisID> child = it.next();
            children.put(child.getKey(), child.getValue());
        }
        return children;
    }
}
