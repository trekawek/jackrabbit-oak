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
import org.apache.jackrabbit.oak.kv.store.Type;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.apache.jackrabbit.oak.kv.store.redis.iterators.ListIterator;
import org.apache.jackrabbit.oak.kv.store.redis.iterators.ScanIterator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;

import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.transform;

/**
 * <pre>
 * UUID:p:jcr:mixinTypes = [ "string", "mix:versionable", "mix:referenceable" ]
 * UUID:p:sling:resourceType = [ "string", "libs/sling/page" ]
 * UUID:c = { "child1" => UUID1, "child2" => UUID2, "child3" => UUID3 }
 * </pre>
 */
public class RedisNode implements Node {

    private final Jedis jedis;

    private final RedisID id;

    RedisNode(Jedis jedis, RedisID id) {
        this.jedis = jedis;
        this.id = id;
    }

    private Iterator<String> getPropertyNames() {
        String pattern = id.getUUID() + ":p:*";
        ScanParams params = new ScanParams().match(pattern);
        return transform(
                new ScanIterator<String>(cursor -> jedis.scan(cursor, params)),
                s -> s.substring(pattern.length() - 1)
        );
    }

    private Iterator<Map.Entry<String, RedisID>> getChildrenIterator() {
        String key = id.getUUID() + ":c";
        return transform(
                new ScanIterator<Map.Entry<String, String>>(cursor -> jedis.hscan(key, cursor)),
                e -> new AbstractMap.SimpleEntry<>(e.getKey(), new RedisID(e.getValue()))
        );
    }

    private Value getProperty(String name) {
        String key = id.getUUID() + ":p:" + name;
        List<String> list = jedis.lrange(key, 0, 2);

        Type type = Type.valueOf(list.get(0));
        boolean isArray = Boolean.parseBoolean(list.get(1));

        if (isArray) {
            Iterable<String> values = () -> new ListIterator(jedis, key, 2);
            switch (type) {
                case STRING:
                    return Value.newStringArray(values);

                case BINARY:
                    return Value.newBinaryArray(values);

                case LONG:
                    return Value.newLongArray(transform(values, Long::parseLong));

                case DOUBLE:
                    return Value.newDoubleArray(transform(values, Double::parseDouble));

                case DATE:
                    return Value.newDateArray(values);

                case BOOLEAN:
                    return Value.newBooleanArray(transform(values, Boolean::parseBoolean));

                case NAME:
                    return Value.newNameArray(values);

                case PATH:
                    return Value.newPathArray(values);

                case REFERENCE:
                    return Value.newReferenceArray(values);

                case WEAK_REFERENCE:
                    return Value.newWeakReferenceArray(values);

                case URI:
                    return Value.newURIArray(values);

                case DECIMAL:
                    return Value.newDecimalArray(transform(values, BigDecimal::new));
            }
        } else {
            String value = list.get(2);

            switch (type) {
                case STRING:
                    return Value.newStringValue(value);

                case BINARY:
                    return Value.newBinaryValue(value);

                case LONG:
                    return Value.newLongValue(Long.parseLong(value));

                case DOUBLE:
                    return Value.newDoubleValue(Double.parseDouble(value));

                case DATE:
                    return Value.newDateValue(value);

                case BOOLEAN:
                    return Value.newBooleanValue(Boolean.parseBoolean(value));

                case NAME:
                    return Value.newNameValue(value);

                case PATH:
                    return Value.newPathValue(value);

                case REFERENCE:
                    return Value.newReferenceValue(value);

                case WEAK_REFERENCE:
                    return Value.newWeakReferenceValue(value);

                case URI:
                    return Value.newURIValue(value);

                case DECIMAL:
                    return Value.newDecimalValue(new BigDecimal(value));
            }
        }
        throw new IllegalStateException("Invalid property type " + name + ": " + list);
    }

    @Override
    public Map<String, Value> getProperties() {
        Map<String, Value> properties = new HashMap<>();
        Iterator<String> it = getPropertyNames();
        while (it.hasNext()) {
            String key = it.next();
            properties.put(key, getProperty(key));
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
