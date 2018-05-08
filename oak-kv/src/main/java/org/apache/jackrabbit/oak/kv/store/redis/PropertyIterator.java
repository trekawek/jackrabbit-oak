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

import com.google.common.collect.AbstractIterator;
import org.apache.jackrabbit.oak.kv.store.Type;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.apache.jackrabbit.oak.kv.store.redis.iterators.ListIterator;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.transform;

public class PropertyIterator extends AbstractIterator<Map.Entry<String, Value>> {

    private final Jedis jedis;

    private final RedisID nodeID;

    private final ListIterator it;

    private long i;

    public PropertyIterator(Jedis jedis, RedisID nodeID) {
        this.jedis = jedis;
        this.nodeID = nodeID;
        this.it = new ListIterator(jedis, nodeID.getPropertyListKey(), 0);
    }

    @Override
    protected Map.Entry<String, Value> computeNext() {
        if (it.hasNext()) {
            byte[] propertyName = it.next();
            byte[] key = nodeID.getPropertyKey(i++);
            return new AbstractMap.SimpleEntry<>(new String(propertyName), getPropertyByKey(key));
        } else {
            return endOfData();
        }
    }


    private Value getPropertyByKey(byte[] key) {
        List<byte[]> list = jedis.lrange(key, 0, 2);

        Type type = Type.values()[list.get(0)[0]];
        boolean isArray = list.get(1)[0] == 1;

        if (isArray) {
            Iterable<String> values = transform(() -> new ListIterator(jedis, key, 2), String::new);
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
            String value = new String(list.get(2));

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
        throw new IllegalStateException("Invalid property type " + key + ": " + list);
    }
}
