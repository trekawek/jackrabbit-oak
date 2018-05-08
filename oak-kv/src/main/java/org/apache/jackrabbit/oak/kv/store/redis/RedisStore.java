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

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Store;
import org.apache.jackrabbit.oak.kv.store.Value;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RedisStore implements Store, Closeable {

    /**
     * A number of array values to put in a single call. Has to be larger than 3.
     */
    private static final int BATCH_SIZE = 16;

    private static final byte[] TAG_HASH = "tags".getBytes();

    private final Jedis jedis;

    public RedisStore(Jedis jedis) {
        checkNotNull(jedis);
        this.jedis = jedis;
    }

    @Override
    public ID getTag(String tag) throws IOException {
        checkNotNull(tag);
        byte[] id;
        try {
            id = jedis.hget(TAG_HASH, tag.getBytes());
        } catch (JedisException e) {
            throw new IOException(e);
        }
        if (id == null) {
            return null;
        } else {
            return new RedisID(id);
        }
    }

    @Override
    public void putTag(String tag, ID id) throws IOException {
        checkNotNull(tag);
        checkArgument(id instanceof RedisID, "id is not RedisID");
        try {
            jedis.hset(TAG_HASH, tag.getBytes(), ((RedisID) id).getAsBytes());
        } catch (JedisException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void deleteTag(String tag) throws IOException {
        checkNotNull(tag);
        try {
            jedis.hdel(TAG_HASH, tag.getBytes());
        } catch (JedisException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Node getNode(ID id) {
        checkArgument(id instanceof RedisID, "id is not RedisID");
        return new RedisNode(jedis, (RedisID) id);
    }

    @Override
    public ID putNode(Map<String, Value> properties, Map<String, ID> children) throws IOException {
        checkNotNull(properties);
        checkNotNull(children);
        checkArgument(children.values().stream().allMatch(id -> id instanceof RedisID), "id is not RedisID");
        RedisID id = generateNewId();
        try {
            Transaction t = jedis.multi();
            if (!children.isEmpty()) {
                Map<byte[], byte[]> childrenHash = new HashMap<>(children.size());
                children.forEach((k, v) -> childrenHash.put(k.getBytes(), ((RedisID) v).getAsBytes()));
                t.hmset(id.getChildrenHashKey(), childrenHash);
            }
            if (!properties.isEmpty()) {
                byte[] propertyListKey = id.getPropertyListKey();
                int i = 0;
                for (Map.Entry<String, Value> e : properties.entrySet()) {
                    setProperty(t, id.getPropertyKey(i++), e.getValue());
                    t.rpush(propertyListKey, e.getKey().getBytes());
                }
            }
            t.exec();
        } catch (JedisException e) {
            throw new IOException(e);
        }
        return id;
    }

    private static void setProperty(Transaction t, byte[] propertyKey, Value value) {
        if (value.isArray()) {
            byte[][] list = new byte[BATCH_SIZE][];
            int i = 0;
            list[i++] = new byte[] { (byte) value.getType().ordinal() };
            list[i++] = new byte[] { 1 };
            for (String v : getAsStringIterable(value)) {
                list[i++] = v.getBytes();
                if (i == list.length) {
                    t.rpush(propertyKey, list);
                    i = 0;
                }
            }
            if (i != 0) {
                byte[][] list2 = new byte[i][];
                for (int j = 0; j < i; j++) {
                    list2[j] = list[j];
                }
                t.rpush(propertyKey, list2);
            }
        } else {
            byte[][] list = new byte[3][];
            int i = 0;
            list[i++] = new byte[] { (byte) value.getType().ordinal() };
            list[i++] = new byte[] { 0 };
            list[i++] = getAsString(value).getBytes();
            t.rpush(propertyKey, list);
        }
    }

    private static Iterable<String> getAsStringIterable(Value value) {
        switch (value.getType()) {
            case STRING:
            case BINARY:
            case DATE:
            case NAME:
            case PATH:
            case REFERENCE:
            case WEAK_REFERENCE:
            case URI:
                return value.asStringArray();

            case LONG:
                return Iterables.transform(value.asLongArray(), v -> Long.toString(v));

            case DOUBLE:
                return Iterables.transform(value.asDoubleArray(), v -> Double.toString(v));

            case BOOLEAN:
                return Iterables.transform(value.asBooleanArray(), v -> Boolean.toString(v));

            case DECIMAL:
                return Iterables.transform(value.asDecimalArray(), v -> v.toString());
        }
        throw new IllegalArgumentException("Invalid type: " + value.getType());
    }

    private static String getAsString(Value value) {
        switch (value.getType()) {
            case STRING:
            case BINARY:
            case DATE:
            case NAME:
            case PATH:
            case REFERENCE:
            case WEAK_REFERENCE:
            case URI:
                return value.asStringValue();

            case LONG:
                return Long.toString(value.asLongValue());

            case DOUBLE:
                return Double.toString(value.asDoubleValue());

            case BOOLEAN:
                return Boolean.toString(value.asBooleanValue());

            case DECIMAL:
                return value.asDecimalValue().toString();
        }
        throw new IllegalArgumentException("Invalid type: " + value.getType());
    }

    @Override
    public void close() throws IOException {
        try {
            jedis.close();
        } catch (JedisException e) {
            throw new IOException(e);
        }
    }

    private RedisID generateNewId() throws IOException {
        try {
            long id = jedis.incr("seq_id");
            return new RedisID(id);
        } catch (JedisException e) {
            throw new IOException(e);
        }
    }

}
