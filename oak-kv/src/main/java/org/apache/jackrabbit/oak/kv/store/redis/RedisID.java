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

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class RedisID implements ID {

    private final long id;

    RedisID(byte[] serializedId) {
        id = ByteBuffer.wrap(serializedId).asLongBuffer().get();
    }

    RedisID(long id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        return id == ((RedisID) o).id;
    }

    @Override
    public int hashCode() {
        return Long.valueOf(id).hashCode();
    }

    @Override
    public String toString() {
        return Long.toHexString(id);
    }

    byte[] getAsBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1);
        buffer.asLongBuffer().put(id);
        return buffer.array();
    }

    byte[] getPropertyListKey() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1);
        buffer.asLongBuffer().put(id);
        buffer.put((byte) 0);
        return buffer.array();
    }

    byte[] getChildrenHashKey() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1);
        buffer.asLongBuffer().put(id);
        buffer.put((byte) 1);
        return buffer.array();
    }

    byte[] getPropertyKey(long propertyIndex) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + 1);
        LongBuffer longBuffer = buffer.asLongBuffer().put(id);
        buffer.put((byte) 2);
        longBuffer.put(propertyIndex);
        return buffer.array();
    }
}
