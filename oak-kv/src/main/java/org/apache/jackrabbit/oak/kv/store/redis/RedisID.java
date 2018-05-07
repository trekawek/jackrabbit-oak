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

import java.util.Objects;
import java.util.UUID;

public class RedisID implements ID {

    private final UUID uuid;

    RedisID(String uuid) {
        this.uuid = UUID.fromString(uuid);
    }

    RedisID(UUID uuid) {
        this.uuid = uuid;
    }

    static RedisID newRandomID() {
        return new RedisID(UUID.randomUUID());
    }

    UUID getUUID() {
        return uuid;
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
        return equals((RedisID) o);
    }

    private boolean equals(RedisID o) {
        return Objects.equals(uuid, o.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }

    @Override
    public String toString() {
        return uuid.toString();
    }

}
