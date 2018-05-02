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

package org.apache.jackrabbit.oak.kv.store.memory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Store;
import org.apache.jackrabbit.oak.kv.store.Value;

public class MemoryStore implements Store {

    private final AtomicLong sequence = new AtomicLong();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<String, ID> tags = new HashMap<>();

    private final Map<ID, Node> nodes = new HashMap<>();

    @Override
    public ID getTag(String tag) {
        lock.readLock().lock();
        try {
            return tags.get(tag);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void putTag(String tag, ID id) {
        lock.writeLock().lock();
        try {
            tags.put(tag, id);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void deleteTag(String tag) {
        lock.writeLock().lock();
        try {
            tags.remove(tag);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Node getNode(ID id) {
        lock.readLock().lock();
        try {
            return nodes.get(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ID putNode(Map<String, Value> properties, Map<String, ID> children) {
        ID id = new MemoryID(sequence.getAndIncrement());

        lock.writeLock().lock();
        try {
            nodes.put(id, new MemoryNode(properties, children));
        } finally {
            lock.writeLock().unlock();
        }

        return id;
    }

}
