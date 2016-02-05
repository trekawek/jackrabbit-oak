/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.util.HashMap;
import java.util.Map;

class CacheWriteQueue<K, V> {

    private final CacheActionDispatcher dispatcher;

    private final PersistentCache cache;

    private final MultiGenerationMap<K, V> map;

    final Map<K, Integer> counters = new HashMap<K, Integer>();

    final Map<K, OperationType> finalOp = new HashMap<K, OperationType>();

    CacheWriteQueue(CacheActionDispatcher dispatcher, PersistentCache cache, MultiGenerationMap<K, V> map) {
        this.dispatcher = dispatcher;
        this.cache = cache;
        this.map = map;
    }

    void addInvalidate(Iterable<K> keys) {
        synchronized(this) {
            for (K key : keys) {
                incrementCounter(key);
                finalOp.put(key, OperationType.INVALIDATE);
            }
        }
        dispatcher.add(new InvalidateCacheAction<K, V>(this, keys));
    }

    void addPut(K key, V value) {
        synchronized(this) {
            incrementCounter(key);
            finalOp.put(key, OperationType.PUT);
        }
        dispatcher.add(new PutToCacheAction<K, V>(this, key, value));
    }

    synchronized boolean hasInvalidateOnTail(K key) {
        return finalOp.get(key) == OperationType.INVALIDATE;
    }

    synchronized void remove(K key) {
        Integer counter = counters.get(key) - 1;
        if (counter == 0) {
            counters.remove(key);
            finalOp.remove(key);
        } else {
            counters.put(key, counter);
        }
    }

    PersistentCache getCache() {
        return cache;
    }

    MultiGenerationMap<K, V> getMap() {
        return map;
    }

    private void incrementCounter(K key) {
        Integer counter = counters.get(key);
        if (counter == null) {
            counter = 0;
        }
        counters.put(key, ++counter);
    }

    private static enum OperationType {
        INVALIDATE, PUT;
    }
}
