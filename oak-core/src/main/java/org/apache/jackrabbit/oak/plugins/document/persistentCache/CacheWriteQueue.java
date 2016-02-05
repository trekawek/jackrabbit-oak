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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A fronted for the {@link CacheActionDispatcher} creating actions and maintaining their state.
 *
 * @param <K> key type
 * @param <V> value type
 */
class CacheWriteQueue<K, V> {

    private final CacheActionDispatcher dispatcher;

    private final PersistentCache cache;

    private final MultiGenerationMap<K, V> map;

    final Map<K, Integer> counters = new HashMap<K, Integer>();

    final Set<K> waitsForInvalidation = new HashSet<K>();

    CacheWriteQueue(CacheActionDispatcher dispatcher, PersistentCache cache, MultiGenerationMap<K, V> map) {
        this.dispatcher = dispatcher;
        this.cache = cache;
        this.map = map;
    }

    /**
     * Add new invalidate action.
     *
     * @param keys to be invalidated
     */
    void addInvalidate(Iterable<K> keys) {
        synchronized(this) {
            for (K key : keys) {
                incrementCounter(key);
                waitsForInvalidation.add(key);
            }
        }
        dispatcher.add(new InvalidateCacheAction<K, V>(this, keys));
    }

    /**
     * Add new put action
     *
     * @param key to be put to cache
     * @param value to be put to cache
     */
    void addPut(K key, V value) {
        synchronized(this) {
            incrementCounter(key);
            waitsForInvalidation.remove(key);
        }
        dispatcher.add(new PutToCacheAction<K, V>(this, key, value));
    }

    /**
     * Check if the last action added for this key was invalidate
     *
     * @param key to check 
     * @return {@code true} if the last added action was invalidate
     */
    synchronized boolean waitsForInvalidation(K key) {
        return waitsForInvalidation.contains(key);
    }

    /**
     * Remove the action state when it's finished or cancelled.
     *
     * @param key to be removed
     */
    synchronized void remove(K key) {
        Integer counter = counters.get(key) - 1;
        if (counter == 0) {
            counters.remove(key);
            waitsForInvalidation.remove(key);
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
}
