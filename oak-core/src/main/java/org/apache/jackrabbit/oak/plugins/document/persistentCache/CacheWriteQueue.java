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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;

class CacheWriteQueue<K, V> {

    private final CacheActionDispatcher dispatcher;

    private final PersistentCache cache;

    private final MultiGenerationMap<K, V> map;

    final Map<K, Integer> toBeInvalidated = new HashMap<K, Integer>();

    final Map<K, Integer> toBePut = new HashMap<K, Integer>();

    final Map<K, OperationType> finalOp = new HashMap<K, OperationType>();

    CacheWriteQueue(CacheActionDispatcher dispatcher, PersistentCache cache, MultiGenerationMap<K, V> map) {
        this.dispatcher = dispatcher;
        this.cache = cache;
        this.map = map;
    }

    void addWrite(K key, V value) {
        incrementCounter(key, value);
        dispatcher.addAction(new CacheWriteAction<K, V>(this, key, value));
    }

    boolean waitsForInvalidation(K key) {
        return finalOp.get(key) == OperationType.INVALIDATE;
    }

    void cancelAll(Iterable<CacheAction<K, V>> actions) {
        List<K> keys = new ArrayList<K>();
        for (CacheAction<K, V> action : actions) {
            action.cancel();
            Iterables.addAll(keys, action.getAffectedKeys());
        }
        
        for (K key : keys) {
            incrementCounter(key, null);
        }
        dispatcher.addAction(new InvalidateAllCacheAction<K, V>(this, keys));
    }

    synchronized void decrementCounter(K key, V value) {
        OperationType type = OperationType.getFromValue(value);
        Map<K, Integer> map = getMap(type);
        Integer counter = map.get(key) - 1;
        map.put(key, counter);

        if (counter == 0) {
            clean(key);
        }
    }

    PersistentCache getCache() {
        return cache;
    }

    MultiGenerationMap<K, V> getMap() {
        return map;
    }

    private synchronized void incrementCounter(K key, V value) {
        OperationType type = OperationType.getFromValue(value);
        Map<K, Integer> map = getMap(type);
        Integer counter = map.get(key);
        if (counter == null) {
            counter = 0;
        }
        map.put(key, ++counter);
        finalOp.put(key, type);
    }

    private void clean(K key) {
        if (toBeInvalidated.containsKey(key) && toBeInvalidated.get(key) > 0) {
            return;
        }
        if (toBePut.containsKey(key) && toBePut.get(key) > 0) {
            return;
        }
        toBeInvalidated.remove(key);
        toBePut.remove(key);
        finalOp.remove(key);
    }

    private Map<K, Integer> getMap(OperationType type) {
        if (type == OperationType.INVALIDATE) {
            return toBeInvalidated;
        } else {
            return toBePut;
        }
    }

    private static enum OperationType {
        INVALIDATE, PUT;

        private static OperationType getFromValue(Object value) {
            return value == null ? INVALIDATE : PUT;
        }
    }
}
