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

import static java.util.Collections.singleton;

class CacheWriteAction<K, V> implements CacheAction<K, V> {

    private final PersistentCache cache;

    private final MultiGenerationMap<K, V> map;

    private final CacheWriteQueue<K, V> owner;

    private final K key;

    private final V value;

    CacheWriteAction(CacheWriteQueue<K, V> cacheWriteQueue, K key, V value) {
        this.owner = cacheWriteQueue;
        this.key = key;
        this.value = value;
        this.cache = cacheWriteQueue.getCache();
        this.map = cacheWriteQueue.getMap();
    }

    @Override
    public void execute() {
        try {
            cache.switchGenerationIfNeeded();
            if (map != null) {
                if (value == null) {
                    map.remove(key);
                } else {
                    map.put(key, value);
                }
            }
        } finally {
            decrement();
        }
    }

    @Override
    public void cancel() {
        decrement();
    }

    private void decrement() {
        owner.decrementCounter(key, value);
    }
    
    @Override
    public CacheWriteQueue<K, V> getOwner() {
        return owner;
    }

    @Override
    public Iterable<K> getAffectedKeys() {
        return singleton(key);
    }
}