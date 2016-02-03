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

public class CacheWriteQueue<K, V> {

    private final CacheActionDispatcher dispatcher;

    private final NodeCache<K, V> nodeCache;

    private final Map<K, Integer> toBeInvalidated = new HashMap<K, Integer>();

    private final Map<K, Integer> toBePut = new HashMap<K, Integer>();

    private final Map<K, OperationType> finalOp = new HashMap<K, OperationType>();

    public CacheWriteQueue(CacheActionDispatcher dispatcher, NodeCache<K, V> nodeCache) {
        this.dispatcher = dispatcher;
        this.nodeCache = nodeCache;
    }

    public void addWrite(K key, V value, boolean broadcast) {
        if (increaseCounter(key, value)) {
            dispatcher.addAction(new CacheWriteAction(key, value, broadcast));
        }
    }

    public synchronized boolean waitsForInvalidation(K key) {
        return finalOp.get(key) == OperationType.INVALIDATE;
    }

    private synchronized boolean increaseCounter(K key, V value) {
        OperationType type = OperationType.getFromValue(value);
        if (type == finalOp.get(key)) {
            return false;
        }
        Map<K, Integer> map = getMap(type);
        Integer counter = map.get(key);
        if (counter == null) {
            counter = 0;
        }
        map.put(key, ++counter);
        finalOp.put(key, type);
        return true;
    }

    private synchronized void decreaseCounter(K key, V value) {
        OperationType type = OperationType.getFromValue(value);
        Map<K, Integer> map = getMap(type);
        Integer counter = map.get(key) - 1;
        map.put(key, counter);

        if (counter == 0) {
            clean(key);
        }
    }

    private synchronized boolean isFinalOperation(K key, V value) {
        OperationType type = OperationType.getFromValue(value);
        OperationType lastAddedOp = finalOp.get(key);
        return type == lastAddedOp && getMap(type).get(key) == 1;
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

    private class CacheWriteAction implements CacheAction {

        private final K key;

        private final V value;

        private final boolean broadcast;

        private CacheWriteAction(K key, V value, boolean broadcast) {
            this.key = key;
            this.value = value;
            this.broadcast = broadcast;
        }

        @Override
        public void execute() {
            if (isFinalOperation(key, value)) {
                nodeCache.syncWrite(key, value, broadcast);
            }
            decreaseCounter(key, value);
        }

        @Override
        public void cancel() {
            decreaseCounter(key, value);
        }
    }

    private static enum OperationType {
        INVALIDATE, PUT;

        private static OperationType getFromValue(Object value) {
            return value == null ? INVALIDATE : PUT;
        }
    }
}
