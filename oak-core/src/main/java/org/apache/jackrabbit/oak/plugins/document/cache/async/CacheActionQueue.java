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
package org.apache.jackrabbit.oak.plugins.document.cache.async;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

public class CacheActionQueue implements Runnable {

    private static final int MAX_SIZE = 1024;

    private static final Logger LOG = LoggerFactory.getLogger(CacheActionQueue.class);

    private final BlockingQueue<CacheAction> queue = new ArrayBlockingQueue<CacheAction>(MAX_SIZE);

    private final Cache<CacheValue, NodeDocument> cache;

    private final ConcurrentHashMap<String, AtomicInteger> counters = new ConcurrentHashMap<String, AtomicInteger>();

    private volatile boolean isRunning = true;

    public CacheActionQueue(Cache<CacheValue, NodeDocument> cache) {
        this.cache = cache;
    }

    public synchronized void addAction(CacheAction action) {
        incrementCounters(action.affectedKeys());
        if (!queue.offer(action)) {
            queue.clear();
            Set<String> waitingKeys = new HashSet<String>(counters.keySet());
            counters.clear();
            addAction(new InvalidateAllAction(waitingKeys));
        }
    }

    public synchronized NodeDocument get(String key) {
        if (counters.containsKey(key)) {
            return null;
        } else {
            return cache.getIfPresent(key);
        }
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                CacheAction action = queue.poll(10, TimeUnit.MILLISECONDS);
                if (action != null) {
                    action.execute(cache);
                    decrementCounters(action.affectedKeys());
                }
            } catch (InterruptedException e) {
                LOG.debug("Interrupted the queue.poll()", e);
            }
        }
    }

    public void stop() {
        isRunning = false;
    }

    private void incrementCounters(Iterable<String> keys) {
        for (String key : keys) {
            counters.getOrDefault(key, new AtomicInteger()).incrementAndGet();
        }
    }

    private void decrementCounters(Iterable<String> keys) {
        for (String key : keys) {
            if (counters.get(key).decrementAndGet() == 0) {
                counters.remove(key);
            }
        }
    }
}
