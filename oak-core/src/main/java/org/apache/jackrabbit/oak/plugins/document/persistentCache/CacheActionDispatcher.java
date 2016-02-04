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

import static com.google.common.collect.Multimaps.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

public class CacheActionDispatcher implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CacheActionDispatcher.class);

    /**
     * What's the length of the queue.
     */
    static final int MAX_SIZE = 1024;

    /**
     * How many actions remove once the queue is longer than {@link #MAX_SIZE}.
     */
    static final int ACTIONS_TO_REMOVE = 256;

    final BlockingQueue<CacheAction<?, ?>> queue = new ArrayBlockingQueue<CacheAction<?, ?>>(MAX_SIZE * 2);

    private volatile boolean isRunning = true;

    public void addAction(CacheAction<?, ?> action) {
        if (queue.size() >= MAX_SIZE) {
            cleanTheQueue();
        }
        queue.offer(action);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void cleanTheQueue() {
        List<CacheAction> removed = removeOldest();
        for (Entry<CacheWriteQueue, Collection<CacheAction>> e : groupByOwner(removed).entrySet()) {
            CacheWriteQueue owner = e.getKey();
            Collection<CacheAction> actions = e.getValue();
            owner.cancelAll((Iterable) actions);
        }
    }

    @SuppressWarnings("rawtypes")
    private Map<CacheWriteQueue, Collection<CacheAction>> groupByOwner(List<CacheAction> actions) {
        return index(actions, new Function<CacheAction, CacheWriteQueue>() {
            @Override
            public CacheWriteQueue apply(CacheAction input) {
                return input.getOwner();
            }
        }).asMap();
    }

    @SuppressWarnings("rawtypes")
    private List<CacheAction> removeOldest() {
        List<CacheAction> removed = new ArrayList<CacheAction>();
        while (queue.size() > MAX_SIZE - ACTIONS_TO_REMOVE) {
            CacheAction toBeCanceled = queue.poll();
            if (toBeCanceled == null) {
                break;
            } else {
                removed.add(toBeCanceled);
            }
        }
        return removed;
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                CacheAction<?, ?> action = queue.poll(10, TimeUnit.MILLISECONDS);
                if (action != null && isRunning) {
                    action.execute();
                }
            } catch (InterruptedException e) {
                LOG.debug("Interrupted the queue.poll()", e);
            }
        }
    }

    public void stop() {
        isRunning = false;
    }
}