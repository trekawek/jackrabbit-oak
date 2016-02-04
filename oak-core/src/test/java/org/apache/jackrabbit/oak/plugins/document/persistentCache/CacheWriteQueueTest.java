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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CacheWriteQueueTest {

    private CacheWriteQueue<String, Object> queue;

    private List<CacheAction> actions = Collections.synchronizedList(new ArrayList<CacheAction>());

    @Before
    public void initQueue() {
        actions.clear();

        CacheActionDispatcher dispatcher = new CacheActionDispatcher() {
            public void addAction(CacheAction action) {
                actions.add(action);
            }
        };

        PersistentCache cache = Mockito.mock(PersistentCache.class);
        queue = new CacheWriteQueue<String, Object>(dispatcher, cache, null);
    }

    @Test
    public void testCounters() throws InterruptedException {
        final int threadCount = 10;
        final int actionsPerThread = 50;

        final Map<String, AtomicInteger> invalidations = new HashMap<String, AtomicInteger>();
        final Map<String, AtomicInteger> updates = new HashMap<String, AtomicInteger>();
        for (int i = 0; i < 10; i++) {
            String key = "key_" + i;
            invalidations.put(key, new AtomicInteger());
            updates.put(key, new AtomicInteger());
        }

        final Random random = new Random();
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < actionsPerThread; j++) {
                        for (String key : invalidations.keySet()) {
                            if (random.nextBoolean()) {
                                queue.addWrite(key, null);
                                invalidations.get(key).incrementAndGet();
                            } else {
                                queue.addWrite(key, new Object());
                                updates.get(key).incrementAndGet();
                            }
                        }
                    }
                }
            });
            threads.add(t);
        }

        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        for (String key : invalidations.keySet()) {
            assertEquals(queue.toBeInvalidated.get(key).intValue(), invalidations.get(key).get());
            assertEquals(queue.toBePut.get(key).intValue(), updates.get(key).get());
        }

        for (CacheAction action : actions) {
            if (random.nextBoolean()) {
                action.execute();
            } else {
                action.cancel();
            }
        }

        assertTrue(queue.toBeInvalidated.isEmpty());
        assertTrue(queue.toBePut.isEmpty());
        assertTrue(queue.finalOp.isEmpty());
    }

    @Test
    public void testWaitsForInvalidation() {
        assertFalse(queue.waitsForInvalidation("key"));

        queue.addWrite("key", null);
        assertTrue(queue.waitsForInvalidation("key"));

        queue.addWrite("key", new Object());
        assertFalse(queue.waitsForInvalidation("key"));

        queue.addWrite("key", null);
        assertTrue(queue.waitsForInvalidation("key"));

        int i;
        for (i = 0; i < actions.size() - 1; i++) {
            actions.get(i).execute();
            assertTrue(queue.waitsForInvalidation("key"));
        }

        actions.get(i).execute();
        assertFalse(queue.waitsForInvalidation("key"));
    }

}
