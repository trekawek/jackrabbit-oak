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
package org.apache.jackrabbit.oak.plugins.document.bulk;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.plugins.document.bulk.DocumentUpdateHistory.UpdateStats;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;

public class HistoricBulkOperationStrategyTest {

    @Test
    public void testBasicCase() {
        Clock.Virtual clock = new Clock.Virtual();
        BulkOperationStrategy strategy = new HistoricBulkOperationStrategy(clock, MINUTES.toMillis(1), 10, 0.5);
        update("xyz", strategy, true, 6);
        update("xyz", strategy, false, 4);
        assertTrue(strategy.apply("xyz"));

        update("xyz", strategy, false, 3);
        assertFalse(strategy.apply("xyz"));
    }

    @Test
    public void testMinimumAttempts() {
        Clock.Virtual clock = new Clock.Virtual();
        BulkOperationStrategy strategy = new HistoricBulkOperationStrategy(clock, MINUTES.toMillis(1), 10, 0.5);
        update("xyz", strategy, false, 9);
        assertTrue(strategy.apply("xyz"));

        update("xyz", strategy, false, 1);
        assertFalse(strategy.apply("xyz"));
    }

    @Test
    public void testTtl() {
        Clock.Virtual clock = new Clock.Virtual();
        BulkOperationStrategy strategy = new HistoricBulkOperationStrategy(clock, SECONDS.toMillis(60), 10, 0.5);
        update("xyz", strategy, false, 6);
        clock.waitUntil(clock.getTime() + SECONDS.toMillis(30));
        update("xyz", strategy, false, 4);

        assertFalse(strategy.apply("xyz"));

        clock.waitUntil(clock.getTime() + SECONDS.toMillis(30));
        assertTrue(strategy.apply("xyz"));
    }

    @Test
    public void testRemoveOutdated() {
        Clock.Virtual clock = new Clock.Virtual();
        HistoricBulkOperationStrategy strategy = new HistoricBulkOperationStrategy(clock, SECONDS.toMillis(60), 10, 0.5);
        update("xyz", strategy, false, 10);
        assertFalse(strategy.apply("xyz"));

        clock.waitUntil(clock.getTime() + SECONDS.toMillis(60));
        assertTrue(strategy.apply("xyz"));
        assertTrue(strategy.histories.isEmpty());
    }

    @Test
    public void testConcurrentUpdates() throws InterruptedException {
        Clock.Virtual clock = new Clock.Virtual();
        final HistoricBulkOperationStrategy strategy = new HistoricBulkOperationStrategy(clock, SECONDS.toMillis(60), 10, 0.5);

        final Map<String, AtomicInteger> successCounters = new HashMap<String, AtomicInteger>();
        final Map<String, AtomicInteger> updateCounters = new HashMap<String, AtomicInteger>();
        for (int i = 0; i < 10 ; i++) {
            successCounters.put("key" + i, new AtomicInteger());
            updateCounters.put("key" + i, new AtomicInteger());
        }

        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    Random random = new Random();
                    for (int i = 0; i < 1000; i++) {
                        String key = "key" + random.nextInt(10);
                        if (random.nextBoolean()) {
                            successCounters.get(key).incrementAndGet();
                            strategy.updateApplied(key);
                        } else {
                            strategy.updateConflicted(key);
                        }
                        updateCounters.get(key).incrementAndGet();
                    }
                }
            });
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        for (int i = 0; i < 10; i++) {
            String key = "key" + i;
            DocumentUpdateHistory history = strategy.histories.get(key);
            UpdateStats counts = history.getCounts();

            long s = successCounters.get(key).get();
            long t = updateCounters.get(key).get();
            assertEquals(s, counts.getSuccessCount());
            assertEquals(t, counts.getTotalCounts());
            assertEquals(((double) s / t) >= 0.5, strategy.apply(key));
        }
    }

    private void update(String id, BulkOperationStrategy strategy, boolean success, int times) {
        for (int i = 0; i < times; i++) {
            if (success) {
                strategy.updateApplied(id);
            } else {
                strategy.updateConflicted(id);
            }
        }
    }
}
