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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.plugins.document.bulk.DocumentUpdateHistory.UpdateStats;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;

public class DocumentUpdateHistoryTest {

    @Test
    public void testCounts() {
        DocumentUpdateHistory history = new DocumentUpdateHistory(MINUTES.toMillis(1), Clock.SIMPLE);

        int success = 0;
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            if (random.nextBoolean()) {
                success++;
                history.add(true);
            } else {
                history.add(false);
            }
        }

        UpdateStats stats = history.getCounts();
        assertEquals(success, stats.getSuccessCount());
        assertEquals(100 - success, stats.getFailureCount());
        assertEquals(100, stats.getTotalCounts());
        assertEquals(((double) success) / 100, stats.getSuccessRatio(), 0.001);
    }

    @Test
    public void testConcurrentUpdates() throws InterruptedException {
        final DocumentUpdateHistory history = new DocumentUpdateHistory(MINUTES.toMillis(1), Clock.SIMPLE);

        final AtomicInteger success = new AtomicInteger();
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    Random random = new Random();
                    for (int i = 0; i < 100; i++) {
                        if (random.nextBoolean()) {
                            success.incrementAndGet();
                            history.add(true);
                        } else {
                            history.add(false);
                        }
                    }
                }
            });
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        UpdateStats stats = history.getCounts();
        assertEquals(success.get(), stats.getSuccessCount());
        assertEquals(1000 - success.get(), stats.getFailureCount());
        assertEquals(1000, stats.getTotalCounts());
        assertEquals(((double) success.get()) / 1000, stats.getSuccessRatio(), 0.001);
    }

    @Test
    public void testTtl() throws InterruptedException {
        Clock.Virtual clock = new Clock.Virtual();
        DocumentUpdateHistory history = new DocumentUpdateHistory(MINUTES.toMillis(60), clock);

        for (int i = 0; i < 5; i++) {
            history.add(true);
            history.add(false);
        }

        clock.waitUntil(clock.getTime() + MINUTES.toMillis(30));

        for (int i = 0; i < 3; i++) {
            history.add(true);
            history.add(false);
        }

        UpdateStats stats = history.getCounts();
        assertEquals(8, stats.getSuccessCount());
        assertEquals(8, stats.getFailureCount());

        clock.waitUntil(clock.getTime() + MINUTES.toMillis(30));

        stats = history.getCounts();
        assertEquals(3, stats.getSuccessCount());
        assertEquals(3, stats.getFailureCount());

        clock.waitUntil(clock.getTime() + MINUTES.toMillis(30));

        stats = history.getCounts();
        assertEquals(0, stats.getSuccessCount());
        assertEquals(0, stats.getFailureCount());
    }

    @Test
    public void testOutdated() throws InterruptedException {
        Clock.Virtual clock = new Clock.Virtual();
        DocumentUpdateHistory history = new DocumentUpdateHistory(MINUTES.toMillis(60), clock);

        for (int i = 0; i < 5; i++) {
            history.add(true);
            history.add(false);
        }

        clock.waitUntil(clock.getTime() + MINUTES.toMillis(30));

        for (int i = 0; i < 3; i++) {
            history.add(true);
            history.add(false);
        }

        assertFalse(history.isOutdated());

        clock.waitUntil(clock.getTime() + MINUTES.toMillis(30));

        assertFalse(history.isOutdated());

        clock.waitUntil(clock.getTime() + MINUTES.toMillis(30));

        assertTrue(history.isOutdated());
    }

}
