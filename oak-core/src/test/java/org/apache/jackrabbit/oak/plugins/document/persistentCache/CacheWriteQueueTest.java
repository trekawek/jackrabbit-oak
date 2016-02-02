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

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheWriteQueue.MAX_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

public class CacheWriteQueueTest {

    @Test
    public void testMaxQueueSize() {
        CacheWriteQueue queue = new CacheWriteQueue();
        for (int i = 0; i < MAX_SIZE + 10; i++) {
            queue.addAction(createWriteAction(Integer.toString(i)));
        }
        assertEquals(MAX_SIZE, queue.queue.size());
        assertEquals("10", queue.queue.peek().toString());
    }

    @Test
    public void testQueue() throws InterruptedException {
        final int threads = 5;
        final int actionsPerThread = 100;

        final CacheWriteQueue queue = new CacheWriteQueue();
        Thread queueThread = new Thread(queue);
        queueThread.start();

        List<DummyCacheWriteAction> allActions = new ArrayList<DummyCacheWriteAction>();
        List<Thread> producerThreads = new ArrayList<Thread>();
        for (int i = 0; i < threads; i++) {
            final List<DummyCacheWriteAction> threadActions = new ArrayList<DummyCacheWriteAction>();
            for (int j = 0; j < actionsPerThread; j++) {
                DummyCacheWriteAction action = new DummyCacheWriteAction(String.format("%d_%d", i, j));
                threadActions.add(action);
                allActions.add(action);
            }
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (DummyCacheWriteAction a : threadActions) {
                        queue.addAction(a);
                    }
                }
            });
            producerThreads.add(t);
        }

        for (Thread t : producerThreads) {
            t.start();
        }
        for (Thread t : producerThreads) {
            t.join();
        }

        long start = currentTimeMillis();
        while (!allActions.isEmpty()) {
            Iterator<DummyCacheWriteAction> it = allActions.iterator();
            while (it.hasNext()) {
                if (it.next().finished) {
                    it.remove();
                }
            }
            if (currentTimeMillis() - start > 5000) {
                fail("Following actions hasn't been executed: " + allActions);
            }
        }

        queue.stop();
        queueThread.join();
        assertFalse(queueThread.isAlive());
    }

    private static DummyCacheWriteAction createWriteAction(String id) {
        return new DummyCacheWriteAction(id);
    }

    @SuppressWarnings("rawtypes")
    private static class DummyCacheWriteAction extends CacheWriteAction {

        private final String id;

        private final Random random;

        private volatile boolean finished;

        @SuppressWarnings("unchecked")
        DummyCacheWriteAction(String id) {
            super(null, null, null, true);
            this.id = id;
            this.random = new Random();
        }

        @Override
        public void run() {
            try {
                sleep(random.nextInt(10));
            } catch (InterruptedException e) {
                fail("Interrupted");
            }
            finished = true;
        }

        @Override
        public String toString() {
            return id;
        }
    }
}
