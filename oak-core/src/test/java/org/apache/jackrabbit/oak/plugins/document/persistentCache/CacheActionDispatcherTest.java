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

import static com.google.common.collect.Iterables.size;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheActionDispatcher.ACTIONS_TO_REMOVE;
import static org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheActionDispatcher.MAX_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.mockito.Mockito;

public class CacheActionDispatcherTest {

    @Test
    public void testMaxQueueSize() {
        CacheActionDispatcher dispatcher = new CacheActionDispatcher();
        @SuppressWarnings({ "unchecked", "rawtypes" })
        CacheWriteQueue<String, Object> queue = new CacheWriteQueue(dispatcher, mock(PersistentCache.class), new MultiGenerationMap());

        for (int i = 0; i < MAX_SIZE + 10; i++) {
            dispatcher.add(createWriteAction(valueOf(i), queue));
        }
        assertEquals(MAX_SIZE - ACTIONS_TO_REMOVE + 10 + 1, dispatcher.queue.size());
        assertEquals(valueOf(ACTIONS_TO_REMOVE), dispatcher.queue.peek().toString());

        InvalidateCacheAction<?, ?> invalidateAction = null;
        for (CacheAction<?, ?> action : dispatcher.queue) {
            if (action instanceof InvalidateCacheAction) {
                invalidateAction = (InvalidateCacheAction<?, ?>) action;
            }
        }
        assertNotNull(invalidateAction);
        assertEquals(ACTIONS_TO_REMOVE, size(invalidateAction.getAffectedKeys()));
    }

    @Test
    public void testQueue() throws InterruptedException {
        final int threads = 5;
        final int actionsPerThread = 100;

        @SuppressWarnings("unchecked")
        final CacheWriteQueue<String, Object> queue = Mockito.mock(CacheWriteQueue.class);
        final CacheActionDispatcher dispatcher = new CacheActionDispatcher();
        Thread queueThread = new Thread(dispatcher);
        queueThread.start();

        List<DummyCacheWriteAction> allActions = new ArrayList<DummyCacheWriteAction>();
        List<Thread> producerThreads = new ArrayList<Thread>();
        for (int i = 0; i < threads; i++) {
            final List<DummyCacheWriteAction> threadActions = new ArrayList<DummyCacheWriteAction>();
            for (int j = 0; j < actionsPerThread; j++) {
                DummyCacheWriteAction action = new DummyCacheWriteAction(String.format("%d_%d", i, j), queue);
                threadActions.add(action);
                allActions.add(action);
            }
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (DummyCacheWriteAction a : threadActions) {
                        dispatcher.add(a);
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

        dispatcher.stop();
        queueThread.join();
        assertFalse(queueThread.isAlive());
    }

    private DummyCacheWriteAction createWriteAction(String id, CacheWriteQueue<String, Object> queue) {
        return new DummyCacheWriteAction(id, queue);
    }

    private class DummyCacheWriteAction implements CacheAction<String, Object> {

        private final CacheWriteQueue<String, Object> queue;

        private final String id;

        private final Random random;

        private volatile boolean finished;

        private DummyCacheWriteAction(String id, CacheWriteQueue<String, Object> queue) {
            this.queue = queue;
            this.id = id;
            this.random = new Random();
        }

        @Override
        public void execute() {
            try {
                sleep(random.nextInt(10));
            } catch (InterruptedException e) {
                fail("Interrupted");
            }
            finished = true;
        }

        @Override
        public void cancel() {
        }

        @Override
        public String toString() {
            return id;
        }

        @Override
        public Iterable<String> getAffectedKeys() {
            return Collections.singleton(id);
        }

        @Override
        public CacheWriteQueue<String, Object> getOwner() {
            return queue;
        }
    }
}
