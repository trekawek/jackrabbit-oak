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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CacheWriteQueue implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CacheWriteQueue.class);

    private static final int MAX_SIZE = 1024;

    private final BlockingQueue<CacheWriteAction<?,?>> queue = new ArrayBlockingQueue<CacheWriteAction<?,?>>(MAX_SIZE * 2);

    private volatile boolean isRunning = true;

    CacheWriteQueue() {
    }

    public void addAction(CacheWriteAction<?,?> action) {
        while (queue.size() > MAX_SIZE) {
            queue.poll();
        }
        queue.offer(action);
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                CacheWriteAction<?,?> action = queue.poll(10, TimeUnit.MILLISECONDS);
                if (action != null && isRunning) {
                    action.run();
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