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
package org.apache.jackrabbit.oak.plugins.document.locks;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import com.google.common.util.concurrent.Striped;

public class StripedNodeDocumentLocks implements NodeDocumentLocks {

    private static final String ROOT = Utils.getIdFromPath("/");

    /**
     * Locks to ensure cache consistency on reads, writes and invalidation.
     */
    private final Striped<Lock> locks = Striped.lock(4096);
    private final Lock rootLock = Striped.lock(1).get(ROOT);

    /**
     * Counts how many times {@link TreeLock}s were acquired.
     */
    private volatile AtomicLong lockAcquisitionCounter;

    @Override
    public Lock acquire(String key) {
        Lock lock = ROOT.equals(key) ? rootLock : locks.get(key);
        lock.lock();
        return lock;
    }

    @Override
    public Lock acquire(Collection<String> keys) {
        List<Lock> lockList = new ArrayList<Lock>();
        if (keys.contains(ROOT)) {
            lockList.add(rootLock);
        }
        for (Lock l : locks.bulkGet(filter(keys, not(equalTo(ROOT))))) {
            lockList.add(l);
        }
        Lock lock = new BulkLock(lockList);
        lock.lock();
        return lock;
    }

    public void resetLockAcquisitionCount() {
        lockAcquisitionCounter = new AtomicLong();
    }

    public long getLockAcquisitionCount() {
        if (lockAcquisitionCounter == null) {
            throw new IllegalStateException("The counter hasn't been initialized");
        }
        return lockAcquisitionCounter.get();
    }
}
