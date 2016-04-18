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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.jackrabbit.oak.plugins.document.bulk.DocumentUpdateHistory.UpdateStats;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of the {@link BulkOperationStrategy} uses historic
 * results of the bulk updates to find the documents which conflicts too often
 * to be included in the batch operations.
 * <p>
 * The algorithm is as follows: if the document was included in at least
 * {@link #minAttempts} bulk operations during the last {@link #ttlMillis} and
 * only less than {@link #minSuccessRatio} succeeded, then it should be excluded
 * from the bulk operations.
 */
public class HistoricBulkOperationStrategy implements BulkOperationStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(HistoricBulkOperationStrategy.class);

    final ConcurrentMap<String, DocumentUpdateHistory> histories = new ConcurrentHashMap<String, DocumentUpdateHistory>();

    private final Clock clock;

    /**
     * How long should the strategy remember about bulk update results.
     */
    private final long ttlMillis;

    /**
     * How many update attempts are required to run the strategy.
     */
    private final long minAttempts;

    /**
     * What's the minimum success ratio to include the update in the bulk operation.
     */
    private final double minSuccessRatio;

    public HistoricBulkOperationStrategy(Clock clock, long ttlMillis, long minAttempts, double minSuccessRatio) {
        this.clock = clock;
        this.ttlMillis = ttlMillis;
        this.minAttempts = minAttempts;
        this.minSuccessRatio = minSuccessRatio;
    }

    public HistoricBulkOperationStrategy() {
        this(Clock.SIMPLE);
    }

    public HistoricBulkOperationStrategy(Clock clock) {
        this(clock, MINUTES.toMillis(60), 20, 0.5d);
    }

    @Override
    public boolean apply(String id) {
        DocumentUpdateHistory history = histories.get(id);
        if (history == null) {
            LOG.debug("{} will be included in the batch update. No stats gathered", id);
            return true;
        }

        if (history.isOutdated()) {
            removeOutdated(id, history);
        }

        UpdateStats counts = history.getCounts();
        if (counts.getTotalCounts() < minAttempts) {
            LOG.debug("{} will be included in the batch update. Not enough stats gathered: {}", id, counts);
            return true;
        }
        if (counts.getSuccessRatio() >= minSuccessRatio) {
            LOG.debug("{} will be included in the batch update. Stats: {}", id, counts);
            return true;
        } else {
            LOG.debug("{} won't be included in the batch update. Stats: {}", id, counts);
            return false;
        }
    }

    @Override
    public void updateConflicted(String id) {
        update(id, false);
    }

    @Override
    public void updateApplied(String id) {
        update(id, true);
    }

    private void update(String id, boolean success) {
        DocumentUpdateHistory history = histories.get(id);
        if (history == null) {
            synchronized (histories) { // synchronized to avoid conflict with #removeOutdated
                history = histories.get(id);
                if (history == null) {
                    DocumentUpdateHistory newHistory = new DocumentUpdateHistory(ttlMillis, clock);
                    newHistory.add(success);
                    histories.put(id, newHistory);
                    return;
                }
            }
        }
        history.add(success);
    }

    private void removeOutdated(String id, DocumentUpdateHistory history) {
        synchronized (histories) {
            if (history.isOutdated()) {
                histories.remove(id);
            }
        }
    }
}
