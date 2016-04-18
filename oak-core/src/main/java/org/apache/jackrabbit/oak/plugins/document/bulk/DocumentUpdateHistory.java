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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.stats.Clock;

/**
 * The class contains a history of successful and failed bulk updates for a
 * single document.
 */
public class DocumentUpdateHistory {

    private final List<Long> successes = new ArrayList<Long>();

    private final List<Long> failures = new ArrayList<Long>();

    private final long ttlMillis;

    private final Clock clock;

    private volatile UpdateStats stats = new UpdateStats(0, 0);

    private volatile long lastUpdate;

    private volatile long earliestSuccess = Long.MAX_VALUE;

    private volatile long earliestFailure = Long.MAX_VALUE;

    /**
     * Create the new DocumentUpdateHistory
     *
     * @param ttlMillis how long should entries exists in the history
     * @param clock
     */
    public DocumentUpdateHistory(long ttlMillis, Clock clock) {
        this.clock = clock;
        this.ttlMillis = ttlMillis;
    }

    /**
     * Add new entry to the document history
     *
     * @param success {@code true} for a successful operation, {@code false} for failure
     */
    public void add(boolean success) {
        long time = clock.getTime();

        if (success) {
            synchronized (successes) {
                successes.add(time);
                if (earliestSuccess > time) {
                    earliestSuccess = time;
                }
            }
        } else {
            synchronized (failures) {
                failures.add(time);
                if (earliestFailure > time) {
                    earliestFailure = time;
                }
            }
        }
        clean(time);

        if (lastUpdate < time) {
            lastUpdate = time;
        }

        stats = null;
    }

    private void clean(long timeNow) {
        long deadline = timeNow - ttlMillis;
        boolean modified = false;
        if (deadline > earliestSuccess) {
            synchronized (successes) {
                earliestSuccess = removeSmallerThan(successes, deadline);
                modified = true;
            }
        }
        if (deadline > earliestFailure) {
            synchronized (failures) {
                earliestFailure = removeSmallerThan(failures, deadline);
                modified = true;
            }
        }
        if (modified) {
            stats = null;
        }
    }

    /**
     * Remove entries smaller than the {@code minValue}
     *
     * @param list to process
     * @param minValue the limiting value
     *
     * @return the smallest value not removed from the queue or
     *         {@link Long#MAX_VALUE} if the resulting list is empty
     */
    private static long removeSmallerThan(List<Long> list, long minValue) {
        Iterator<Long> it = list.iterator();
        long smallestValue = Long.MAX_VALUE;
        while (it.hasNext()) {
            long v = it.next();
            if (v < minValue) {
                it.remove();
            } else if (v < smallestValue) {
                smallestValue = v;
            }
        }
        return smallestValue;
    }

    /**
     * Return the update stats.
     *
     * @return the number of successful and failed updates
     */
    public UpdateStats getCounts() {
        clean(clock.getTime());

        UpdateStats currentStats = stats;
        if (currentStats == null) {
            int successCount = 0;
            int failureCount = 0;
            synchronized (successes) {
                successCount = successes.size();
            }
            synchronized (failures) {
                failureCount = failures.size();
            }
            currentStats = new UpdateStats(successCount, failureCount);
        }
        stats = currentStats;
        return currentStats;
    }

    /**
     * Check whether the history contains only out-dated entries
     *
     * @return {@code true} if the history no longer contains entries younger than {@link #ttlMillis}
     */
    public boolean isOutdated() {
        return lastUpdate < clock.getTime() - ttlMillis;
    }

    public class UpdateStats {

        private final int successCount;

        private final int failureCount;

        public UpdateStats(int successCount, int failureCount) {
            this.successCount = successCount;
            this.failureCount = failureCount;
        }

        /**
         * @return the number of successful updates
         */
        public int getSuccessCount() {
            return successCount;
        }

        /**
         * @return the number of failed updates
         */
        public int getFailureCount() {
            return failureCount;
        }

        /**
         * @return the total number of updates
         */
        public int getTotalCounts() {
            return successCount + failureCount;
        }

        /**
         * @return the number between 0 and 1 describing the ratio of successful
         *         updates or {@link Double#NaN} if there were no updates
         */
        public double getSuccessRatio() {
            long total = getTotalCounts();
            if (total == 0) {
                return Double.NaN;
            } else {
                return ((double) successCount) / total;
            }
        }

        @Override
        public String toString() {
            return String.format("UpdateStats[success=%d,failure=%d,total=%d,ratio=%.2f%%]", successCount, failureCount,
                    getTotalCounts(), getSuccessRatio() * 100);
        }
    }
}
