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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

/**
 * This observer wrapper runs processes the incoming content changes in the
 * background. There's no queue - if there are many content changes, only
 * the latest snapshot is passed to the configured observers.
 * <p>
 * The distinctive feature of this class is that it allows to wait until
 * the incoming changes are processed. One can call {@link #waitUntilProcessingIsFinished()}
 * method and it'll block until the underlying observers are up-to-date
 * with the repository state from the moment of calling this method. Any changes
 * introduced to the repository <i>after</i> the {@link #waitUntilProcessingIsFinished()}
 * is called will be ignored - in other words, the method is not interested
 * in processing the future repository updates.
 */
public class MonitoringBackgroundObserver implements Observer {

    private static final Logger log = LoggerFactory.getLogger(MonitoringBackgroundObserver.class);

    private final Executor executor;

    private final List<Observer> observers = new CopyOnWriteArrayList<Observer>();

    private final Deque<ProcessingState> processingCallbacks = new ArrayDeque<ProcessingState>();

    private final Clock clock = Clock.SIMPLE;

    private boolean updateInProgress;

    private ObservationEvent waitingEvent;

    public MonitoringBackgroundObserver(Executor executor) {
        this.executor = executor;
    }

    @Override
    public synchronized void contentChanged(@Nonnull final NodeState root, @Nullable CommitInfo info) {
        final ObservationEvent event = new ObservationEvent(root, info, startProcessing());
        if (updateInProgress) {
            waitingEvent = event;
        } else {
            updateInProgress = true;
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    processChange(event);
                }
            });
        }
    }

    public void addObserver(Observer observer) {
        observers.add(observer);
    }

    private void processChange(ObservationEvent event) {
        ObservationEvent nextEvent = event;

        while (nextEvent != null) {
            for (Observer o : observers) {
                o.contentChanged(nextEvent.root, nextEvent.commitInfo);
            }
            processingFinished(nextEvent.processingState);

            synchronized (this) {
                nextEvent = waitingEvent;
                waitingEvent = null;

                if (nextEvent == null) {
                    updateInProgress = false;
                }
            }
        }
    }

    private ProcessingState startProcessing() {
        synchronized (processingCallbacks) {
            ProcessingState newProcess = new ProcessingState();
            processingCallbacks.addLast(newProcess);
            return newProcess;
        }
    }

    private void processingFinished(ProcessingState process) {
        synchronized (processingCallbacks) {
            while (!processingCallbacks.isEmpty() && process.compareTo(processingCallbacks.peekFirst()) >= 0) {
                processingCallbacks.removeFirst().markAsDone();
            }
        }
    }

    public void waitUntilProcessingIsFinished() throws InterruptedException {
        ProcessingState monitor = null;
        synchronized (processingCallbacks) {
            if (!processingCallbacks.isEmpty()) {
                monitor = processingCallbacks.getLast();
            }
        }
        if (monitor != null) {
            monitor.waitUntilDone();
        }
    }

    private static class ObservationEvent {

        private final NodeState root;

        private final CommitInfo commitInfo;

        private final ProcessingState processingState;

        private ObservationEvent(NodeState root, CommitInfo commitInfo, ProcessingState processingState) {
            this.root = root;
            this.commitInfo = commitInfo;
            this.processingState = processingState;
        }
    }

    /**
     * This class represents processing a single event.
     */
    private class ProcessingState implements Comparable<ProcessingState> {

        /**
         * When the processing started (this value is unique and always increasing)
         */
        private final long startTime;

        /**
         * Whether this or any younger process has been finished.
         */
        private volatile boolean isDone;

        private ProcessingState() {
            long time;
            try {
                time = clock.getTimeIncreasing();
            } catch (InterruptedException e) {
                log.error("Can't get time from the clock", e);
                time = System.currentTimeMillis();
            }
            startTime = time;
        }

        @Override
        public int compareTo(ProcessingState o) {
            return Long.compare(startTime, o.startTime);
        }

        /**
         * Informs all the clients waiting in the {@link #waitUntilDone()} method
         * that the changes are already processed.
         */
        private void markAsDone() {
            synchronized (this) {
                isDone = true;
                notifyAll();
            }
        }

        /**
         * Wait until all changes are applied.
         *
         * @throws InterruptedException
         */
        private void waitUntilDone() throws InterruptedException {
            if (isDone) {
                return;
            }
            synchronized (this) {
                while (!isDone) {
                    wait();
                }
            }
        }
    }
}