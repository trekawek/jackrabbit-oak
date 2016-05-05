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
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MonitoringBackgroundObserverTest {

    private MonitoringBackgroundObserver mbo;

    @Before
    public void createObserver() {
        mbo = new MonitoringBackgroundObserver(Executors.newSingleThreadExecutor());
    }

    @Test(timeout = 10)
    public void testNoProcessing() throws InterruptedException {
        mbo.waitUntilProcessingIsFinished();
    }

    @Test
    public void testWaitForFinish() throws InterruptedException {
        final AtomicBoolean observerProcessed = new AtomicBoolean();
        mbo.addObserver(new Observer() {
            @Override
            public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
                sleep(50);
                observerProcessed.set(true);
            }
        });
        mbo.contentChanged(null, null);

        mbo.waitUntilProcessingIsFinished();
        assertTrue(observerProcessed.get());
    }

    @Test
    public void testProcessOnlyLatest() throws InterruptedException {
        final List<String> processed = new ArrayList<String>();
        mbo.addObserver(new Observer() {
            @Override
            public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
                sleep(50);
                processed.add(info.getSessionId());
            }
        });
        mbo.contentChanged(null, new CommitInfo("c1", ""));
        mbo.contentChanged(null, new CommitInfo("c2", "")); // this should be ignored
        mbo.contentChanged(null, new CommitInfo("c3", "")); // this will be processed
        mbo.waitUntilProcessingIsFinished();

        assertEquals(asList("c1", "c3"), processed);
    }

    @Test
    public void testXyz() throws InterruptedException {
        final List<String> processed = new ArrayList<String>();
        mbo.addObserver(new Observer() {
            @Override
            public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
                sleep(100);
                processed.add(info.getSessionId());
            }
        });
        mbo.contentChanged(null, new CommitInfo("c1", ""));
        mbo.contentChanged(null, new CommitInfo("c2", "")); // this should be ignored

        // add a new content after 50 millis, while we'll be waiting
        new Thread(new Runnable() {
            @Override
            public void run() {
                sleep(50);
                mbo.contentChanged(null, new CommitInfo("c3", "")); // this will be ignored
                mbo.contentChanged(null, new CommitInfo("c4", "")); // this will be processed
            }
        }).start();

        // wait for the c2 processing to be finished - in the meantime, the c3 and c4
        // will be added
        mbo.waitUntilProcessingIsFinished();

        assertEquals(asList("c1", "c4"), processed);
    }

    private static void sleep(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
