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
 *
 */
package org.apache.jackrabbit.oak.segment.persistentcache;

import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CachingPersistenceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private SegmentNodeStorePersistence cachingPersistence;

    private SegmentNodeStorePersistence backendPersistence;

    private DiskCache cache;

    @Before
    public void setup() throws Exception {
        backendPersistence = createBackendPersistence();
        cache = new DiskCache(folder.newFolder());
        cachingPersistence = new CachingPersistence(cache, backendPersistence);
    }

    protected SegmentNodeStorePersistence createBackendPersistence() throws Exception {
        return new TarPersistence(folder.newFolder());
    }

    @After
    public void tearDown() throws IOException {
        cache.close();
    }

    @Test
    public void testWrittenSegmentsAddedToCache() throws Exception {
        byte[] data = getRandomData(1024);
        SegmentArchiveWriter writer = create(cachingPersistence, "data00000.tar");
        try {
            writer.writeSegment(1, 1, data, 0, data.length, 0, 0, false);
        } finally {
            writer.close();
        }

        cleanBackendPersistence();

        try (SegmentArchiveReader reader = open(cachingPersistence, "data00000.tar")) {
            assertTrue(reader.containsSegment(1, 1));
            assertEquals(ByteBuffer.wrap(data), reader.readSegment(1, 1));
        }
    }

    @Test
    public void testReadSegmentsAddedToCache() throws Exception {
        byte[] data = getRandomData(1024);
        SegmentArchiveWriter writer = create(backendPersistence, "data00000.tar");
        try {
            writer.writeSegment(1, 1, data, 0, data.length, 0, 0, false);
        } finally {
            writer.close();
        }

        try (SegmentArchiveReader reader = open(cachingPersistence, "data00000.tar")) {
            reader.readSegment(1, 1);
        }

        cleanBackendPersistence();

        try (SegmentArchiveReader reader = open(cachingPersistence, "data00000.tar")) {
            assertTrue(reader.containsSegment(1, 1));
            assertEquals(ByteBuffer.wrap(data), reader.readSegment(1, 1));
        }
    }

    @Test
    public void testWrittenSegmentsArePersisted() throws IOException {
        byte[] data = getRandomData(1024);
        SegmentArchiveWriter writer = create(cachingPersistence, "data00000.tar");
        try {
            writer.writeSegment(1, 1, data, 0, data.length, 0, 0, false);
        } finally {
            writer.close();
        }

        try (SegmentArchiveReader reader = open(backendPersistence, "data00000.tar")) {
            assertTrue(reader.containsSegment(1, 1));
            assertEquals(ByteBuffer.wrap(data), reader.readSegment(1, 1));
        }
    }

    // use the old cache with a new backend, to see if the segments will be read from the cache
    private void cleanBackendPersistence() throws Exception {
        List<String> archives = getManager(backendPersistence).listArchives();

        // recreate the backend persistence with the same archives, but empty
        backendPersistence = createBackendPersistence();
        for (String a : archives) {
            SegmentArchiveWriter writer = create(backendPersistence, a);
            writer.writeSegment(255, 255, new byte[10], 0, 10, 0, 0, false);
            writer.close();
        }
        cachingPersistence = new CachingPersistence(cache, backendPersistence);
    }

    private static SegmentArchiveManager getManager(SegmentNodeStorePersistence persistence) throws IOException {
        return persistence.createArchiveManager(true, new IOMonitorAdapter(), new FileStoreMonitorAdapter());
    }

    private static SegmentArchiveWriter create(SegmentNodeStorePersistence persistence, String name) throws IOException {
        return getManager(persistence).create(name);
    }

    private static SegmentArchiveReader open(SegmentNodeStorePersistence persistence, String name) throws IOException {
        return getManager(persistence).open(name);
    }

    private byte[] getRandomData(int length) {
        byte[] data = new byte[length];
        new Random().nextBytes(data);
        return data;
    }
}
