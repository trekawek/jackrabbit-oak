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
package org.apache.jackrabbit.oak.remote.client.persistence;

import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ArchiveTailingReader implements SegmentArchiveReader {

    private static final Logger log = LoggerFactory.getLogger(ArchiveTailingReader.class);

    private final SegmentArchiveManager archiveManager;

    private final Map<String, SegmentArchiveReader> closedReaders = new LinkedHashMap<>();

    private final String tailFromArchive;

    private final CloudBlobDirectory segmentStoreDirectory;

    private SegmentTailingReader currentReader;

    public ArchiveTailingReader(SegmentArchiveManager archiveManager, String archiveName, CloudBlobDirectory segmentStoreDirectory) throws IOException {
        this.archiveManager = archiveManager;
        this.tailFromArchive = archiveName;
        this.segmentStoreDirectory = segmentStoreDirectory;
        updateReaders();
    }

    private List<String> listArchives() throws IOException {
        List<String> archives = archiveManager.listArchives();
        Collections.sort(archives);
        int i = archives.indexOf(tailFromArchive);
        if (i == -1) {
            i = 0;
        }
        return archives.subList(i, archives.size());
    }

    private synchronized List<SegmentArchiveReader> updateReaders() throws IOException {
        List<String> archives = listArchives();
        if (archives.isEmpty()) {
            return null;
        }
        List<SegmentArchiveReader> newReaders = new ArrayList<>();
        for (int i = 0; i < archives.size() - 1; i++) {
            String archiveName = archives.get(i);
            if (!closedReaders.containsKey(archiveName)) {
                SegmentArchiveReader reader = archiveManager.forceOpen(archiveName);
                closedReaders.put(archiveName, archiveManager.forceOpen(archiveName));
                newReaders.add(reader);
            }
        }
        currentReader = new SegmentTailingReader(archiveManager, archives.get(archives.size() - 1), segmentStoreDirectory);
        return newReaders;
    }

    private SegmentArchiveReader findReaderForSegment(long msb, long lsb) throws IOException {
        synchronized (this) {
            for (SegmentArchiveReader r : closedReaders.values()) {
                if (r.containsSegment(msb, lsb)) {
                    return r;
                }
            }
            if (currentReader.containsSegment(msb, lsb)) {
                return currentReader;
            }
        }

        SegmentTailingReader localCurrentReader;
        while (true) {
            List<SegmentArchiveReader> newReaders;
            synchronized (this) {
                newReaders = updateReaders();
                localCurrentReader = currentReader;
            }
            for (SegmentArchiveReader r : newReaders) {
                if (r.containsSegment(msb, lsb)) {
                    return r;
                }
            }
            if (localCurrentReader.containsSegment(msb, lsb)) {
                return localCurrentReader;
            }
            boolean segmentFound = localCurrentReader.waitForSegment(msb, lsb);
            if (segmentFound) {
                return localCurrentReader;
            } else if (localCurrentReader.isClosed()) {
                continue;
            } else {
                break;
            }
        }
        return localCurrentReader;
    }

    @Override
    @Nullable
    public Buffer readSegment(long msb, long lsb) throws IOException {
        return findReaderForSegment(msb, lsb).readSegment(msb, lsb);
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        try {
            return findReaderForSegment(msb, lsb).containsSegment(msb, lsb);
        } catch (IOException e) {
            log.error("Can't check segment existence", e);
            return false;
        }
    }

    @Override
    public synchronized List<SegmentArchiveEntry> listSegments() {
        List<SegmentArchiveEntry> entries = new ArrayList<>();
        for (SegmentArchiveReader reader : closedReaders.values()) {
            entries.addAll(reader.listSegments());
        }
        entries.addAll(currentReader.listSegments());
        return entries;
    }

    @Override
    @Nullable
    public Buffer getGraph() {
        return null;
    }

    @Override
    public boolean hasGraph() {
        return false;
    }

    @Override
    @NotNull
    public Buffer getBinaryReferences() {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized long length() {
        long l = 0;
        for (SegmentArchiveReader reader : closedReaders.values()) {
            l += reader.length();
        }
        return l;
    }

    @Override
    @NotNull
    public synchronized String getName() {
        if (closedReaders.isEmpty()) {
            return currentReader.getName();
        } else {
            return closedReaders.values().iterator().next().getName();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        for (SegmentArchiveReader reader : closedReaders.values()) {
            reader.close();
        }
        currentReader.close();
    }

    @Override
    public synchronized int getEntrySize(int size) {
        return currentReader.getEntrySize(size);
    }
}
