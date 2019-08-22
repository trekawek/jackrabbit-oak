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

import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.remote.proto.SegmentServiceGrpc;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ArchiveTailingReader implements SegmentArchiveReader {

    private static final Logger log = LoggerFactory.getLogger(ArchiveTailingReader.class);

    private final SegmentArchiveManager archiveManager;

    private final Map<String, SegmentArchiveReader> closedReaders = new LinkedHashMap<>();

    private final CloudBlobDirectory segmentStoreDirectory;

    private SegmentTailingReader currentReader;

    private StreamObserver<Empty> streamObserver;

    public ArchiveTailingReader(SegmentArchiveManager archiveManager, CloudBlobDirectory segmentStoreDirectory, SegmentServiceGrpc.SegmentServiceStub segmentService) throws IOException {
        this.archiveManager = archiveManager;
        this.segmentStoreDirectory = segmentStoreDirectory;
        streamObserver = segmentService.observeSegments(new StreamObserver<StringValue>() {
            @Override
            public void onNext(StringValue stringValue) {
                if (currentReader != null) {
                    currentReader.onNewSegment(stringValue.getValue());
                }
            }
            @Override
            public void onError(Throwable throwable) {
            }
            @Override
            public void onCompleted() {
            }
        });
        updateReaders();
    }

    private synchronized List<SegmentArchiveReader> updateReaders() throws IOException {
        List<String> archives = archiveManager.listArchives();
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
        String lastArchive = archives.get(archives.size() - 1);
        if (currentReader == null || !lastArchive.equals(currentReader.getName())) {
            try {
                currentReader = new SegmentTailingReader(archiveManager, lastArchive, segmentStoreDirectory);
                currentReader.init();
            } catch (URISyntaxException e) {
                throw new IOException(e);
            }
        }
        return newReaders;
    }

    private SegmentArchiveReader findReaderForSegment(long msb, long lsb) throws IOException {
        synchronized (this) {
            for (SegmentArchiveReader r : closedReaders.values()) {
                if (r.containsSegment(msb, lsb)) {
                    return r;
                }
            }
        }

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < TimeUnit.MINUTES.toMillis(1)) {
            SegmentTailingReader localReader = currentReader;
            if (localReader.containsSegment(msb, lsb)) {
                return localReader;
            } else if (localReader.isClosed()) {
                List<SegmentArchiveReader> newReaders = updateReaders();
                for (SegmentArchiveReader r : newReaders) {
                    if (r.containsSegment(msb, lsb)) {
                        return r;
                    }
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }
        return currentReader;
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
        streamObserver.onNext(Empty.getDefaultInstance());
        streamObserver.onCompleted();
    }

    @Override
    public synchronized int getEntrySize(int size) {
        return currentReader.getEntrySize(size);
    }
}
