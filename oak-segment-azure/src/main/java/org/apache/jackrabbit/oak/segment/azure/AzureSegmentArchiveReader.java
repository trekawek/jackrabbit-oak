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
package org.apache.jackrabbit.oak.segment.azure;

import com.google.common.base.Stopwatch;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.file.tar.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.file.tar.GraphLoader;
import org.apache.jackrabbit.oak.segment.file.tar.IOMonitor;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndex;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.InvalidBinaryReferencesIndexException;
import org.apache.jackrabbit.oak.segment.file.tar.index.Index;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexEntry;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.index.InvalidIndexException;
import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.file.tar.index.IndexLoader.newIndexLoader;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.getSegmentFileName;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.readBufferFully;

public class AzureSegmentArchiveReader implements SegmentArchiveManager.SegmentArchiveReader {

    private static final Logger log = LoggerFactory.getLogger(AzureSegmentArchiveReader.class);

    private static final IndexLoader indexLoader = newIndexLoader(BLOCK_SIZE);

    private final CloudBlobDirectory archiveDirectory;

    private final IOMonitor ioMonitor;

    private final FileStoreMonitor monitor;

    private final Index index;

    private final long length;

    private Boolean hasGraph;

    AzureSegmentArchiveReader(CloudBlobDirectory archiveDirectory, IOMonitor ioMonitor, FileStoreMonitor monitor, Index index) throws IOException {
        this.archiveDirectory = archiveDirectory;
        this.ioMonitor = ioMonitor;
        this.monitor = monitor;
        this.index = index;
        this.length = AzureUtilities.getBlobs(archiveDirectory)
                .map(CloudBlob::getProperties)
                .mapToLong(BlobProperties::getLength)
                .sum();
    }

    @Override
    public ByteBuffer readSegment(long msb, long lsb) throws IOException {
        int i = index.findEntry(msb, lsb);
        if (i == -1) {
            return null;
        }
        IndexEntry entry = index.entry(i);

        ByteBuffer buffer = ByteBuffer.allocate(entry.getLength());
        ioMonitor.beforeSegmentRead(pathAsFile(), msb, lsb, entry.getLength());
        Stopwatch stopwatch = Stopwatch.createStarted();
        readBufferFully(getBlob(getSegmentFileName(entry)), buffer);
        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        ioMonitor.afterSegmentRead(pathAsFile(), msb, lsb, entry.getLength(), elapsed);
        return buffer;
    }

    @Override
    public Index getIndex() {
        return index;
    }

    @Override
    public Map<UUID, List<UUID>> getGraph() throws IOException {
        ByteBuffer graph = loadGraph();
        if (graph == null) {
            return null;
        } else {
            return GraphLoader.parseGraph(graph);
        }
    }
    private ByteBuffer loadGraph() throws IOException {
        ByteBuffer graph = GraphLoader.loadGraph(openAsReaderAtEnd(getName() + ".gph"));
        hasGraph = graph != null;
        return graph;
    }

    @Override
    public boolean hasGraph() {
        if (hasGraph == null) {
            try {
                loadGraph();
            } catch (IOException ignore) { }
        }
        return hasGraph;
    }

    @Override
    public BinaryReferencesIndex getBinaryReferences() {
        BinaryReferencesIndex index = null;
        try {
            index = loadBinaryReferences();
        } catch (InvalidBinaryReferencesIndexException | IOException e) {
            log.warn("Exception while loading binary reference", e);
        }
        return index;
    }

    private BinaryReferencesIndex loadBinaryReferences() throws IOException, InvalidBinaryReferencesIndexException {
        return BinaryReferencesIndexLoader.loadBinaryReferencesIndex(openAsReaderAtEnd(getName() + ".brf"));
    }


    @Override
    public long length() {
        return length;
    }

    @Override
    public String getName() {
        return AzureUtilities.getName(archiveDirectory);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public int getEntrySize(int size) {
        return size;
    }

    private ReaderAtEnd openAsReaderAtEnd(String name) throws IOException {
        return openAsReaderAtEnd(getBlob(name));
    }

    private static ReaderAtEnd openAsReaderAtEnd(CloudBlob cloudBlob) throws IOException {
        try {
            if (!cloudBlob.exists()) {
                return null;
            }
            int length = (int) cloudBlob.getProperties().getLength();
            ByteBuffer buffer = ByteBuffer.allocate(length);
            cloudBlob.downloadToByteArray(buffer.array(), 0);

            return (whence, amount) -> {
                ByteBuffer result = buffer.duplicate();
                result.position(length - whence);
                result.limit(length - whence + amount);
                return result.slice();
            };
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    private File pathAsFile() {
        return new File(archiveDirectory.getUri().getPath());
    }

    public static Index loadAndValidateIndex(CloudBlobDirectory archiveDirectory) throws IOException {
        CloudBlockBlob blob;
        try {
            blob = archiveDirectory.getBlockBlobReference(AzureUtilities.getName(archiveDirectory) + ".idx");
        } catch (StorageException | URISyntaxException e) {
            log.error("Can't open index", e);
            return null;
        }
        ReaderAtEnd reader = openAsReaderAtEnd(blob);
        if (reader == null) {
            return null;
        } else {
            try {
                return indexLoader.loadIndex(reader);
            } catch (InvalidIndexException e) {
                log.warn("Can't open index file: {}", blob.getUri().getPath(), e);
                return null;
            }
        }
    }

    private CloudBlockBlob getBlob(String name) throws IOException {
        try {
            return archiveDirectory.getBlockBlobReference(name);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }
}
