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
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.jackrabbit.oak.segment.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.file.tar.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.IOMonitor;
import org.apache.jackrabbit.oak.segment.file.tar.TarEntry;
import org.apache.jackrabbit.oak.segment.azure.queue.SegmentWriteAction;
import org.apache.jackrabbit.oak.segment.azure.queue.SegmentWriteQueue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.getSegmentFileName;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.readBufferFully;

public class AzureSegmentArchiveWriter implements SegmentArchiveManager.SegmentArchiveWriter {

    private final CloudBlobDirectory archiveDirectory;

    private final IOMonitor ioMonitor;

    private final FileStoreMonitor monitor;

    private final Optional<SegmentWriteQueue> queue;

    private int entries;

    private long totalLength;

    private volatile boolean created = false;

    public AzureSegmentArchiveWriter(CloudBlobDirectory archiveDirectory, IOMonitor ioMonitor, FileStoreMonitor monitor) {
        this.archiveDirectory = archiveDirectory;
        this.ioMonitor = ioMonitor;
        this.monitor = monitor;
        this.queue = SegmentWriteQueue.THREADS > 0 ? Optional.of(new SegmentWriteQueue(this::doWriteEntry)) : Optional.empty();
    }

    @Override
    public TarEntry writeSegment(long msb, long lsb, byte[] data, int offset, int size, GCGeneration generation) throws IOException {
        created = true;

        TarEntry tarEntry = new TarEntry(msb, lsb, (entries++) * BLOCK_SIZE, size, generation);
        if (queue.isPresent()) {
            queue.get().addToQueue(tarEntry, data, offset, size);
        } else {
            doWriteEntry(tarEntry, data, offset, size);
        }
        totalLength += size;
        monitor.written(size);
        return tarEntry;
    }

    private void doWriteEntry(TarEntry tarEntry, byte[] data, int offset, int size) throws IOException {
        long msb = tarEntry.msb();
        long lsb = tarEntry.lsb();
        ioMonitor.beforeSegmentWrite(pathAsFile(), msb, lsb, size);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            CloudBlockBlob blob = getBlob(getSegmentFileName(tarEntry));
            blob.uploadFromByteArray(data, offset, size);
        } catch (StorageException e) {
            throw new IOException(e);
        }
        ioMonitor.afterSegmentWrite(pathAsFile(), msb, lsb, size, stopwatch.elapsed(TimeUnit.NANOSECONDS));
    }

    @Override
    public ByteBuffer readSegment(TarEntry tarEntry) throws IOException {
        UUID uuid = new UUID(tarEntry.msb(), tarEntry.lsb());
        Optional<SegmentWriteAction> segment = queue.map(q -> q.read(uuid));
        if (segment.isPresent()) {
            return segment.get().toByteBuffer();
        }
        ByteBuffer buffer = ByteBuffer.allocate(tarEntry.size());
        readBufferFully(getBlob(getSegmentFileName(tarEntry)), buffer);
        return buffer;
    }

    @Override
    public void writeIndex(byte[] data) throws IOException {
        writeDataFile(data, ".idx");
    }

    @Override
    public void writeGraph(byte[] data) throws IOException {
        writeDataFile(data, ".gph");
    }

    @Override
    public void writeBinaryReferences(byte[] data) throws IOException {
        writeDataFile(data, ".brf");
    }

    private void writeDataFile(byte[] data, String extension) throws IOException {
        try {
            getBlob(getName() + extension).uploadFromByteArray(data, 0, data.length);
        } catch (StorageException e) {
            throw new IOException(e);
        }
        totalLength += data.length;
        monitor.written(data.length);
    }

    @Override
    public long getLength() {
        return totalLength;
    }

    @Override
    public void close() throws IOException {
        if (queue.isPresent()) { // required to handle IOException
            SegmentWriteQueue q = queue.get();
            q.flush();
            q.close();
        }
    }

    @Override
    public boolean isCreated() {
        return created || !queueIsEmpty();
    }

    @Override
    public void flush() throws IOException {
        if (queue.isPresent()) { // required to handle IOException
            queue.get().flush();
        }
    }

    private boolean queueIsEmpty() {
        return queue.map(SegmentWriteQueue::isEmpty).orElse(true);
    }

    @Override
    public String getName() {
        return AzureUtilities.getName(archiveDirectory);
    }

    private File pathAsFile() {
        return new File(archiveDirectory.getUri().getPath());
    }

    private CloudBlockBlob getBlob(String name) throws IOException {
        try {
            return archiveDirectory.getBlockBlobReference(name);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }
}
