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
package org.apache.jackrabbit.oak.remote.common.persistence;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.remote.proto.SegmentProtos;
import org.apache.jackrabbit.oak.remote.proto.SegmentServiceGrpc.SegmentServiceBlockingStub;
import org.apache.jackrabbit.oak.segment.azure.AzureBlobMetadata;
import org.apache.jackrabbit.oak.segment.azure.AzureSegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.readBufferFully;

public class TailingReader implements SegmentArchiveReader  {

    private static final Logger log = LoggerFactory.getLogger(TailingReader.class);

    private final CloudBlobContainer container;

    private final Map<UUID, AzureSegment> index = new ConcurrentHashMap<>();

    private final AtomicLong length = new AtomicLong();

    private final Cache<UUID, Buffer> segmentCache;

    private final SegmentServiceBlockingStub segmentService;

    public TailingReader(CloudBlobContainer container, List<String> segmentStoreNames, SegmentServiceBlockingStub segmentService) throws IOException {
        this.segmentService = segmentService;
        this.segmentCache = CacheBuilder.newBuilder()
                .maximumSize(128)
                .build();
        try {
            this.container = container;
            for (String segmentStore : segmentStoreNames) {
                for (ListBlobItem blob : container.getDirectoryReference(segmentStore).listBlobs("data", true, EnumSet.of(BlobListingDetails.METADATA), null, null)) {
                    if (blob instanceof CloudBlob) {
                        addNewSegment((CloudBlob) blob);
                    }
                }
            }
        } catch (StorageException | URISyntaxException e) {
            throw new IOException(e);
        }
    }

    public void onNewSegment(SegmentProtos.SegmentBlob segmentBlob) {
        try {
            CloudBlob cloudBlob = getBlob(segmentBlob.getBlobName());
            for (int i = 0; i < 10; i++) {
                if (cloudBlob.exists()) {
                    break;
                }
                log.info("Blob {} doesn't exist yet...", cloudBlob.getName());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
            cloudBlob.downloadAttributes();
            addNewSegment(cloudBlob);
        } catch (StorageException | IOException e) {
            log.error("Can't read blob {} (segment name: {})", segmentBlob.getBlobName(), e);
        }
    }

    private void addNewSegment(CloudBlob blob) {
        Map<String, String> metadata = blob.getMetadata();
        if (AzureBlobMetadata.isSegment(metadata)) {
            AzureSegmentArchiveEntry indexEntry = AzureBlobMetadata.toIndexEntry(metadata, (int) blob.getProperties().getLength());
            AzureSegment segment = new AzureSegment(blob.getName(), indexEntry);
            UUID uuid = new UUID(indexEntry.getMsb(), indexEntry.getLsb());
            if (index.containsKey(uuid)) {
                return;
            }
            index.put(uuid, segment);
            length.addAndGet(blob.getProperties().getLength());
        }
    }

    @Override
    @Nullable
    public Buffer readSegment(long msb, long lsb) throws IOException {
        UUID uuid = new UUID(msb, lsb);
        AzureSegment segment = index.get(uuid);
        if (segment == null) {
            return getRecentSegment(msb, lsb);
        }
        try {
            return segmentCache.get(uuid, () -> loadSegmentFromCloud(segment));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException(e);
            }
        }
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        if (index.containsKey(new UUID(msb, lsb))) {
            return true;
        } else {
            return getRecentSegment(msb, lsb) != null;
        }
    }

    private Buffer getRecentSegment(long msb, long lsb) {
        UUID uuid = new UUID(msb, lsb);
        if (segmentService == null) {
            while (!index.containsKey(uuid)) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                    return null;
                }
            }
            try {
                return segmentCache.get(uuid, () -> loadSegmentFromCloud(index.get(uuid)));
            } catch (ExecutionException e) {
                log.error("Can't load segment {}", uuid, e);
                return null;
            }
        } else {
            try {
                return segmentCache.get(uuid, () -> loadSegmentFromGrpc(msb, lsb));
            } catch (ExecutionException e) {
                log.error("Can't load segment {}", uuid, e);
                return null;
            }
        }
    }

    private Buffer loadSegmentFromGrpc(long msb, long lsb) {
        SegmentProtos.Segment segment = segmentService.getSegment(SegmentProtos.SegmentId.newBuilder()
                .setMsb(msb)
                .setLsb(lsb)
                .build());
        return Buffer.wrap(segment.getSegmentData().toByteArray());
    }

    private Buffer loadSegmentFromCloud(AzureSegment segment) throws IOException {
        Buffer buffer = Buffer.allocate(segment.segmentArchiveEntry.getLength());
        readBufferFully(getBlob(segment.blobName), buffer);
        return buffer;
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        return index
                .values()
                .stream()
                .map(a -> a.segmentArchiveEntry)
                .collect(Collectors.toList());
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
    public @NotNull Buffer getBinaryReferences() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long length() {
        return length.get();
    }

    @Override
    public @NotNull String getName() {
        return "data00000a.tar";
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public int getEntrySize(int size) {
        return 0;
    }

    private CloudBlockBlob getBlob(String name) throws IOException {
        try {
            return container.getBlockBlobReference(name);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    private static class AzureSegment {

        private final AzureSegmentArchiveEntry segmentArchiveEntry;

        private final String blobName;

        public AzureSegment(String blobName, AzureSegmentArchiveEntry segmentArchiveEntry) {
            this.blobName = blobName;
            this.segmentArchiveEntry = segmentArchiveEntry;
        }
    }
}
