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
package org.apache.jackrabbit.oak.remote.server;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.oak.remote.common.persistence.TailingPersistence;
import org.apache.jackrabbit.oak.remote.proto.SegmentProtos;
import org.apache.jackrabbit.oak.segment.RevRepositoryService;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.spi.rev.RevRepository;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PrivateFileStores {

    private static final Logger log = LoggerFactory.getLogger(PrivateFileStores.class);

    private final CloudBlobContainer container;

    private final String sharedDirectoryName;

    private final Map<String, FileStoreEntry> fileStoreMap;

    private final BlobStore blobStore;

    private final RevRepositoryService revNodeStoreService = new RevRepositoryService();

    public PrivateFileStores(CloudBlobDirectory sharedDirectory, BlobStore blobStore) throws URISyntaxException, StorageException {
        this.container = sharedDirectory.getContainer();
        this.sharedDirectoryName = sharedDirectory.getPrefix();
        this.fileStoreMap = new HashMap<>();
        this.blobStore = blobStore;
    }

    public NodeState getNodeState(String segmentStoreDir, String revision) throws IOException {
        return getFileStoreEntry(segmentStoreDir).revNodeStore.getNodeStateByRevision(revision);
    }

    public synchronized void onNewSharedSegment(SegmentProtos.SegmentBlob segmentBlob) {
        for (FileStoreEntry entry : fileStoreMap.values()) {
            entry.privatePersistence.onNewSegment(segmentBlob);
        }
    }

    public void onNewPrivateSegment(String segmentStoreDir, SegmentProtos.SegmentBlob segmentBlob) {
        try {
            getFileStoreEntry(segmentStoreDir).privatePersistence.onNewSegment(segmentBlob);
        } catch (IOException e) {
            log.error("Can't process new segment {}", segmentBlob.getBlobName(), e);
        }
    }

    private synchronized FileStoreEntry getFileStoreEntry(String segmentStoreDir) throws IOException {
        if (fileStoreMap.containsKey(segmentStoreDir)) {
            return fileStoreMap.get(segmentStoreDir);
        }
        TailingPersistence privatePersistence;
        try {
            privatePersistence = new TailingPersistence(
                    new AzurePersistence(container.getDirectoryReference(sharedDirectoryName)),
                    null,
                    Arrays.asList(sharedDirectoryName, segmentStoreDir));
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
        FileStoreEntry entry = new FileStoreEntry(privatePersistence, blobStore);
        fileStoreMap.put(segmentStoreDir, entry);
        return entry;
    }

    private class FileStoreEntry implements Closeable {

        private final RevRepository revNodeStore;

        private final TailingPersistence privatePersistence;

        public FileStoreEntry(TailingPersistence privatePersistence, BlobStore blobStore) throws IOException {
            this.privatePersistence = privatePersistence;
            this.revNodeStore = revNodeStoreService.builder()
                    .withBlobStore(blobStore)
                    .withPersistence(privatePersistence)
                    .readOnly()
                    .build();
        }

        public void close() throws IOException {
            revNodeStore.close();
        }
    }
}
