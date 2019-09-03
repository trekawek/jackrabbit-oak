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

import com.google.common.io.Closer;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.remote.common.persistence.TailingPersistence;
import org.apache.jackrabbit.oak.remote.proto.SegmentProtos;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.io.Files.createTempDir;

public class PrivateFileStores {

    private static final Logger log = LoggerFactory.getLogger(PrivateFileStores.class);

    private final CloudBlobContainer container;

    private final String sharedDirectoryName;

    private final Map<String, FileStoreEntry> fileStoreMap;

    private final BlobStore blobStore;

    public PrivateFileStores(CloudBlobDirectory sharedDirectory, BlobStore blobStore) throws URISyntaxException, StorageException {
        this.container = sharedDirectory.getContainer();
        this.sharedDirectoryName = sharedDirectory.getPrefix();
        this.fileStoreMap = new HashMap<>();
        this.blobStore = blobStore;
    }

    public NodeState getNodeState(String segmentStoreDir, String revision) throws IOException {
        ReadOnlyFileStore fileStore = getFileStoreEntry(segmentStoreDir).fileStore;
        RecordId recordId = RecordId.fromString(fileStore.getSegmentIdProvider(), revision);
        return fileStore.getReader().readNode(recordId);
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
        TailingPersistence privatePersistence = null;
        try {
            privatePersistence = new TailingPersistence(
                    new AzurePersistence(container.getDirectoryReference(segmentStoreDir)),
                    null,
                    Arrays.asList(sharedDirectoryName, segmentStoreDir));
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
        FileStoreEntry entry = new FileStoreEntry(privatePersistence, blobStore);
        fileStoreMap.put(segmentStoreDir, entry);
        return entry;
    }

    private static class FileStoreEntry implements Closeable {

        private final ReadOnlyFileStore fileStore;

        private final TailingPersistence privatePersistence;

        private final Closer closer = Closer.create();

        public FileStoreEntry(TailingPersistence privatePersistence, BlobStore blobStore) throws IOException {
            this.privatePersistence = privatePersistence;
            File dir = createTempDir();
            closer.register(() -> FileUtils.deleteDirectory(dir));
            FileStoreBuilder builder = FileStoreBuilder
                    .fileStoreBuilder(dir)
                    .withCustomPersistence(privatePersistence)
                    .withBlobStore(blobStore);
            try {
                fileStore = builder.buildReadOnly();
            } catch (InvalidFileStoreVersionException e) {
                throw new IOException(e);
            }
            closer.register(fileStore);
        }

        public void close() throws IOException {
            closer.close();
        }
    }
}
