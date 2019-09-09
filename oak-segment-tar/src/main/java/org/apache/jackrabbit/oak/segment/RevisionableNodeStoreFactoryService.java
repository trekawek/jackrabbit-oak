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
package org.apache.jackrabbit.oak.segment;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.spi.RevisionableNodeStoreFactory;
import org.apache.jackrabbit.oak.segment.spi.state.RevisionableNodeState;
import org.apache.jackrabbit.oak.segment.spi.state.RevisionableNodeStore;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import java.io.File;
import java.io.IOException;

@Component(configurationPolicy = ConfigurationPolicy.OPTIONAL)
public class RevisionableNodeStoreFactoryService implements RevisionableNodeStoreFactory {

    @Override
    public RevisionableNodeStore create(Builder builder) throws IOException {
        File dir = Files.createTempDir();

        FileStoreBuilder fsBuilder = FileStoreBuilder.fileStoreBuilder(dir)
                .withCustomPersistence(builder.getPersistence())
                .withBlobStore(builder.getBlobStore());

        try {
            if (builder.isReadOnly()) {
                return new ReadOnlyRevisionableNodeStore(fsBuilder.buildReadOnly(), dir);
            } else {
                return new DefaultRevisionableNodeStore(fsBuilder.build(), dir);
            }
        } catch (InvalidFileStoreVersionException e) {
            throw new IOException(e);
        }
    }

    private static class DefaultRevisionableNodeStore implements RevisionableNodeStore {

        private final File directory;

        private final FileStore fileStore;

        public DefaultRevisionableNodeStore(FileStore fileStore, File directory) {
            this.fileStore = fileStore;
            this.directory = directory;
        }

        @Override
        public RevisionableNodeState getNodeStateByRevision(String revision) {
            RecordId recordId = RecordId.fromString(fileStore.getSegmentIdProvider(), revision);
            return fileStore.getReader().readNode(recordId);
        }

        @Override
        public void flushData() throws IOException {
            fileStore.getWriter().flush();
        }

        @Override
        public void flushJournal() throws IOException {
            fileStore.flush();
        }

        @Override
        public void close() throws IOException {
            fileStore.close();
            FileUtils.deleteDirectory(directory);
        }
    }

    private static class ReadOnlyRevisionableNodeStore implements RevisionableNodeStore {

        private final File directory;

        private final ReadOnlyFileStore fileStore;

        public ReadOnlyRevisionableNodeStore(ReadOnlyFileStore fileStore, File directory) {
            this.fileStore = fileStore;
            this.directory = directory;
        }

        @Override
        public RevisionableNodeState getNodeStateByRevision(String revision) {
            RecordId recordId = RecordId.fromString(fileStore.getSegmentIdProvider(), revision);
            return fileStore.getReader().readNode(recordId);
        }

        @Override
        public void flushData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flushJournal() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            fileStore.close();
            FileUtils.deleteDirectory(directory);
        }
    }
}
