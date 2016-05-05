/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional debugrmation regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.RAMDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class MemoryDirectoryStorage {

    private static final Logger log = LoggerFactory.getLogger(IndexTracker.class);

    final Map<String, RAMDirectory> directories;

    public MemoryDirectoryStorage() {
        this.directories = new HashMap<String, RAMDirectory>();
    }

    @Nonnull
    public Directory getOrCreateDirectory(@Nonnull final String indexPath) {
        checkNotNull(indexPath);

        log.debug("getOrCreate {}", indexPath);
        final RAMDirectory directory;
        synchronized (this) {
            if (directories.containsKey(indexPath)) {
                directory = directories.get(indexPath);
                log.debug("returned {} directory: {}", indexPath, directory);
            } else {
                directory = new RAMDirectory();
                log.debug("created {} directory: {}", indexPath, directory);
            }
        }
        return new UnclosableDirectory(directory) {
            @Override
            public void close() throws IOException {
                log.debug("stored {} directory on close(): {}", indexPath, directory);
                directories.put(indexPath, directory);
            }
        };
    }

    @CheckForNull
    public Directory getDirectory(String indexPath) {
        log.debug("get {}", indexPath);

        synchronized (this) {
            RAMDirectory directory = directories.get(indexPath);
            if (directory == null) {
                log.debug("there's no {} directory", indexPath);
                return null;
            } else {
                log.debug("returned {} directory: {}", indexPath, directory);
                return new UnclosableDirectory(directory);
            }
        }
    }

    public void purge(String indexPath) {
        log.debug("purge {}", indexPath);

        RAMDirectory directory;
        synchronized (this) {
            directory = directories.remove(indexPath);
        }
        if (directory != null) {
            directory.close();
        }
    }

    private static class UnclosableDirectory extends Directory {

        private final Directory directory;

        public UnclosableDirectory(Directory directory) {
            this.directory = directory;
        }

        @Override
        public String[] listAll() throws IOException {
            return directory.listAll();
        }

        @Override
        public boolean fileExists(String name) throws IOException {
            return directory.fileExists(name);
        }

        @Override
        public void deleteFile(String name) throws IOException {
            directory.deleteFile(name);
        }

        @Override
        public long fileLength(String name) throws IOException {
            return directory.fileLength(name);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            return directory.createOutput(name, context);
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
            directory.sync(names);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return directory.openInput(name, context);
        }

        @Override
        public Lock makeLock(String name) {
            return directory.makeLock(name);
        }

        @Override
        public void clearLock(String name) throws IOException {
            directory.clearLock(name);
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }

        @Override
        public void setLockFactory(LockFactory lockFactory) throws IOException {
            directory.setLockFactory(lockFactory);
        }

        @Override
        public LockFactory getLockFactory() {
            return directory.getLockFactory();
        }
    }
}