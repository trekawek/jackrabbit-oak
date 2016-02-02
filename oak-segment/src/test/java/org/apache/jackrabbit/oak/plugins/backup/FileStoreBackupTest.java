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
package org.apache.jackrabbit.oak.plugins.backup;

import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore.newSegmentNodeStore;
import static org.apache.jackrabbit.oak.plugins.segment.file.FileStore.newFileStore;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileStoreBackupTest {

    private File src;
    private File destination;

    @Before
    public void before() {
        long run = System.currentTimeMillis();
        File root = new File("target");
        src = new File(root, "tar-src-" + run);
        destination = new File(root, "tar-dest-" + run);
    }

    @After
    public void after() {
        deleteQuietly(src);
        deleteQuietly(destination);
    }

    @Test
    public void testBackup() throws Exception {
        FileStore source = newFileStore(src).withMaxFileSize(8).create();

        NodeStore store = newSegmentNodeStore(source).create();
        init(store);

        // initial content
        FileStoreBackup.backup(store, destination);

        compare(source, destination);

        addTestContent(store);
        FileStoreBackup.backup(store, destination);
        compare(source, destination);

        source.close();
    }

    @Test
    public void testRestore() throws Exception {
        FileStore source = newFileStore(src).withMaxFileSize(8).create();

        NodeStore store = newSegmentNodeStore(source).create();
        init(store);
        FileStoreBackup.backup(store, destination);
        addTestContent(store);
        source.close();

        FileStoreRestore.restore(destination, src);

        source = newFileStore(src).withMaxFileSize(8).create();
        compare(source, destination);
        source.close();
    }

    private static void addTestContent(NodeStore store)
            throws CommitFailedException, IOException {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder c = builder.child("test-backup").child("binaries");
        for (int i = 0; i < 2; i++) {
            c.setProperty("bin" + i, createBlob(store, 64 * 1024));
        }
        builder.child("root"); // make sure we don't backup the super-root
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static void compare(FileStore store, File destination)
            throws IOException {
        FileStore backup = newFileStore(destination).withMaxFileSize(8).create();
        assertEquals(store.getHead(), backup.getHead());
        backup.close();
    }

    private static void init(NodeStore store) {
        new Oak(store).with(new OpenSecurityProvider())
                .with(new InitialContent()).createContentRepository();
    }
}
