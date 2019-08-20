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
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import java.io.File;
import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        Closer closer = Closer.create();
        try {
            File dataStorePath = new File("datastore");
            dataStorePath.mkdirs();

            FileDataStore dataStore = new FileDataStore();
            dataStore.setPath(dataStorePath.getPath());
            dataStore.init(null);
            closer.register(() -> dataStore.close());
            BlobStore blobStore = new DataStoreBlobStore(dataStore);

            File segmentStore = new File("segmentstore");
            segmentStore.mkdirs();
            FileStore fs = FileStoreBuilder.fileStoreBuilder(segmentStore).withBlobStore(blobStore).build();
            closer.register(fs);

            SegmentNodeStore delegate = SegmentNodeStoreBuilders.builder(fs).build();

            System.out.println("Starting server. Press ^C to stop.");
            NodeStoreServer server = new NodeStoreServer(12300, delegate, blobStore);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop()));
            server.start();
            server.blockUntilShutdown();
        } catch(Throwable t) {
            throw closer.rethrow(t);
        } finally {
            closer.close();
        }
    }

}
