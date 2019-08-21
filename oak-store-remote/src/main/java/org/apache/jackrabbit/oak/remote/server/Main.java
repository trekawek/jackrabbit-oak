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

import com.google.common.base.Strings;
import com.google.common.io.Closer;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.composite.InitialContentMigrator;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static com.google.common.io.Files.createTempDir;
import static java.util.Arrays.asList;

public class Main {

    public static void main(String[] args) throws IOException {
        Closer closer = Closer.create();
        try {
            BlobStore blobStore;
            if (getenv("blobAzureAccount") == null) {
                File dataStorePath = new File("datastore");
                dataStorePath.mkdirs();

                FileDataStore dataStore = new FileDataStore();
                dataStore.setPath(dataStorePath.getPath());
                dataStore.init(null);
                closer.register(() -> dataStore.close());
                blobStore = new DataStoreBlobStore(dataStore);
            } else {
                blobStore = createAzureBlobStore();
            }

            File segmentStore = new File("segmentstore");
            segmentStore.mkdirs();
            FileStore fs = FileStoreBuilder.fileStoreBuilder(segmentStore).withBlobStore(blobStore).build();
            closer.register(fs);

            SegmentNodeStore delegate = SegmentNodeStoreBuilders.builder(fs).build();

            String seedSegmentStore = getenv("seed_segmentstore");
            if (!Strings.isNullOrEmpty(seedSegmentStore)) {
                initialize(seedSegmentStore, delegate);
            }

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

    private static void initialize(String seedSegmentStore, SegmentNodeStore delegate) throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        File seedStore = new File(seedSegmentStore);
        FileStore fs = FileStoreBuilder.fileStoreBuilder(seedStore).build();
        try {
            SegmentNodeStore seed = SegmentNodeStoreBuilders.builder(fs).build();
            new InitialContentMigrator(delegate, seed, createMountInfoProvider().getMountByName("libs")).migrate();
        } finally {
            fs.close();
        }
    }

    private static BlobStore createAzureBlobStore() throws DataStoreException {
        Properties properties = new Properties();
        properties.setProperty("accessKey", getenv("blobAzureAccount"));
        properties.setProperty("secretKey", getenv("blobAzureAccessKey"));
        properties.setProperty("container", getenv("blobAzureContainer"));
        properties.setProperty("cacheSize", getenv("blobAzureCacheSize"));
        properties.setProperty("secret", getenv("blobAzureSecret"));

        properties.setProperty("maxConnections", "4");
        properties.setProperty("maxErrorRetry", "10");
        properties.setProperty("socketTimeout", "120000");
        properties.setProperty("path", createTempDir().getAbsolutePath());

        AzureDataStore azureDataStore = new AzureDataStore();
        azureDataStore.setProperties(properties);
        azureDataStore.init(createTempDir().getAbsolutePath());
        return new DataStoreBlobStore(azureDataStore);
    }

    private static String getenv(String envName) {
        return System.getenv(envName);
    }

    private static String getenv(String envName, String defaultValue) {
        String value = System.getenv(envName);
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        } else {
            return value;
        }
    }

    public static MountInfoProvider createMountInfoProvider() {
        return Mounts.newBuilder()
                .mount("libs", true, asList(
                        "/oak:index/*$" // pathsSupportingFragments
                ), asList(
                        "/libs",        // mountedPaths
                        "/apps",
                        "/jcr:system/rep:permissionStore/oak:mount-libs-crx.default"))
                .build();
    }

}
