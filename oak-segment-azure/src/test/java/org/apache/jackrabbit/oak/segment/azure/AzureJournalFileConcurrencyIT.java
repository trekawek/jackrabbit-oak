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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class AzureJournalFileConcurrencyIT {

    private static CloudBlobContainer container;

    private static int suffix;

    private AzurePersistence persistence;

    @BeforeClass
    public static void connectToAzure() throws URISyntaxException, InvalidKeyException, StorageException {
        String azureConnectionString = System.getenv("AZURE_CONNECTION");
        Assume.assumeNotNull(azureConnectionString);
        CloudBlobClient client = CloudStorageAccount.parse(azureConnectionString).createCloudBlobClient();
        container = client.getContainerReference("oak-test-" + System.currentTimeMillis());
        container.createIfNotExists();
        suffix = 1;
    }

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        persistence = new AzurePersistence(container.getDirectoryReference("oak-" + (suffix++)));
    }

    @AfterClass
    public static void cleanupContainer() throws StorageException {
        container.deleteIfExists();
    }

    @Test
    public void testConcurrency() throws Exception {
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Exception> exContainer = new AtomicReference<>();

        Thread producer = new Thread(() -> {
            int i = 0;
            try {
                while (!stop.get()) {
                    JournalFile file = persistence.getJournalFile();
                    try (JournalFileWriter writer = file.openJournalWriter()) {
                        for (int j = 0; j < 128; j++) {
                            writer.writeLine(String.format("%4X - %s", i++, UUID.randomUUID().toString()));
                        }
                    }
                }
            } catch(Exception e) {
                exContainer.set(e);
                stop.set(true);
            }
        });

        Thread consumer = new Thread(() -> {
            try {
                while (!stop.get()) {
                    JournalFile file = persistence.getJournalFile();
                    try (JournalFileReader reader = file.openJournalReader()) {
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            System.out.println(line);
                            Thread.sleep(100);
                        }
                    }
                }
            } catch (IOException | InterruptedException e) {
                exContainer.set(e);
                stop.set(true);
            }
        });

        producer.start();
        consumer.start();

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 30_000) {
            Thread.sleep(100);
        }
        stop.set(true);

        producer.join();
        consumer.join();

        if (exContainer.get() != null) {
            throw exContainer.get();
        }
    }

}
