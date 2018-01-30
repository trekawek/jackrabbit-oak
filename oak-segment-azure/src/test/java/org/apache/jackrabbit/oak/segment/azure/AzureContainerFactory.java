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

import java.io.IOException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.junit.Assume;

public final class AzureContainerFactory {

    private static final boolean LOCAL_CLOUD_STORAGE_AVAILABLE = isMockCloudStorageAvailable();

    private AzureContainerFactory() {
    }

    public static CloudBlobContainer getContainer(String name) throws URISyntaxException, StorageException, InvalidKeyException {
        Assume.assumeTrue(LOCAL_CLOUD_STORAGE_AVAILABLE);

        CloudStorageAccount cloud = CloudStorageAccount.parse("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;");
        CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(name);
        container.deleteIfExists();
        container.create();
        return container;
    }

    private static boolean isMockCloudStorageAvailable() {
        try (Socket ignored = new Socket("localhost", 10000)) {
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }
}
