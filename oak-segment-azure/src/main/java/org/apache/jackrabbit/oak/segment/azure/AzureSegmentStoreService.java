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
package org.apache.jackrabbit.oak.segment.azure;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.segment.SegmentNodeStorePersistence;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupConfigurationThenFramework;
import static org.apache.jackrabbit.oak.segment.azure.AzureSegmentStoreService.AZURE_ACCESS_KEY;
import static org.apache.jackrabbit.oak.segment.azure.AzureSegmentStoreService.AZURE_ACCOUNT_NAME;
import static org.apache.jackrabbit.oak.segment.azure.AzureSegmentStoreService.AZURE_BLOB_ENDPOINT;
import static org.apache.jackrabbit.oak.segment.azure.AzureSegmentStoreService.AZURE_CONTAINER_NAME;
import static org.apache.jackrabbit.oak.segment.azure.AzureSegmentStoreService.AZURE_ROOT_PATH;

@Component(
        label = "Apache Jackrabbit Oak Azure Segment Store",
        name = "org.apache.jackrabbit.oak.segment.azure.AzureSegmentStoreService",
        metatype = true,
        ds = false
)
public class AzureSegmentStoreService {

    private static final Logger log = LoggerFactory.getLogger(AzureSegmentStoreService.class);

    @Property(
            label = "Azure account name"
    )
    public static final String AZURE_ACCOUNT_NAME = "azure.accountName";

    @Property(
            label = "Azure container name"
    )
    public static final String AZURE_CONTAINER_NAME = "azure.containerName";

    @Property(
            label = "Azure access key"
    )
    public static final String AZURE_ACCESS_KEY = "azure.accessKey";

    @Property(
            label = "Azure root path",
            value = "/oak"
    )
    public static final String AZURE_ROOT_PATH = "azure.rootPath";

    @Property(
            label = "Azure blob endpoint (optional)"
    )
    public static final String AZURE_BLOB_ENDPOINT = "azure.blobEndpoint";

    private ServiceRegistration registration;

    private SegmentNodeStorePersistence persistence;

    @Activate
    public void activate(ComponentContext context) throws IOException {
        Configuration config = new Configuration(context);
        persistence = createAzurePersistence(config);
        registration = context.getBundleContext().registerService(SegmentNodeStorePersistence.class.getName(), persistence, new Properties());
    }

    @Deactivate
    public void deactivate() throws IOException {
        if (registration != null) {
            registration.unregister();
            registration = null;
        }
        persistence = null;
    }

    private static SegmentNodeStorePersistence createAzurePersistence(Configuration configuration) throws IOException {
        try {
            StringBuilder connectionString = new StringBuilder();
            connectionString.append("DefaultEndpointsProtocol=http;");
            connectionString.append("AccountName=").append(configuration.getAzureAccountName()).append(';');
            connectionString.append("AccountKey=").append(configuration.getAzureAccessKey()).append(';');
            if (configuration.getAzureBlobEndpoint() != null) {
                connectionString.append("BlobEndpoint=").append(configuration.getAzureBlobEndpoint()).append(';');
            }
            log.info("Connection string: {}", connectionString.toString());
            CloudStorageAccount cloud = CloudStorageAccount.parse(connectionString.toString());
            CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(configuration.getAzureContainerName());
            container.createIfNotExists();

            String path = configuration.getAzureRootPath();
            if (path != null && path.length() > 0 && path.charAt(0) == '/') {
                path = path.substring(1);
            }

            AzurePersistence persistence = new AzurePersistence(container.getDirectoryReference(path));
            return persistence;
        } catch (StorageException | URISyntaxException | InvalidKeyException e) {
            throw new IOException(e);
        }
    }

}

class Configuration {

    private final ComponentContext context;

    Configuration(ComponentContext context) {
        this.context = context;
    }

    String property(String name) {
        return lookupConfigurationThenFramework(context, name);
    }

    String getAzureAccountName() {
        return property(AZURE_ACCOUNT_NAME);
    }

    String getAzureContainerName() {
        return property(AZURE_CONTAINER_NAME);
    }

    String getAzureAccessKey() {
        return property(AZURE_ACCESS_KEY);
    }

    String getAzureRootPath() { return PropertiesUtil.toString(property(AZURE_ROOT_PATH), "/oak"); }

    String getAzureBlobEndpoint() {
        return property(AZURE_BLOB_ENDPOINT);
    }

}
