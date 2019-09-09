/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
package org.apache.jackrabbit.oak.remote.client;

import com.google.common.io.Closer;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.segment.spi.rev.RevRepositoryFactory;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.apache.jackrabbit.oak.spi.descriptors.GenericDescriptors;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.UUID;

import static org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo.getOrCreateId;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = Configuration.class)
public class RemoteNodeStoreService {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteNodeStoreService.class);

    @Reference(cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private BlobStore blobStore;

    @Reference(cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private RevRepositoryFactory nodeStoreFactory;

    private ComponentContext context;

    private Closer closer;

    private Configuration config;

    private RemoteNodeStoreClient client;

    private String privateDirName;

    private SegmentNodeStorePersistence persistence;

    @Activate
    protected void activate(ComponentContext context, Configuration config) throws Exception {
        this.context = context;
        this.config = config;
        this.closer = Closer.create();
        createClient();
        registerPersistence();
        registerRemoteNodeStore();
    }

    @Deactivate
    protected void deactivate() throws IOException {
        closer.close();
    }

    private void createClient() {
        client = new RemoteNodeStoreClient(config.remoteHost(), config.remotePort());
        closer.register(client);
    }

    private void registerPersistence() throws StorageException, InvalidKeyException, URISyntaxException, IOException {
        String sharedDirName = config.rootPath();
        privateDirName = sharedDirName + "-" + UUID.randomUUID().toString();
        CloudBlobContainer container = createContainer(config);
        TailingPersistenceFactory persistenceFactory = new TailingPersistenceFactory(container, client, config.rootPath(), privateDirName);
        closer.register(persistenceFactory);
        persistence = persistenceFactory.create();
    }

    private static CloudBlobContainer createContainer(Configuration config) throws URISyntaxException, StorageException, InvalidKeyException {
        CloudStorageAccount cloud;
        if (config.connectionURL() != null) {
            cloud = CloudStorageAccount.parse(config.connectionURL());
        } else {
            StorageCredentials credentials = new StorageCredentialsAccountAndKey(
                    config.accountName(),
                    config.accessKey());
            cloud = new CloudStorageAccount(credentials, true);
        }
        return cloud.createCloudBlobClient().getContainerReference(config.containerName());
    }

    private void registerRemoteNodeStore() throws Exception {
        RemoteNodeStore.Builder builder = new RemoteNodeStore.Builder();
        builder.setBlobStore(blobStore);
        builder.setClient(client);
        builder.setPrivateDirName(privateDirName);
        builder.setNodeStore(nodeStoreFactory.builder().withBlobStore(blobStore).withPersistence(persistence).build());

        RemoteNodeStore store = builder.build();

        Whiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());

        if (config.role() == null) {
            ObserverTracker observerTracker = new ObserverTracker(store);
            observerTracker.start(context.getBundleContext());
            closer.register(() -> observerTracker.stop());

            registerMBean(whiteboard,
                    CheckpointMBean.class,
                    new RemoteCheckpointMBean(store),
                    CheckpointMBean.TYPE,
                    "Remote node store checkpoint management");

            registerDescriptors(whiteboard, store);

            Dictionary<String, Object> props = new Hashtable<String, Object>();
            props.put(Constants.SERVICE_PID, RemoteNodeStore.class.getName());
            props.put("oak.nodestore.description", new String[] { "nodeStoreType=remote" } );

            LOG.info("Registering the remote node store");
            ServiceRegistration nsReg = context.getBundleContext().registerService(
                    NodeStore.class.getName(),
                    store,
                    props);
            closer.register(() -> nsReg.unregister());
        } else {
            registerDescriptors(whiteboard, store);

            Dictionary<String, Object> props = new Hashtable<String, Object>();
            props.put(NodeStoreProvider.ROLE, config.role());

            LOG.info("Registering the remote node store provider");
            ServiceRegistration nsReg = context.getBundleContext().registerService(
                    NodeStoreProvider.class.getName(),
                    (NodeStoreProvider) () -> store,
                    props);
            closer.register(() -> nsReg.unregister());
        }
    }

    private void registerDescriptors(Whiteboard whiteboard, RemoteNodeStore remoteNodeStore) {
        GenericDescriptors clusterIdDesc = new GenericDescriptors();
        clusterIdDesc.put(
                ClusterRepositoryInfo.OAK_CLUSTERID_REPOSITORY_DESCRIPTOR_KEY,
                new SimpleValueFactory().createValue(getOrCreateId(remoteNodeStore)),
                true,
                false
        );
        register(whiteboard, Descriptors.class, clusterIdDesc);
        register(whiteboard, Descriptors.class, new RemoteNodeStoreDiscoveryLiteDescriptors(remoteNodeStore));
    }

    private <T> void register(Whiteboard whiteboard, Class<T> iface, T bean) {
        Registration reg = whiteboard.register(iface, bean, new HashMap<>());
        closer.register(() -> reg.unregister());
    }

   private <T> void registerMBean(Whiteboard whiteboard, Class<T> iface, T bean, String type, String name) {
        Registration reg = WhiteboardUtils.registerMBean(whiteboard, iface, bean, type, name);
        closer.register(() -> reg.unregister());
    }
}