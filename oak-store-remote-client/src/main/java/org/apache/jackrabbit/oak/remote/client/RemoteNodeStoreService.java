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
import com.google.common.io.Files;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.commons.io.FileUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.UUID;

import static org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo.getOrCreateId;

@Component(policy = ConfigurationPolicy.REQUIRE, metatype = true)
public class RemoteNodeStoreService {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteNodeStoreService.class);

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY, policy = ReferencePolicy.STATIC)
    private BlobStore blobStore;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY, policy = ReferencePolicy.STATIC)
    private SegmentNodeStorePersistence persistence;

    @Property(label = "Remote server host",
            description = "The host name of the remote server",
            value = "localhost"
    )
    private static final String REMOTE_HOST = "remoteHost";

    @Property(label = "Remote server port",
            description = "The port number of the remote server",
            intValue = 12300
    )
    private static final String REMOTE_PORT = "remotePort";

    @Property(label = "NodeStoreProvider role",
            description = "Property indicating that this component will not register as a NodeStore but as a NodeStoreProvider with given role")
    private static final String ROLE = "role";

    private ComponentContext context;

    private Closer registrations;

    private String remoteHost;

    private int remotePort;

    private String role;

    @Activate
    protected void activate(ComponentContext context, Map<String, ?> config) throws IOException, InvalidFileStoreVersionException, URISyntaxException, StorageException {
        this.context = context;
        remoteHost = PropertiesUtil.toString(config.get(REMOTE_HOST), "localhost");
        remotePort = PropertiesUtil.toInteger(config.get(REMOTE_PORT), 12300);
        role = PropertiesUtil.toString(config.get(ROLE), null);
        registerRemoteNodeStore();
    }

    @Deactivate
    protected void deactivate() throws IOException {
        unregisterRemoteNodeStore();
    }

    private void registerRemoteNodeStore() throws IOException, InvalidFileStoreVersionException, URISyntaxException, StorageException {
        registrations = Closer.create();

        RemoteNodeStoreClient client = new RemoteNodeStoreClient(remoteHost, remotePort);
        RemoteNodeStore.Builder builder = new RemoteNodeStore.Builder();
        builder.setBlobStore(blobStore);
        builder.setClient(client);
        if (persistence instanceof AzurePersistence) {
            AzurePersistence azurePersistence = (AzurePersistence) persistence;
            CloudBlobDirectory sharedDirectory = azurePersistence.getSegmentstoreDirectory();
            builder.setCloudContainer(sharedDirectory.getContainer())
                    .setSharedDirName(sharedDirectory.getPrefix())
                    .setPrivateDirName(sharedDirectory.getPrefix() + UUID.randomUUID().toString());
        } else {
            throw new IllegalArgumentException("Invalid persistence, only AzurePersistence is supported");
        }
        RemoteNodeStore store = builder.build();

        Whiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());

        if (role == null) {
            ObserverTracker observerTracker = new ObserverTracker(store);
            observerTracker.start(context.getBundleContext());
            registrations.register(() -> observerTracker.stop());

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
            registrations.register(() -> nsReg.unregister());
        } else {
            registerDescriptors(whiteboard, store);

            Dictionary<String, Object> props = new Hashtable<String, Object>();
            props.put(NodeStoreProvider.ROLE, role);

            LOG.info("Registering the remote node store provider");
            ServiceRegistration nsReg = context.getBundleContext().registerService(
                    NodeStoreProvider.class.getName(),
                    (NodeStoreProvider) () -> store,
                    props);
            registrations.register(() -> nsReg.unregister());
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
        registrations.register(() -> reg.unregister());
    }

   private <T> void registerMBean(Whiteboard whiteboard, Class<T> iface, T bean, String type, String name) {
        Registration reg = WhiteboardUtils.registerMBean(whiteboard, iface, bean, type, name);
        registrations.register(() -> reg.unregister());
    }

    private void unregisterRemoteNodeStore() throws IOException {
        if (registrations != null) {
            registrations.close();
            registrations = null;
        }
    }
}