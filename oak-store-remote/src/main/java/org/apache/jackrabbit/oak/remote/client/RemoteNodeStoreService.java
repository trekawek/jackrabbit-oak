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
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newIdentityHashSet;

@Component(policy = ConfigurationPolicy.REQUIRE, metatype = true)
public class RemoteNodeStoreService {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteNodeStoreService.class);

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY, policy = ReferencePolicy.STATIC)
    private BlobStore blobStore;

    @Reference
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;

    @Property(label = "Remote server host",
            description = "The host name of the remote server"
    )
    private static final String REMOTE_HOST = "remoteHost";

    @Property(label = "Remote server port",
            description = "The port number of the remote server",
            intValue = 12300
    )
    private static final String REMOTE_PORT = "remotePort";

    private ComponentContext context;

    private Set<NodeStoreProvider> nodeStoresInUse = newIdentityHashSet();

    private ServiceRegistration nsReg;

    private Closer mbeanRegistrations;

    private ObserverTracker observerTracker;

    private String remoteHost;

    private int remotePort;

    @Activate
    protected void activate(ComponentContext context, Map<String, ?> config) throws IOException, CommitFailedException {
        this.context = context;
        remoteHost = PropertiesUtil.toString(config.get(REMOTE_HOST), null);
        remotePort = PropertiesUtil.toInteger(config.get(REMOTE_PORT), 12300);
        registerRemoteNodeStore();
    }

    @Deactivate
    protected void deactivate() throws IOException {
        unregisterRemoteNodeStore();
    }

    private void registerRemoteNodeStore() throws IOException, CommitFailedException {
        if (nsReg != null) {
            return; // already registered
        }

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, RemoteNodeStore.class.getName());
        props.put("oak.nodestore.description", new String[] { "nodeStoreType=remote" } );

        RemoteNodeStoreClient client = new RemoteNodeStoreClient(remoteHost, remotePort);
        RemoteNodeStore store = new RemoteNodeStore(client, blobStore);

        observerTracker = new ObserverTracker(store);
        observerTracker.start(context.getBundleContext());

        Whiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());

        mbeanRegistrations = Closer.create();
        registerMBean(whiteboard,
                CheckpointMBean.class,
                new RemoteCheckpointMBean(store),
                CheckpointMBean.TYPE,
                "Remote node store checkpoint management");

        LOG.info("Registering the remote node store");
        nsReg = context.getBundleContext().registerService(
                new String[]{
                        NodeStore.class.getName()
                },
                store,
                props);
    }

    private <T> void registerMBean(Whiteboard whiteboard,
                                   Class<T> iface, T bean, String type, String name) {
        Registration reg = WhiteboardUtils.registerMBean(whiteboard, iface, bean, type, name);
        mbeanRegistrations.register(() -> reg.unregister());
    }

    private void unregisterRemoteNodeStore() throws IOException {
        if (nsReg != null) {
            LOG.info("Unregistering the composite node store");
            nsReg.unregister();
            nsReg = null;
        }
        if (mbeanRegistrations != null) {
            mbeanRegistrations.close();
            mbeanRegistrations = null;
        }
        if (observerTracker != null) {
            observerTracker.stop();
            observerTracker = null;
        }
        nodeStoresInUse.clear();
    }
}