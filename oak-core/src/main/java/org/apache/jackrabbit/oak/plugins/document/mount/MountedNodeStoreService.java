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
package org.apache.jackrabbit.oak.plugins.document.mount;

import com.google.common.collect.Lists;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStateCache;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.plugins.document.secondary.SecondaryStoreCacheService;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

@Component(label = "Apache Jackrabbit Oak MountedNodeState Provider",
        metatype = true,
        policy = ConfigurationPolicy.REQUIRE,
        description = "Configures the MountedNodeStore"
)
public class MountedNodeStoreService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /*
     * Have an optional dependency on DocumentNodeStore such that we do not have hard dependency
     * on it and DocumentNodeStore can make use of this service even after it has unregistered
     */
    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC)
    private DocumentNodeStore documentNodeStore;

    @Reference(target = "(role=secondary)")
    private NodeStoreProvider mountedStoreProvider;

    @Reference
    private MountInfoProvider mountInfoProvider;

    private static final String PROP_MOUNT_NAME_DEFAULT = "private";
    @Property(
            value = PROP_MOUNT_NAME_DEFAULT,
            label = "Mount name",
            description = "Name of the mount configured in MountInfoProvider"
    )
    private static final String PROP_MOUNT_NAME = "mountName";

    private final List<ServiceRegistration> regs = Lists.newArrayList();

    private final MountedNodeStoreService.MultiplexingNodeStateDiffer differ = new MountedNodeStoreService.MultiplexingNodeStateDiffer();

    @Activate
    private void activate(BundleContext context, Map<String, Object> config) {
        String mountName = PropertiesUtil.toString(config.get(PROP_MOUNT_NAME), PROP_MOUNT_NAME_DEFAULT);
        MountedNodeStore mountedNodeStore = new MountedNodeStore(mountInfoProvider, mountedStoreProvider.getNodeStore(), mountName, differ);
        regs.add(context.registerService(DocumentNodeStateCache.class.getName(), mountedNodeStore, null));
    }

    @Deactivate
    private void deactivate(){
        for (ServiceRegistration r : regs){
            r.unregister();
        }
    }

    protected void bindDocumentNodeStore(DocumentNodeStore documentNodeStore){
        log.info("Registering DocumentNodeStore as the differ");
        differ.setDelegate(documentNodeStore);
    }

    protected void unbindDocumentNodeStore(DocumentNodeStore documentNodeStore){
        differ.setDelegate(NodeStateDiffer.DEFAULT_DIFFER);
    }

    private static class MultiplexingNodeStateDiffer implements NodeStateDiffer {
        private volatile NodeStateDiffer delegate = NodeStateDiffer.DEFAULT_DIFFER;
        @Override
        public boolean compare(@Nonnull AbstractDocumentNodeState node,
                               @Nonnull AbstractDocumentNodeState base, @Nonnull NodeStateDiff diff) {
            return delegate.compare(node, base, diff);
        }

        public void setDelegate(NodeStateDiffer delegate) {
            this.delegate = delegate;
        }
    }
}