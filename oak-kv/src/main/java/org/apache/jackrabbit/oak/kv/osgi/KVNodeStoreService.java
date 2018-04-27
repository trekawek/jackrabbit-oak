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

package org.apache.jackrabbit.oak.kv.osgi;

import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.io.File;

import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.kv.KVNodeStore;
import org.apache.jackrabbit.oak.kv.store.leveldb.LevelDBStore;
import org.apache.jackrabbit.oak.kv.store.memory.MemoryStore;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
public class KVNodeStoreService {

    @Reference
    private BlobStore blobStore;

    private LevelDBStore store;

    @Activate
    public void activate(ComponentContext context) throws Exception {
        store = new LevelDBStore(getPath(context));
        KVNodeStore nodeStore = new KVNodeStore(new MemoryStore(), blobStore);

        Whiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
        whiteboard.register(NodeStore.class, nodeStore, emptyMap());
        whiteboard.register(Descriptors.class, new KVDiscoveryLiteDescriptors(nodeStore), emptyMap());
        registerMBean(whiteboard, CheckpointMBean.class, new KVCheckpointMBean(nodeStore), CheckpointMBean.TYPE, "Oak KV Checkpoints");
    }

    @Deactivate
    public void deactivate() throws Exception {
        store.close();
    }

    private static File getPath(ComponentContext context) {
        String path = PropertiesUtil.toString(context.getProperties().get("path"), null);
        if (path == null) {
            throw new IllegalStateException("path");
        }
        return new File(path);
    }

}
