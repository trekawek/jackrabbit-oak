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
package org.apache.jackrabbit.oak.remote.client;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.remote.common.PropertyDeserializer;
import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

public class RemoteNodeStoreContext {

    private static final NodeValueProtos.NodeValue EMPTY_NODE_VALUE = NodeValueProtos.NodeValue.newBuilder().setExists(false).build();

    private final RemoteNodeStoreClient client;

    private final PropertyDeserializer propertyDeserializer;

    private final BlobStore blobStore;

    private final LoadingCache<String, NodeValueProtos.NodeValue> nodeValueCache;

    public RemoteNodeStoreContext(RemoteNodeStoreClient client, BlobStore blobStore) {
        this.client = client;
        this.blobStore = blobStore;
        this.propertyDeserializer = new PropertyDeserializer(blobId -> new BlobStoreBlob(blobStore, blobId));
        this.nodeValueCache = CacheBuilder.newBuilder()
            .maximumSize(10240)
            .build(new CacheLoader<String, NodeValueProtos.NodeValue>() {
                @Override
                public NodeValueProtos.NodeValue load(String key) {
                    return client.getNodeStateService().getNodeValue(NodeStateProtos.NodeStateId.newBuilder().setRevision(key).build());
                }
            });
    }

    public RemoteNodeStoreClient getClient() {
        return client;
    }

    public BlobStore getBlobStore() {
        return blobStore;
    }

    public PropertyDeserializer getPropertyDeserializer() {
        return propertyDeserializer;
    }

    public NodeValueProtos.NodeValue loadNodeValue(NodeStateProtos.NodeStateId id) {
        String revision = id.getRevision();
        if (Strings.isNullOrEmpty(revision)) {
            return EMPTY_NODE_VALUE;
        }
        return nodeValueCache.getUnchecked(revision);
    }

}
