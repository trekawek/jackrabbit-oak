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

import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.remote.common.PropertyDeserializer;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderId;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RemoteNodeStoreContext {

    private final IdRepository<NodeBuilderId> nodeBuilderIdRepository;

    private final RemoteNodeStoreClient client;

    private final PropertyDeserializer propertyDeserializer;

    private final BlobStore blobStore;

    private final ConcurrentMap<Long, NodeBuilderChangeQueue> changeQueueMap = new ConcurrentHashMap<>();

    public RemoteNodeStoreContext(RemoteNodeStoreClient client, BlobStore blobStore) {
        this.nodeBuilderIdRepository = new IdRepository<>(NodeBuilderId::getValue);
        this.client = client;
        this.blobStore = blobStore;
        this.propertyDeserializer = new PropertyDeserializer(blobId -> new BlobStoreBlob(blobStore, blobId));
    }

    public RemoteNodeStoreClient getClient() {
        return client;
    }

    public BlobStore getBlobStore() {
        return blobStore;
    }

    public NodeBuilderChangeQueue getNodeBuilderChangeQueue(NodeBuilderId nodeBuilderId) {
        return changeQueueMap.computeIfAbsent(
                nodeBuilderId.getValue(),
                id -> new NodeBuilderChangeQueue(client.getNodeBuilderService(), NodeBuilderId.newBuilder().setValue(id).build()));
    }

    public PropertyDeserializer getPropertyDeserializer() {
        return propertyDeserializer;
    }

    public void addNodeBuilderId(NodeBuilderId id) {
        nodeBuilderIdRepository.addId(id);
    }

    public void collectOrphanedReferences() {
        for (Long id : nodeBuilderIdRepository.getClearList()) {
            client.getNodeBuilderService().release(NodeBuilderId.newBuilder().setValue(id).build());
            changeQueueMap.remove(id);
        }
    }
}
