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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class RemoteNodeBuilder extends MemoryNodeBuilder {

    private static final Logger log = LoggerFactory.getLogger(RemoteNodeStore.class);

    private static final int UPDATE_LIMIT = Integer.getInteger("update.limit", 10000);

    private final RemoteNodeStoreContext context;

    private long updateCount;

    public RemoteNodeBuilder(RemoteNodeStoreContext context, RemoteNodeState base) {
        super(base);
        this.context = context;
        this.updateCount = 0;
    }

    protected RemoteNodeBuilder(RemoteNodeBuilder parent, String name) {
        super(parent, name);
        this.context = parent.context;
        this.updateCount = -1;
    }

    @Override
    protected void updated() {
        if (isChildBuilder()) {
            super.updated();
        } else {
            updateCount++;
            if (updateCount > UPDATE_LIMIT) {
                getNodeState();
            }
        }
    }

    private boolean isChildBuilder() {
        return updateCount < 0;
    }

    @NotNull
    @Override
    public NodeState getNodeState() {
        try {
            NodeState state = super.getNodeState();
            RecordId recordId = context.getSegmentWriter().writeNode(state);
            RemoteNodeState newNodeState = new RemoteNodeState(context, recordId.toString());
            set(newNodeState);
            if(!isChildBuilder()) {
                updateCount = 0;
            }
            return newNodeState;
        } catch (IOException e) {
            log.error("Error flushing changes", e);
            throw new IllegalStateException("Unexpected IOException", e);
        }
    }

    @Override
    protected RemoteNodeBuilder createChildBuilder(String name) {
        return new RemoteNodeBuilder(this, name);
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return new BlobStoreBlob(context.getBlobStore(), context.getBlobStore().writeBlob(stream));
    }

    public boolean isRootBuilder() {
        return isRoot();
    }
}