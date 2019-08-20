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

import java.io.IOException;
import java.io.InputStream;

public class RemoteNodeBuilder extends MemoryNodeBuilder {

    private final RemoteNodeStoreContext context;

    public RemoteNodeBuilder(RemoteNodeStoreContext context, RemoteNodeState base) {
        super(base);
        this.context = context;
    }

    protected RemoteNodeBuilder(RemoteNodeBuilder parent, String name) {
        super(parent, name);
        this.context = parent.context;
    }

    @Override
    protected RemoteNodeBuilder createChildBuilder(String name) {
        return new RemoteNodeBuilder(this, name);
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return new BlobStoreBlob(context.getBlobStore(), context.getBlobStore().writeBlob(stream));
    }

}
