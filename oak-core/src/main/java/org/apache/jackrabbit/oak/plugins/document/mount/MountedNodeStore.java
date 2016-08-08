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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStateCache;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static com.google.common.base.Preconditions.checkNotNull;

public class MountedNodeStore implements DocumentNodeStateCache {

    private static RevisionVector LAST_REV = new RevisionVector(new Revision(0, 0, 1));

    private static RevisionVector ROOT_REV = new RevisionVector(new Revision(0, 0, 1));

    private final MountInfoProvider mountInfoProvider;

    private final NodeStore nodeStore;

    private final String mountName;

    private final NodeStateDiffer differ;

    public MountedNodeStore(MountInfoProvider mountInfoProvider, NodeStore nodeStore, String mountName, NodeStateDiffer differ) {
        this.mountInfoProvider = checkNotNull(mountInfoProvider);
        this.nodeStore = checkNotNull(nodeStore);
        this.mountName = checkNotNull(mountName);
        this.differ = differ;
    }

    @Override
    public AbstractDocumentNodeState getDocumentNodeState(String path, RevisionVector rootRevision, RevisionVector lastRev) {
        Mount mount = mountInfoProvider.getMountByPath(path);
        if (mountName.equals(mount.getName())) {
            return getMounted(path);
        } else {
            return null;
        }
    }

    public AbstractDocumentNodeState getMounted(String path) {
        NodeState state = nodeStore.getRoot();
        for(String element : PathUtils.elements(path)) {
            state = state.getChildNode(element);
        }
        if (state instanceof AbstractDocumentNodeState) {
            return (AbstractDocumentNodeState) state;
        } else {
            return MountedDocumentNodeState.wrap(state, path, LAST_REV, ROOT_REV, differ);
        }
    }
}
