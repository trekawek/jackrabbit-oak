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
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EventualConsistencyTest {

    @Test
    public void testEventualConsistency() throws Exception {
        DocumentStore docStore = new MemoryDocumentStore();

        DocumentNodeStore nodeStore1 = DocumentNodeStoreBuilder
                .newDocumentNodeStoreBuilder()
                .setDocumentStore(docStore)
                .setAsyncDelay(1)
                .build();
        nodeStore1.addObserver(new RevisionObserver(nodeStore1.getClusterInfo()));

        NodeBuilder rootBuilder = nodeStore1.getRoot().builder();
        rootBuilder.child("foo").child("bar");
        nodeStore1.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        DocumentNodeStore nodeStore2 = DocumentNodeStoreBuilder
                .newDocumentNodeStoreBuilder()
                .setDocumentStore(docStore)
                .setAsyncDelay(1)
                .build();
        nodeStore2.addObserver(new RevisionObserver(nodeStore2.getClusterInfo()));
        
        UpdateWatcher updateWatcher = new UpdateWatcher(nodeStore2);

        rootBuilder = nodeStore1.getRoot().builder();
        rootBuilder.getChildNode("foo").getChildNode("bar").setProperty("newProperty", 123);
        nodeStore1.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse(nodeStore2.getRoot().getChildNode("foo").getChildNode("bar").hasProperty("newProperty"));
        updateWatcher.waitForPendingUpdates();
        assertTrue(nodeStore2.getRoot().getChildNode("foo").getChildNode("bar").hasProperty("newProperty"));
    }

    /**
     * This observer synchronously persists the most recent revision seen in the
     * repository as a cluster node info property.
     */
    public static class RevisionObserver implements Observer {

        private static final String LAST_REVISION = "lastRevision";

        private final ClusterNodeInfo clusterNodeInfo;

        public RevisionObserver(ClusterNodeInfo clusterNodeInfo) {
            this.clusterNodeInfo = clusterNodeInfo;
        }

        @Override
        public void contentChanged(@NotNull NodeState root, @NotNull CommitInfo info) {
            if (info.isExternal()) {
                return;
            }

            if (!(root instanceof DocumentNodeState)) {
                throw new IllegalArgumentException("Expected a DocumentNodeState");
            }

            RevisionVector lastRevisions = ((DocumentNodeState) root).getLastRevision();
            Revision lastRevision = lastRevisions.getRevision(clusterNodeInfo.getId());
            clusterNodeInfo.setInfo(Collections.singletonMap(LAST_REVISION, lastRevision.toString()));
        }
    }

    /**
     * This class allows to wait until all the pending revisions, received from
     * the cluster node info properties, are already persisted in the repository
     * and fetched by the background read operation.
     *
     * The list of pending revisions is compiled just once, right after calling
     * the #waitForPendingUpdates(). This means that the method waits until
     * the changes already made when the method is called are available, but
     * not any changes made *after* calling this method. This way we're avoiding
     * an infinite loop, in case there's another instance constantly committing
     * to repository.
     */
    public static class UpdateWatcher {

        private final DocumentNodeStore nodeStore;

        public UpdateWatcher(DocumentNodeStore nodeStore) {
            this.nodeStore = nodeStore;
        }

        public void waitForPendingUpdates() throws InterruptedException {
            RevisionVector minimumRevisions = null;
            while (true) {
                List<ClusterNodeInfoDocument> instances = ClusterNodeInfoDocument.all(nodeStore.getDocumentStore());
                if (minimumRevisions == null) {
                    minimumRevisions = getMinimumRevisions(instances);
                }
                if (allRevisionsPersisted(minimumRevisions, instances)) {
                    break;
                } else {
                    Thread.sleep(500);
                }
            }
        }

        private boolean allRevisionsPersisted(RevisionVector minimumRevisions, List<ClusterNodeInfoDocument> instances) {
            RevisionVector currentRootRevisions = nodeStore.getRoot().getLastRevision();

            boolean allRevisionsPersisted = true;
            for (ClusterNodeInfoDocument instance : instances) {
                if (!instance.isActive()) {
                    continue;
                }

                int clusterId = instance.getClusterId();
                Revision minRevision = minimumRevisions.getRevision(clusterId);
                if (minRevision == null) {
                    continue;
                }
                Revision currentRootRevision = currentRootRevisions.getRevision(clusterId);
                if (currentRootRevision.compareRevisionTime(minRevision) < 0) {
                    allRevisionsPersisted = false;
                }
            }
            return allRevisionsPersisted;
        }

        private static RevisionVector getMinimumRevisions(List<ClusterNodeInfoDocument> instances) {
            RevisionVector vector = new RevisionVector();
            for (ClusterNodeInfoDocument instance : instances) {
                String lastRevision = (String) instance.get(RevisionObserver.LAST_REVISION);
                if (lastRevision == null) {
                    continue;
                }
                vector = vector.update(Revision.fromString(lastRevision));
            }
            return vector;
        }
    }
}
