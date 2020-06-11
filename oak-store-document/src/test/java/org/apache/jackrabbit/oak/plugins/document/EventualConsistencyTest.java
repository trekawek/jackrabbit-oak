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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.Collections;

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

        DocumentNodeStore nodeStore2 = DocumentNodeStoreBuilder
                .newDocumentNodeStoreBuilder()
                .setDocumentStore(docStore)
                .setAsyncDelay(1)
                .build();

        NodeState root = nodeStore1.getRoot();
        NodeBuilder rootBuilder = nodeStore1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            NodeBuilder child = rootBuilder.child("child-" + i);
            for (int j = 0; j < 10; j++) {
                child.child("test-" + j);
            }
        }
        DocumentNodeState newRoot = (DocumentNodeState) nodeStore1.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        nodeStore1.getClusterInfo().setInfo(Collections.singletonMap("lastRevision", newRoot.getLastRevision().getRevision(nodeStore1.getClusterId()).toString()));

        while (true) {
            RevisionVector lastRootRevisionVector = nodeStore2.getRoot().getLastRevision();

            boolean allRevisionsPersisted = true;
            for (ClusterNodeInfoDocument doc : ClusterNodeInfoDocument.all(docStore)) {
                if (!doc.isActive()) {
                    continue;
                }
                String lastRevisionString = (String) doc.get("lastRevision");
                if (lastRevisionString == null) {
                    continue;
                }
                Revision lastRevision = Revision.fromString(lastRevisionString);
                Revision lastRootRevision = lastRootRevisionVector.getRevision(doc.getClusterId());
                if (lastRevision.compareRevisionTime(lastRootRevision) > 0) {
                    allRevisionsPersisted = false;
                    break;
                }
            }
            if (allRevisionsPersisted) {
                break;
            }
            Thread.sleep(10);
        }

        assertTrue(nodeStore2.getRoot().hasChildNode("child-0"));
    }
}
