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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.spi.state.EqualsDiff.modified;

/**
 * Update the lucene-memory indexes.
 */
public class LuceneMemoryUpdater implements Observer {

    private static final Logger log = LoggerFactory.getLogger(LuceneMemoryUpdater.class);

    private static final IndexUpdateCallback NOOP_CALLBACK =
            new IndexUpdateCallback() {
                @Override
                public void indexUpdate() {
                    // do nothing
                }
            };

    private final LuceneMemoryIndexEditorProvider memoryEditorProvider;

    private final IndexTracker tracker;

    private final LocalBranch indexDefBranch;

    private NodeState before;

    public LuceneMemoryUpdater(MemoryDirectoryStorage directoryStorage, IndexTracker tracker) {
        this.memoryEditorProvider = new LuceneMemoryIndexEditorProvider(directoryStorage);
        this.tracker = tracker;
        this.indexDefBranch = new LocalBranch();
   }

    @Override
    public void contentChanged(@Nonnull final NodeState root, @Nullable CommitInfo info) {
        if (before != null) {
            process(root);
        } else {
            before = indexDefBranch.rebase(root);
        }
    }

    private void process(NodeState root) {
        NodeState after = indexDefBranch.rebase(root);
        NodeBuilder rootBuilder = after.builder();

        // update the memory indices
        IndexUpdate updatingEditor = new IndexUpdate(memoryEditorProvider, null, after, rootBuilder, NOOP_CALLBACK).withMissingProviderStrategy(new IndexUpdate.MissingIndexProviderStrategy() {
            @Override
            public void onMissingIndex(String type, NodeBuilder definition, String indexPath) throws CommitFailedException {
                if (TYPE_LUCENE.equals(type)) { // ignore index types different than lucene
                    super.onMissingIndex(type, definition, indexPath);
                }
            }
        });
        EditorDiff.process(updatingEditor, before, after);
        NodeState newState = rootBuilder.getNodeState();

        // merge the definition changes made by the update process into the branch
        indexDefBranch.merge(newState);

        // refresh the index nodes in the tracker
        tracker.diffAndUpdate(after, newState, true);
        before = newState;
    }


    /**
     * Updating an index involves modifying its definition in /oak:index. We don't want to do this for the lucene-memory
     * indexes, which are stored locally and not shared across the cluster. That's why we have a local branch, containing
     * the lucene-memory definition modifications.
     */
    private static class LocalBranch {

        private NodeState branchMemoryIndexDefs;

        private NodeState baseMemoryIndexDefs;

        /**
         * Rebase the index definition modifications from this branch onto the root. If there's a conflict caused by the
         * external change of the definitions, the branch will be reset.
         *
         * @param root the node state root to rebase the changes onto
         * @return rebased branch root
         */
        public NodeState rebase(NodeState root) {
            NodeState newIndexDefs = extractHybridDefinitions(root);
            if (baseMemoryIndexDefs == null || modified(baseMemoryIndexDefs, newIndexDefs)) {
                baseMemoryIndexDefs = newIndexDefs;
                branchMemoryIndexDefs = transformDefinitions(newIndexDefs);
                log.debug("The memory index definition has been modified externally. Resetting the observer branch.");
            }

            NodeBuilder rootBuilder = root.builder();
            NodeBuilder indexDefs = rootBuilder.getChildNode(INDEX_DEFINITIONS_NAME);
            for (ChildNodeEntry memoryIndexDef : branchMemoryIndexDefs.getChildNodeEntries()) {
                indexDefs.setChildNode(memoryIndexDef.getName(), memoryIndexDef.getNodeState());
            }
            return rootBuilder.getNodeState();
        }

        /**
         * Merge the definitions changes.
         *
         * @param root the new root of the branch.
         */
        public void merge(NodeState root) {
            branchMemoryIndexDefs = extractHybridDefinitions(root);
        }

        private static NodeState extractHybridDefinitions(NodeState root) {
            NodeBuilder builder = EMPTY_NODE.builder();
            NodeState indexDefs = root.getChildNode(INDEX_DEFINITIONS_NAME);
            for (ChildNodeEntry entry : indexDefs.getChildNodeEntries()) {
                if (entry.getNodeState().getBoolean(LuceneIndexConstants.PROP_HYBRID_INDEX)) {
                    builder.setChildNode(entry.getName(), entry.getNodeState());
                }
            }
            return builder.getNodeState();
        }

        private NodeState transformDefinitions(NodeState indexDefs) {
            NodeBuilder builder = indexDefs.builder();
            for (String name : builder.getChildNodeNames()) {
                NodeBuilder def = builder.getChildNode(name);
                def.removeProperty(ASYNC_PROPERTY_NAME);
            }
            return builder.getNodeState();
        }
    }
}