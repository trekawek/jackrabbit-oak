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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.filterValues;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_HYBRID_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.isLuceneIndexNode;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

class IndexTracker {

    /** Logger instance. */
    private static final Logger log = LoggerFactory.getLogger(IndexTracker.class);
    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(IndexTracker.class.getName() + ".perf"));

    private final MemoryDirectoryStorage directoryStorage;

    private final IndexCopier cloner;

    private final MonitoringBackgroundObserver memoryIndexObserver;

    private NodeState root = EMPTY_NODE;

    private volatile Map<String, IndexNode> indices = emptyMap();

    private volatile Map<String, IndexNode> memoryIndices = emptyMap();

    private volatile boolean refresh;

    IndexTracker() {
        this(null, null, null);
    }

    IndexTracker(MemoryDirectoryStorage directoryStorage) {
        this(directoryStorage, null, null);
    }

    IndexTracker(IndexCopier cloner) {
        this(null, cloner, null);
    }

    IndexTracker(MemoryDirectoryStorage directoryStorage, IndexCopier cloner, MonitoringBackgroundObserver memoryIndexObserver) {
        this.directoryStorage = directoryStorage;
        this.cloner = cloner;
        this.memoryIndexObserver = memoryIndexObserver;
    }

    synchronized void close() {
        Map<String, IndexNode> indices = this.indices;
        this.indices = emptyMap();

        for (Map.Entry<String, IndexNode> entry : indices.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.error("Failed to close the Lucene index at " + entry.getKey(), e);
            }
        }
    }

    synchronized void update(final NodeState root) {
        if (refresh) {
            this.root = root;
            close();
            refresh = false;
            log.info("Refreshed the opened indexes");
        } else {
            diffAndUpdate(this.root, root, false);
            this.root = root;
        }
    }

    void diffAndUpdate(final NodeState before, final NodeState after, final boolean inMemory) {
        final Map<String, IndexNode> original = getIndices(inMemory);
        final Map<String, IndexNode> updates = newHashMap();

        List<Editor> editors = newArrayListWithCapacity(original.size());
        for (Map.Entry<String, IndexNode> entry : original.entrySet()) {
            final String path = entry.getKey();

            editors.add(new SubtreeEditor(new DefaultEditor() {
                @Override
                public void leave(NodeState before, NodeState after) {
                    try {
                        long start = PERF_LOGGER.start();
                        IndexNode index = IndexNode.open(path, root, after, cloner, directoryStorage, inMemory);
                        PERF_LOGGER.end(start, -1, "[{}] Index found to be updated. Reopening the IndexNode", path);
                        updates.put(path, index); // index can be null
                    } catch (IOException e) {
                        log.error("Failed to open Lucene index at " + path, e);
                    }
                }
            }, Iterables.toArray(PathUtils.elements(path), String.class)));
        }

        EditorDiff.process(CompositeEditor.compose(editors), before, after);

        if (!updates.isEmpty()) {
            Set<String> purged = new HashSet<String>();
            for (String path : updates.keySet()) {
                IndexNode index = original.get(path);

                if (!inMemory && index.getDefinition().isHybridIndex()) {
                    purgeMemoryIndex(path);
                    purged.add(path);
                }
            }

            Map<String, IndexNode> result = ImmutableMap.<String, IndexNode>builder()
                    .putAll(filterKeys(original, not(in(updates.keySet()))))
                    .putAll(filterValues(updates, notNull()))
                    .build();

            setIndices(result, inMemory);

            if (!purged.isEmpty()) {
                memoryIndices = ImmutableMap.<String, IndexNode>builder()
                    .putAll(filterKeys(memoryIndices, not(in(purged))))
                    .build();
            }

            //This might take some time as close need to acquire the
            //write lock which might be held by current running searches
            //Given that Tracker is now invoked from a BackgroundObserver
            //not a high concern
            for (String path : updates.keySet()) {
                IndexNode index = original.get(path);
                try {
                    index.close();
                } catch (IOException e) {
                    log.error("Failed to close Lucene index at " + path, e);
                }
            }
        }
    }

    void refresh() {
        refresh = true;
    }

    IndexNode acquireIndexNode(String path) {
        return acquireIndexNode(path, false);
    }


    IndexNode acquireIndexNode(String path, boolean inMemory) {
        if (inMemory && memoryIndexObserver != null) {
            try {
                memoryIndexObserver.waitUntilProcessingIsFinished();
            } catch (InterruptedException e) {
                log.error("The memory index may not contain the recent changes", e);
            }
        }
        IndexNode index = getIndices(inMemory).get(path);
        if (index != null && index.acquire()) {
            return index;
        } else {
            return findIndexNode(path, inMemory);
        }
    }

    Set<String> getIndexNodePaths(){
        return indices.keySet();
    }

    private synchronized IndexNode findIndexNode(String path, boolean inMemory) {
        // Retry the lookup from acquireIndexNode now that we're
        // synchronized. The acquire() call is guaranteed to succeed
        // since the close() method is also synchronized.
        IndexNode index = getIndices(inMemory).get(path);
        if (index != null) {
            checkState(index.acquire());
            return index;
        }

        NodeState node = root;
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
        }

        try {
            if (isLuceneIndexNode(node) && (!inMemory || node.getBoolean(PROP_HYBRID_INDEX))) {
                index = IndexNode.open(path, root, node, cloner, directoryStorage, inMemory);
                if (index != null) {
                    checkState(index.acquire());
                    putIndex(path, index, inMemory);
                    return index;
                }
            } else if (node.exists()) {
                log.warn("Cannot open Lucene Index at path {} as the index is not of type {}", path, TYPE_LUCENE);
            }
        } catch (IOException e) {
            log.error("Could not access the Lucene index at " + path, e);
        }

        return null;
    }

    private void purgeMemoryIndex(String indexPath) {
        directoryStorage.purge(indexPath);
        IndexNode node = memoryIndices.get(indexPath);
        try {
            if (node != null) {
                node.close();
            }
        } catch (IOException e) {
            log.error("Can't close node", e);
        }
    }

    private void setIndices(Map<String, IndexNode> indices, boolean inMemory) {
        if (inMemory) {
            this.memoryIndices = indices;
        } else {
            this.indices = indices;
        }
    }

    private Map<String, IndexNode> getIndices(boolean inMemory) {
        return inMemory ? memoryIndices : indices;
    }

    private void putIndex(String path, IndexNode index, boolean inMemory) {
        setIndices(ImmutableMap.<String, IndexNode>builder()
                .putAll(getIndices(inMemory))
                .put(path, index)
                .build(), inMemory);
    }

}
