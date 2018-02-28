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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.AbstractIterator;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileBufferLinkedList.NodeIterator;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.singletonIterator;

class FlatFileStoreIterator extends AbstractIterator<NodeStateEntry> implements Iterator<NodeStateEntry> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Iterator<NodeStateEntry> baseItr;
    private final FlatFileBufferLinkedList buffer = new FlatFileBufferLinkedList();
    private NodeStateEntry current;
    private final Set<String> preferredPathElements;
    private int maxBufferSize;

    public FlatFileStoreIterator(Iterator<NodeStateEntry> baseItr, Set<String> preferredPathElements) {
        this.baseItr = baseItr;
        this.preferredPathElements = preferredPathElements;
    }

    int getBufferSize(){
        return buffer.size();
    }

    @Override
    protected NodeStateEntry computeNext() {
        //TODO Add some checks on expected ordering
        current = computeNextEntry();
        if (current == null) {
            log.info("Max buffer size in complete traversal is [{}]", maxBufferSize);
            return endOfData();
        } else {
            return current;
        }
    }

    private NodeStateEntry computeNextEntry() {
        if (buffer.size() > maxBufferSize) {
            maxBufferSize = buffer.size();
            log.info("Max buffer size changed {} for path {}", maxBufferSize, current.getPath());
        }
        if (!buffer.isEmpty()) {
            return buffer.remove();
        }
        if (baseItr.hasNext()) {
            return wrap(baseItr.next());
        }
        return null;
    }

    private NodeStateEntry wrap(NodeStateEntry baseEntry) {
        NodeState state = new LazyChildrenNodeState(baseEntry.getNodeState(),
                new ChildNodeStateProvider(getEntries(), baseEntry.getPath(), preferredPathElements));
        return new NodeStateEntry(state, baseEntry.getPath());
    }

    private Iterable<NodeStateEntry> getEntries() {
        return () -> concat(singletonIterator(current), queueIterator());
    }

    private Iterator<NodeStateEntry> queueIterator() {
        NodeIterator qitr = buffer.iterator();
        return new AbstractIterator<NodeStateEntry>() {
            @Override
            protected NodeStateEntry computeNext() {
                //If queue is empty try to append by getting entry from base
                if (!qitr.hasNext() && baseItr.hasNext()) {
                    buffer.add(wrap(baseItr.next()));
                }
                if (qitr.hasNext()) {
                    return qitr.next();
                }
                return endOfData();
            }
        };
    }
}
