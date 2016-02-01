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
package org.apache.jackrabbit.oak.plugins.document.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.cache.async.CacheActionQueue;
import org.apache.jackrabbit.oak.plugins.document.cache.async.InvalidateAction;
import org.apache.jackrabbit.oak.plugins.document.cache.async.InvalidateOutdatedAction;
import org.apache.jackrabbit.oak.plugins.document.cache.async.PutAction;
import org.apache.jackrabbit.oak.plugins.document.cache.async.PutIfAbsentAction;
import org.apache.jackrabbit.oak.plugins.document.cache.async.PutIfNewerAction;
import org.apache.jackrabbit.oak.plugins.document.cache.async.ReplaceAction;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isLeafPreviousDocId;

/**
 * Cache for the NodeDocuments. This class uses an asynchronous queue to put objects into cache.
 */
public class AsynchronousNodeDocumentCache implements NodeDocumentCache {

    private static final Logger LOG = LoggerFactory.getLogger(AsynchronousNodeDocumentCache.class);

    private final Cache<CacheValue, NodeDocument> nodeDocumentsCache;
    private final CacheStats nodeDocumentsCacheStats;
    private final CacheActionQueue queue;
    private final Thread queueThread;

    /**
     * The previous documents cache
     *
     * Key: StringValue, value: NodeDocument
     */
    private final Cache<CacheValue, NodeDocument> prevDocumentsCache;
    private final CacheStats prevDocumentsCacheStats;
    private final CacheActionQueue prevQueue;
    private final Thread prevQueueThread;

    private final NodeDocumentLocks locks;

    public AsynchronousNodeDocumentCache(@Nonnull Cache<CacheValue, NodeDocument> nodeDocumentsCache,
                             @Nonnull CacheStats nodeDocumentsCacheStats,
                             @Nonnull Cache<CacheValue, NodeDocument> prevDocumentsCache,
                             @Nonnull CacheStats prevDocumentsCacheStats,
                             @Nonnull NodeDocumentLocks locks) {
        this.nodeDocumentsCache = nodeDocumentsCache;
        this.nodeDocumentsCacheStats = nodeDocumentsCacheStats;
        this.prevDocumentsCache = prevDocumentsCache;
        this.prevDocumentsCacheStats = prevDocumentsCacheStats;
        this.locks = locks;
        this.queue = new CacheActionQueue(nodeDocumentsCache);
        this.queueThread = new Thread(queue, "Oak CacheActionQueue #1");
        this.queueThread.setDaemon(true);
        this.queueThread.start();

        this.prevQueue = new CacheActionQueue(prevDocumentsCache);
        this.prevQueueThread = new Thread(prevQueue, "Oak CacheActionQueue #2");
        this.prevQueueThread.setDaemon(true);
        this.prevQueueThread.start();
    }

    /**
     * Invalidate document with given key.
     *
     * @param key to invalidate
     */
    @Override
    public void invalidate(@Nonnull String key) {
        Lock lock = locks.acquire(key);
        try {
            getQueue(key).addAction(new InvalidateAction(key));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Invalidate document with given keys iff their mod counts are different as
     * passed in the map.
     *
     * @param modCounts map where key is the document id and the value is the mod count
     * @return number of invalidated entries
     */
    @Override
    @Nonnegative
    public int invalidateOutdated(@Nonnull Map<String, Long> modCounts) {
        Lock lock = locks.acquire(modCounts.keySet());
        try {
            queue.addAction(new InvalidateOutdatedAction(modCounts));
            prevQueue.addAction(new InvalidateOutdatedAction(modCounts));
            return -1;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return the cached value or null.
     *
     * @param key document key
     * @return cached value of null if there's no document with given key cached
     */
    @Override
    @CheckForNull
    public NodeDocument getIfPresent(@Nonnull String key) {
        return getQueue(key).get(key);
    }

    /**
     * Puts document into cache.
     *
     * @param doc document to put
     */
    @Override
    public void put(@Nonnull NodeDocument doc) {
        if (doc != NodeDocument.NULL) {
            Lock lock = locks.acquire(doc.getId());
            try {
                getQueue(doc.getId()).addAction(new PutAction(doc));
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Puts document into cache iff no entry with the given key is cached
     * already or the cached document is older (has smaller {@link Document#MOD_COUNT}).
     *
     * @param doc the document to add to the cache
     * @return either the given <code>doc</code> or the document already present
     *         in the cache if it's newer
     */
    @Override
    public void putIfNewer(@Nonnull final NodeDocument doc) {
        if (doc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        doc.seal();

        String id = doc.getId();
        Lock lock = locks.acquire(id);
        try {
            getQueue(doc.getId()).addAction(new PutIfNewerAction(doc));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Puts document into cache iff no entry with the given key is cached
     * already. This operation is atomic.
     *
     * @param doc the document to add to the cache.
     * @return either the given <code>doc</code> or the document already present
     *         in the cache.
     */
    @Override
    public void putIfAbsent(@Nonnull final NodeDocument doc) {
        if (doc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        doc.seal();

        Lock lock = locks.acquire(doc.getId());
        try {
            getQueue(doc.getId()).addAction(new PutIfAbsentAction(doc));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Replaces the cached value if the old document is currently present in
     * the cache. If the {@code oldDoc} is not cached, nothing will happen. If
     * {@code oldDoc} does not match the document currently in the cache, then
     * the cached document is invalidated.
     *
     * @param oldDoc the old document
     * @param newDoc the replacement
     */
    public void replaceCachedDocument(@Nonnull final NodeDocument oldDoc,
                                      @Nonnull final NodeDocument newDoc) {
        if (newDoc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        Lock lock = locks.acquire(oldDoc.getId());
        try {
            getQueue(oldDoc.getId()).addAction(new ReplaceAction(oldDoc, newDoc));
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return keys stored in cache
     */
    @Override
    public Iterable<CacheValue> keys() {
        return Iterables.concat(nodeDocumentsCache.asMap().keySet(), prevDocumentsCache.asMap().keySet());
    }

    /**
     * @return values stored in cache
     */
    @Override
    public Iterable<NodeDocument> values() {
        return Iterables.concat(nodeDocumentsCache.asMap().values(), prevDocumentsCache.asMap().values());
    }

    @Override
    public Iterable<CacheStats> getCacheStats() {
        return Lists.newArrayList(nodeDocumentsCacheStats, prevDocumentsCacheStats);
    }

    @Override
    public void close() throws IOException {
        queue.stop();
        prevQueue.stop();

        try {
            queueThread.join();
            prevQueueThread.join();
        } catch(InterruptedException e) {
            LOG.error("Can't join the queue threads", e);
        }

        if (prevDocumentsCache instanceof Closeable) {
            ((Closeable) prevDocumentsCache).close();
        }
        if (nodeDocumentsCache instanceof Closeable) {
            ((Closeable) nodeDocumentsCache).close();
        }
    }

    private CacheActionQueue getQueue(String id) {
        if (isLeafPreviousDocId(id)) {
            return prevQueue;
        } else {
            return queue;
        }
    }
}
