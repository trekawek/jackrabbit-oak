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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.cache.prefetch.PrefetchAlgorithm;
import org.apache.jackrabbit.oak.plugins.document.cache.prefetch.PrefetchTimeSeries;
import org.apache.jackrabbit.oak.plugins.document.cache.prefetch.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PrefetchingDocumentStoreWrapper implements DocumentStore, RevisionListener {

    private static final Logger LOG = LoggerFactory.getLogger(PrefetchingDocumentStoreWrapper.class);

    private final Cache<String, PrefetchTimeSeries> sessions;

    private final DocumentStore store;

    private final PrefetchAlgorithm prefetchAlgorithm;

    public PrefetchingDocumentStoreWrapper(DocumentStore store, PrefetchAlgorithm prefetchAlgorithm) {
        this.store = store;
        this.prefetchAlgorithm = prefetchAlgorithm;
        this.sessions = CacheBuilder.<String, PrefetchTimeSeries>newBuilder()
                .expireAfterWrite(2, TimeUnit.SECONDS)
                .removalListener((RemovalListener<String, PrefetchTimeSeries>) n -> n.getValue().onClose())
                .build();
    }

    private void handleRequest(Request request) {
        sessions.cleanUp();

        String threadName = request.getThreadName();
        if (!threadName.endsWith("HTTP/1.1")) {
            return;
        }
        PrefetchTimeSeries series = null;
        try {
            series = sessions.get(threadName, () -> prefetchAlgorithm.newSession());
        } catch (ExecutionException e) {
            LOG.error("Can't create new session", e);
        }
        series.onRequest(request);
    }

    @CheckForNull
    @Override
    public <T extends Document> T find(Collection<T> collection, String key) throws DocumentStoreException {
        if (collection == Collection.NODES) {
            handleRequest(Request.createFindRequest(Thread.currentThread().getName(), key));
        }
        return store.find(collection, key);
    }

    @CheckForNull
    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) throws DocumentStoreException {
        if (collection == Collection.NODES) {
            handleRequest(Request.createFindRequest(Thread.currentThread().getName(), key));
        }
        return store.find(collection, key, maxCacheAge);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) throws DocumentStoreException {
        if (collection == Collection.NODES) {
            handleRequest(Request.createQueryRequest(Thread.currentThread().getName(), fromKey, toKey));
        }
        return store.query(collection, fromKey, toKey, limit);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, String indexedProperty, long startValue, int limit) throws DocumentStoreException {
        if (collection == Collection.NODES) {
            handleRequest(Request.createQueryRequest(Thread.currentThread().getName(), fromKey, toKey, indexedProperty, startValue));
        }
        return store.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) throws DocumentStoreException {
        store.remove(collection, key);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) throws DocumentStoreException {
        store.remove(collection, keys);
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Map<UpdateOp.Key, UpdateOp.Condition>> toRemove) throws DocumentStoreException {
        return store.remove(collection, toRemove);
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, String indexedProperty, long startValue, long endValue) throws DocumentStoreException {
        return store.remove(collection, indexedProperty, startValue, endValue);
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) throws IllegalArgumentException, DocumentStoreException {
        return store.create(collection, updateOps);
    }

    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) throws IllegalArgumentException, DocumentStoreException {
        store.update(collection, keys, updateOp);
    }

    @CheckForNull
    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) throws IllegalArgumentException, DocumentStoreException {
        return store.createOrUpdate(collection, update);
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection, List<UpdateOp> updateOps) throws DocumentStoreException {
        return store.createOrUpdate(collection, updateOps);
    }

    @CheckForNull
    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) throws DocumentStoreException {
        return store.findAndUpdate(collection, update);
    }

    @CheckForNull
    @Override
    public CacheInvalidationStats invalidateCache() {
        return store.invalidateCache();
    }

    @CheckForNull
    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        return store.invalidateCache(keys);
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        store.invalidateCache(collection, key);
    }

    @Override
    public void dispose() {
        store.dispose();
    }

    @CheckForNull
    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String key) {
        return store.getIfCached(collection, key);
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        store.setReadWriteMode(readWriteMode);
    }

    @CheckForNull
    @Override
    public Iterable<CacheStats> getCacheStats() {
        return store.getCacheStats();
    }

    @Override
    public Map<String, String> getMetadata() {
        return store.getMetadata();
    }

    @Override
    public long determineServerTimeDifferenceMillis() throws UnsupportedOperationException, DocumentStoreException {
        return store.determineServerTimeDifferenceMillis();
    }

    @Override
    public void updateAccessedRevision(RevisionVector revision, int currentClusterId) {
        if (store instanceof RevisionListener) {
            ((RevisionListener) store).updateAccessedRevision(revision, currentClusterId);
        }
    }
}
