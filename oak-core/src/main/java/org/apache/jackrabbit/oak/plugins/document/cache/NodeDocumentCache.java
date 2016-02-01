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
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;

/**
 * Cache for the NodeDocuments.
 */
public interface NodeDocumentCache extends Closeable {

    /**
     * Invalidate document with given key.
     *
     * @param key to invalidate
     */
    void invalidate(@Nonnull String key);

    /**
     * Invalidate document with given keys iff their mod counts are different as
     * passed in the map.
     *
     * @param modCounts map where key is the document id and the value is the mod count
     * @return number of invalidated entries
     */
    @Nonnegative
    int invalidateOutdated(@Nonnull Map<String, Long> modCounts);

    /**
     * Return the cached value or null.
     *
     * @param key document key
     * @return cached value of null if there's no document with given key cached
     */
    @CheckForNull
    NodeDocument getIfPresent(@Nonnull String key);

    /**
     * Puts document into cache.
     *
     * @param doc document to put
     */
    void put(@Nonnull NodeDocument doc);

    /**
     * Puts an empty document into cache.
     *
     * @param key key of the null document
     */
    void putNull(@Nonnull String key);

    /**
     * Puts document into cache iff no entry with the given key is cached
     * already or the cached document is older (has smaller {@link Document#MOD_COUNT}).
     *
     * @param doc the document to add to the cache
     */
    void putIfNewer(@Nonnull final NodeDocument doc);

    /**
     * Puts document into cache iff no entry with the given key is cached
     * already. This operation is atomic.
     *
     * @param doc the document to add to the cache.
     */
    void putIfAbsent(@Nonnull final NodeDocument doc);

    /**
     * Replaces the cached value if the old document is currently present in
     * the cache. If the {@code oldDoc} is not cached, nothing will happen. If
     * {@code oldDoc} does not match the document currently in the cache, then
     * the cached document is invalidated.
     *
     * @param oldDoc the old document
     * @param newDoc the replacement
     */
    void replaceCachedDocument(@Nonnull final NodeDocument oldDoc,
                               @Nonnull final NodeDocument newDoc);

    /**
     * @return keys stored in cache
     */
    Iterable<CacheValue> keys();

    /**
     * @return values stored in cache
     */
    Iterable<NodeDocument> values();

    Iterable<CacheStats> getCacheStats();
}
