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

package org.apache.jackrabbit.oak.segment;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.cache.RemovalCause;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheLIRS.EvictionCallback;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FIXME OAK-4373 document, add monitoring, management, tests, logging
 */
public class SegmentCache {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentCache.class);

    /**
     * Cache of recently accessed segments
     */
    @Nonnull
    private final CacheLIRS<SegmentId, Segment> cache;

    public SegmentCache(long cacheSizeMB) {
        this.cache = CacheLIRS.<SegmentId, Segment>newBuilder()
            .module("SegmentCache")
            .maximumWeight(cacheSizeMB * 1024 * 1024)
            .averageWeight(Segment.MAX_SEGMENT_SIZE/2)
            .evictionCallback(new EvictionCallback<SegmentId, Segment>() {
                @Override
                public void evicted(SegmentId id, Segment segment, RemovalCause cause) {
                    if (segment != null) {
                        id.unloaded();
                    }
                } })
            .build();
    }

    /**
     * Get a segment from the cache
     * @param id  segment id
     * @return  segment with the given {@code id} or {@code null} if not in the cache
     */
    @CheckForNull
    public Segment geSegment(@Nonnull SegmentId id) {
        try {
            return cache.get(id);
        } catch (ExecutionException e) {
            LOG.error("Error loading segment {} from cache", id, e);
            return null;
        }
    }

    @Nonnull
    public Segment geSegment(@Nonnull SegmentId id, @Nonnull Callable<Segment> loader)
    throws ExecutionException {
        return cache.get(id, loader);
    }

    public void putSegment(@Nonnull Segment segment) {
        cache.put(segment.getSegmentId(), segment, segment.size());
        segment.getSegmentId().loaded(segment);
    }

    public void clear() {
        cache.invalidateAll();
    }

    @Nonnull
    public CacheStats getCacheStats() {
        return new CacheStats(cache, "Segment Cache", null, -1);
    }
}