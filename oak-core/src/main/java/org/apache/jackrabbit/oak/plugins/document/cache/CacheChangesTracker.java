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

import com.google.common.base.Predicate;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.util.List;

public class CacheChangesTracker {

    private final List<CacheChangesTracker> changeTrackers;

    private final Predicate<String> keyFilter;

    private final LazyBloomFilter lazyBloomFilter;

    CacheChangesTracker(Predicate<String> keyFilter, List<CacheChangesTracker> changeTrackers) {
        this.changeTrackers = changeTrackers;
        this.keyFilter = keyFilter;
        this.lazyBloomFilter = new LazyBloomFilter();
        changeTrackers.add(this);
    }

    public void putDocument(String key) {
        if (keyFilter.apply(key)) {
            lazyBloomFilter.put(key);
        }
    }

    public void invalidateDocument(String key) {
        if (keyFilter.apply(key)) {
            lazyBloomFilter.put(key);
        }
    }

    public boolean mightBeenAffected(String key) {
        return keyFilter.apply(key) && lazyBloomFilter.mightContain(key);
    }

    public void close() {
        changeTrackers.remove(this);
    }

    public static class LazyBloomFilter {

        private static final double FPP = 0.01d;

        private static final int ENTRIES = 1000;

        private volatile BloomFilter<String> filter;

        public void put(String entry) {
            getFilter().put(entry);
        }

        public boolean mightContain(String entry) {
            if (filter == null) {
                return false;
            } else {
                return filter.mightContain(entry);
            }
        }

        private BloomFilter<String> getFilter() {
            if (filter == null) {
                synchronized (this) {
                    if (filter == null) {
                        filter = BloomFilter.create(new Funnel<String>() {
                            private static final long serialVersionUID = -7114267990225941161L;

                            @Override
                            public void funnel(String from, PrimitiveSink into) {
                                into.putUnencodedChars(from);
                            }
                        }, ENTRIES, FPP);
                    }
                }
            }
            return filter;
        }

    }
}
