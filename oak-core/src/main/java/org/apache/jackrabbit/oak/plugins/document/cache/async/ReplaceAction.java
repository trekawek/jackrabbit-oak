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
package org.apache.jackrabbit.oak.plugins.document.cache.async;

import static java.util.Arrays.asList;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;

public class ReplaceAction implements CacheAction {

    private final NodeDocument oldDoc;

    private final NodeDocument newDoc;

    public ReplaceAction(NodeDocument oldDoc, NodeDocument newDoc) {
        this.oldDoc = oldDoc;
        this.newDoc = newDoc;
    }

    @Override
    public void execute(Cache<CacheValue, NodeDocument> target) {
        StringValue key = new StringValue(oldDoc.getId());
        NodeDocument cached = target.getIfPresent(key);
        if (cached != null) {
            if (Objects.equal(cached.getModCount(), oldDoc.getModCount())) {
                target.put(key, newDoc);
            } else {
                // the cache entry was modified by some other thread in
                // the meantime. the updated cache entry may or may not
                // include this update. we cannot just apply our update
                // on top of the cached entry.
                // therefore we must invalidate the cache entry
                target.invalidate(key);
            }
        }
    }

    @Override
    public Iterable<String> affectedKeys() {
        return asList(oldDoc.getId());
    }
}
