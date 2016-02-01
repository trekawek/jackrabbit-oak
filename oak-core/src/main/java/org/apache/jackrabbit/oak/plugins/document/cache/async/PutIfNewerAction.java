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
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

public class PutIfNewerAction implements CacheAction {

    private static final Logger LOG = LoggerFactory.getLogger(PutIfNewerAction.class);

    private final NodeDocument doc;

    public PutIfNewerAction(NodeDocument doc) {
        this.doc = doc;
    }

    @Override
    public void execute(Cache<CacheValue, NodeDocument> target) {
        StringValue key = new StringValue(doc.getId());
        NodeDocument cachedDoc = target.getIfPresent(key);
        if (cachedDoc == null || cachedDoc == NodeDocument.NULL) {
            target.put(key, doc);
        } else {
            Long cachedModCount = cachedDoc.getModCount();
            Long modCount = doc.getModCount();

            if (cachedModCount == null || modCount == null) {
                LOG.error("Missing " + Document.MOD_COUNT + " on " + doc.getId());
            }

            if (modCount > cachedModCount) {
                target.put(key, doc);
            }
        }
    }

    @Override
    public Iterable<String> affectedKeys() {
        return asList(doc.getId());
    }

    @Override
    public String toString() {
        return new StringBuilder("PutIfNewerAction[").append(doc).append("]").toString();
    }
}
