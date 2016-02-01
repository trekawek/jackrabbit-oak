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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

public class PutIfAbsentAction implements CacheAction {

    private static final Logger LOG = LoggerFactory.getLogger(PutIfAbsentAction.class);

    private final NodeDocument doc;

    public PutIfAbsentAction(NodeDocument doc) {
        this.doc = doc;
    }

    @Override
    public void execute(Cache<CacheValue, NodeDocument> target) {
        StringValue key = new StringValue(doc.getId());
        for (;;) {
            NodeDocument cached = null;
            try {
                cached = target.get(key, new Callable<NodeDocument>() {
                    @Override
                    public NodeDocument call() {
                        return doc;
                    }
                });
            } catch (ExecutionException e) {
                LOG.error("Can't put document", e);
            }
            if (cached == NodeDocument.NULL) {
                target.invalidate(key);
            }
        }
    }

    @Override
    public Iterable<String> affectedKeys() {
        return asList(doc.getId());
    }
}
