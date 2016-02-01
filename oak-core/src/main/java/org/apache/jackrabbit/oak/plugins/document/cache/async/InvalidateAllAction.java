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

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.collect.Iterables;

public class InvalidateAllAction implements CacheAction {

    private final Iterable<String> keys;

    public InvalidateAllAction(Iterable<String> keys) {
        this.keys = keys;
    }

    @Override
    public void execute(Cache<CacheValue, NodeDocument> target) {
        target.invalidateAll(Iterables.transform(keys, new Function<String, StringValue>() {
            @Override
            public StringValue apply(String input) {
                return new StringValue(input);
            }
        }));
    }

    @Override
    public Iterable<String> affectedKeys() {
        return keys;
    }

    @Override
    public String toString() {
        return new StringBuilder("InvalidateAllAction[").append(keys).append("]").toString();
    }
}
