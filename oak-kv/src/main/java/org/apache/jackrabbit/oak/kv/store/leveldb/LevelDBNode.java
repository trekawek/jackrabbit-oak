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

package org.apache.jackrabbit.oak.kv.store.leveldb;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Value;

class LevelDBNode implements Node {

    private final Map<String, Value> properties;

    private final Map<String, ID> children;

    LevelDBNode(Map<String, Value> properties, Map<String, ID> children) {
        this.properties = unmodifiableMap(new HashMap<>(properties));
        this.children = unmodifiableMap(new HashMap<>(children));
    }

    @Override
    public Map<String, Value> getProperties() {
        return properties;
    }

    @Override
    public Map<String, ID> getChildren() {
        return children;
    }

}
