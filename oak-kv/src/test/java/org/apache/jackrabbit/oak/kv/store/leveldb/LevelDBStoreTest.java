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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.kv.store.Value.newBinaryArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newBinaryValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newBooleanArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newBooleanValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newDateArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newDateValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newDecimalArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newDecimalValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newDoubleArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newDoubleValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newLongArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newLongValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newNameArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newNameValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newPathArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newPathValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newReferenceArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newReferenceValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newStringArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newStringValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newURIArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newURIValue;
import static org.apache.jackrabbit.oak.kv.store.Value.newWeakReferenceArray;
import static org.apache.jackrabbit.oak.kv.store.Value.newWeakReferenceValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LevelDBStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private LevelDBStore store;

    @Before
    public void setUp() throws Exception {
        store = new LevelDBStore(folder.getRoot());
    }

    @After
    public void tearDown() throws Exception {
        store.close();
    }

    @Test
    public void testProperties() throws Exception {
        Map<String, Value> properties = new HashMap<>();

        properties.put("string", newStringValue("string"));
        properties.put("binary", newBinaryValue("binary"));
        properties.put("long", newLongValue(1));
        properties.put("double", newDoubleValue(2));
        properties.put("date", newDateValue("date"));
        properties.put("boolean", newBooleanValue(true));
        properties.put("name", newNameValue("name"));
        properties.put("path", newPathValue("path"));
        properties.put("reference", newReferenceValue("reference"));
        properties.put("weakReference", newWeakReferenceValue("weakReference"));
        properties.put("uri", newURIValue("uri"));
        properties.put("decimal", newDecimalValue(BigDecimal.ONE));

        properties.put("strings", newStringArray(asList("a", "b")));
        properties.put("binaries", newBinaryArray(asList("c", "d")));
        properties.put("longs", newLongArray(asList(1L, 2L)));
        properties.put("doubles", newDoubleArray(asList(3.0, 4.0)));
        properties.put("dates", newDateArray(asList("e", "f")));
        properties.put("booleans", newBooleanArray(asList(true, true)));
        properties.put("names", newNameArray(asList("g", "h")));
        properties.put("pats", newPathArray(asList("i", "j")));
        properties.put("references", newReferenceArray(asList("k", "l")));
        properties.put("weakReferences", newWeakReferenceArray(asList("m", "n")));
        properties.put("uris", newURIArray(asList("o", "p")));
        properties.put("decimals", newDecimalArray(asList(BigDecimal.ONE, BigDecimal.TEN)));

        Node node = store.getNode(store.putNode(properties, emptyMap()));
        assertEquals(properties, node.getProperties());
    }

    @Test
    public void testChildren() throws Exception {
        Map<String, ID> children = new HashMap<>();

        children.put("a", store.putNode(emptyMap(), emptyMap()));
        children.put("b", store.putNode(emptyMap(), emptyMap()));

        Node node = store.getNode(store.putNode(emptyMap(), children));
        assertEquals(children, node.getChildren());
    }

    @Test
    public void testPutGetNode() throws Exception {
        ID id = store.putNode(emptyMap(), emptyMap());
        assertNotNull(store.getNode(id));
    }

    @Test
    public void testPutDeleteGetNode() throws Exception {
        ID id = store.putNode(emptyMap(), emptyMap());
        store.deleteNode(id);
        assertNull(store.getNode(id));
    }

    @Test
    public void testGetTag() throws Exception {
        ID id = store.putNode(emptyMap(), emptyMap());
        store.putTag("tag", id);
        assertEquals(id, store.getTag("tag"));
    }

    @Test
    public void testPutTag() throws Exception {
        ID a = store.putNode(emptyMap(), emptyMap());
        store.putTag("tag", a);
        ID b = store.putNode(emptyMap(), emptyMap());
        store.putTag("tag", b);
        assertEquals(b, store.getTag("tag"));
    }

    @Test
    public void testDeleteTag() throws Exception {
        ID id = store.putNode(emptyMap(), emptyMap());
        store.putTag("tag", id);
        store.deleteTag("tag");
        assertNull(store.getTag("tag"));
    }

}
