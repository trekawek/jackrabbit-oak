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

import static java.lang.String.format;
import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Store;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

public class LevelDBStore implements Store, Closeable {

    private final Gson gson = new GsonBuilder()
        .registerTypeAdapter(Value.class, Converters.jsonToValue)
        .registerTypeAdapter(Value.class, Converters.valueToJson)
        .registerTypeAdapter(ID.class, Converters.jsonToID)
        .registerTypeAdapter(LevelDBID.class, Converters.idToJson)
        .create();

    private final DB db;

    public LevelDBStore(File path) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        db = JniDBFactory.factory.open(path, options);
    }

    private String getString(String key) {
        return asString(db.get(bytes(key)));
    }

    private void putString(String key, String value) {
        db.put(bytes(key), bytes(value));
    }

    private void deleteString(String key) {
        db.delete(bytes(key));
    }

    private static String tagKey(String tag) {
        return format("t-%s", tag);
    }

    @Override
    public ID getTag(String tag) throws IOException {
        String value = getString(tagKey(tag));
        if (value == null) {
            return null;
        }
        return new LevelDBID(value);
    }

    @Override
    public void putTag(String tag, ID id) throws IOException {
        if (id instanceof LevelDBID) {
            putTag(tag, (LevelDBID) id);
        } else {
            throw new IllegalArgumentException("id");
        }
    }

    private void putTag(String tag, LevelDBID id) {
        putString(tagKey(tag), id.getID());
    }

    @Override
    public void deleteTag(String tag) throws IOException {
        deleteString(tagKey(tag));
    }

    @Override
    public Node getNode(ID id) throws IOException {
        if (id instanceof LevelDBID) {
            return getNode((LevelDBID) id);
        }
        throw new IllegalArgumentException("id");
    }

    private Node getNode(LevelDBID id) {
        return gson.fromJson(getString(id.getID()), LevelDBNode.class);
    }

    @Override
    public ID putNode(Map<String, Value> properties, Map<String, ID> children) throws IOException {
        LevelDBID id = new LevelDBID(UUID.randomUUID().toString());
        putString(id.getID(), gson.toJson(new LevelDBNode(properties, children)));
        return id;
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

}
