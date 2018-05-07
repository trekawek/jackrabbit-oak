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
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Store;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

public class LevelDBStore implements Store, Closeable {

    private final DB db;

    public LevelDBStore(File path) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        db = JniDBFactory.factory.open(path, options);
    }

    private static byte[] tagKey(String tag) {
        return bytes(format("t-%s", tag));
    }

    @Override
    public ID getTag(String tag) {
        byte[] value = db.get(tagKey(tag));
        if (value == null) {
            return null;
        }
        return Converters.readID(value);
    }

    @Override
    public void putTag(String tag, ID id) {
        if (id instanceof LevelDBID) {
            db.put(tagKey(tag), Converters.write(id));
        } else {
            throw new IllegalArgumentException("id");
        }
    }

    @Override
    public void deleteTag(String tag) {
        db.delete(tagKey(tag));
    }

    @Override
    public Node getNode(ID id) {
        if (id instanceof LevelDBID) {
            return getNode((LevelDBID) id);
        }
        throw new IllegalArgumentException("id");
    }

    private Node getNode(LevelDBID id) {
        byte[] value = db.get(Converters.write(id));
        if (value == null) {
            return null;
        }
        return Converters.readNode(value);
    }

    @Override
    public ID putNode(Map<String, Value> properties, Map<String, ID> children) {
        LevelDBID id = new LevelDBID(UUID.randomUUID());
        db.put(Converters.write(id), Converters.write(properties, children));
        return id;
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

}
