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

import static org.apache.jackrabbit.oak.kv.store.Type.values;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Type;
import org.apache.jackrabbit.oak.kv.store.Value;

class Converters {

    private Converters() {
        // Prevent instantiation.
    }

    static byte[] write(Map<String, Value> properties, Map<String, ID> children) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (Output output = new Output(stream)) {
            output.writeVarInt(properties.size(), true);
            for (Entry<String, Value> e : properties.entrySet()) {
                output.writeString(e.getKey());
                write(output, e.getValue());
            }
            output.writeVarInt(children.size(), true);
            for (Entry<String, ID> e : children.entrySet()) {
                output.writeString(e.getKey());
                write(output, e.getValue());
            }
        }
        return stream.toByteArray();
    }

    static byte[] write(ID id) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (Output output = new Output(stream)) {
            write(output, id);
        }
        return stream.toByteArray();
    }

    private static void write(Output output, ID id) {
        if (id instanceof LevelDBID) {
            write(output, (LevelDBID) id);
        } else {
            throw new IllegalArgumentException("id");
        }
    }

    private static void write(Output output, LevelDBID id) {
        output.writeLong(id.getID().getMostSignificantBits());
        output.writeLong(id.getID().getLeastSignificantBits());
    }

    private static void write(Output output, Value value) {
        output.writeVarInt(value.getType().ordinal(), true);
        output.writeBoolean(value.isArray());
        if (value.isArray()) {
            writeArray(output, value);
        } else {
            writeValue(output, value);
        }
    }

    private static void writeValue(Output output, Value value) {
        switch (value.getType()) {
            case STRING:
            case BINARY:
            case DATE:
            case NAME:
            case PATH:
            case REFERENCE:
            case WEAK_REFERENCE:
            case URI:
                output.writeString(value.asStringValue());
                break;
            case LONG:
                output.writeLong(value.asLongValue());
                break;
            case DOUBLE:
                output.writeDouble(value.asDoubleValue());
                break;
            case BOOLEAN:
                output.writeBoolean(value.asBooleanValue());
                break;
            case DECIMAL:
                output.writeString(value.asDecimalValue().toString());
                break;
            default:
                throw new IllegalArgumentException("value");
        }
    }

    private static void writeArray(Output output, Value value) {
        switch (value.getType()) {
            case STRING:
            case BINARY:
            case DATE:
            case NAME:
            case PATH:
            case REFERENCE:
            case WEAK_REFERENCE:
            case URI:
                writeStringArray(output, value.asStringArray());
                break;
            case LONG:
                writeLongArray(output, value.asLongArray());
                break;
            case DOUBLE:
                writeDoubleArray(output, value.asDoubleArray());
                break;
            case BOOLEAN:
                writeBooleanArray(output, value.asBooleanArray());
                break;
            case DECIMAL:
                writeDecimalArray(output, value.asDecimalArray());
                break;
            default:
                throw new IllegalArgumentException("value");
        }
    }

    private static void writeStringArray(Output output, Iterable<String> i) {
        List<String> values = Lists.newArrayList(i);
        output.writeVarInt(values.size(), true);
        for (String value : values) {
            output.writeString(value);
        }
    }

    private static void writeLongArray(Output output, Iterable<Long> i) {
        List<Long> values = Lists.newArrayList(i);
        output.writeVarInt(values.size(), true);
        for (Long value : values) {
            output.writeLong(value);
        }
    }

    private static void writeDoubleArray(Output output, Iterable<Double> i) {
        List<Double> values = Lists.newArrayList(i);
        output.writeVarInt(values.size(), true);
        for (Double value : values) {
            output.writeDouble(value);
        }
    }

    private static void writeBooleanArray(Output output, Iterable<Boolean> i) {
        List<Boolean> values = Lists.newArrayList(i);
        output.writeVarInt(values.size(), true);
        for (Boolean value : values) {
            output.writeBoolean(value);
        }
    }

    private static void writeDecimalArray(Output output, Iterable<BigDecimal> i) {
        List<BigDecimal> values = Lists.newArrayList(i);
        output.writeVarInt(values.size(), true);
        for (BigDecimal value : values) {
            output.writeString(value.toString());
        }
    }

    static Node readNode(byte[] bytes) {
        try (Input input = new Input(bytes)) {
            int propertiesSize = input.readVarInt(true);
            Map<String, Value> properties = new HashMap<>(propertiesSize);
            for (int i = 0; i < propertiesSize; i++) {
                String key = input.readString();
                Value value = readValue(input);
                properties.put(key, value);
            }
            int childrenCount = input.readVarInt(true);
            Map<String, ID> children = new HashMap<>(childrenCount);
            for (int i = 0; i < childrenCount; i++) {
                String key = input.readString();
                ID value = readID(input);
                children.put(key, value);
            }
            return new LevelDBNode(properties, children);
        }
    }

    static ID readID(byte[] bytes) {
        try (Input input = new Input(bytes)) {
            return readID(input);
        }
    }

    private static Value readValue(Input input) {
        Type type = values()[input.readVarInt(true)];
        boolean isArray = input.readBoolean();
        if (isArray) {
            return readArray(input, type);
        }
        return readValue(input, type);
    }

    private static Value readValue(Input input, Type type) {
        switch (type) {
            case STRING:
                return Value.newStringValue(input.readString());
            case BINARY:
                return Value.newBinaryValue(input.readString());
            case LONG:
                return Value.newLongValue(input.readLong());
            case DOUBLE:
                return Value.newDoubleValue(input.readDouble());
            case DATE:
                return Value.newDateValue(input.readString());
            case BOOLEAN:
                return Value.newBooleanValue(input.readBoolean());
            case NAME:
                return Value.newNameValue(input.readString());
            case PATH:
                return Value.newPathValue(input.readString());
            case REFERENCE:
                return Value.newReferenceValue(input.readString());
            case WEAK_REFERENCE:
                return Value.newWeakReferenceValue(input.readString());
            case URI:
                return Value.newURIValue(input.readString());
            case DECIMAL:
                return Value.newDecimalValue(new BigDecimal(input.readString()));
            default:
                throw new IllegalArgumentException("type");
        }
    }

    private static Value readArray(Input input, Type type) {
        switch (type) {
            case STRING:
                return Value.newStringArray(readStringArray(input));
            case BINARY:
                return Value.newBinaryArray(readStringArray(input));
            case DATE:
                return Value.newDateArray(readStringArray(input));
            case NAME:
                return Value.newNameArray(readStringArray(input));
            case PATH:
                return Value.newPathArray(readStringArray(input));
            case REFERENCE:
                return Value.newReferenceArray(readStringArray(input));
            case WEAK_REFERENCE:
                return Value.newWeakReferenceArray(readStringArray(input));
            case URI:
                return Value.newURIArray(readStringArray(input));
            case DECIMAL:
                return Value.newDecimalArray(readDecimalArray(input));
            case LONG:
                return Value.newLongArray(readLongArray(input));
            case DOUBLE:
                return Value.newDoubleArray(readDoubleArray(input));
            case BOOLEAN:
                return Value.newBooleanArray(readBooleanArray(input));
            default:
                throw new IllegalArgumentException("type");
        }
    }

    private static Iterable<String> readStringArray(Input input) {
        int size = input.readVarInt(true);
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(input.readString());
        }
        return values;
    }

    private static Iterable<Long> readLongArray(Input input) {
        int size = input.readVarInt(true);
        List<Long> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(input.readLong());
        }
        return values;
    }

    private static Iterable<Double> readDoubleArray(Input input) {
        int size = input.readVarInt(true);
        List<Double> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(input.readDouble());
        }
        return values;
    }

    private static Iterable<Boolean> readBooleanArray(Input input) {
        int size = input.readVarInt(true);
        List<Boolean> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(input.readBoolean());
        }
        return values;
    }

    private static Iterable<BigDecimal> readDecimalArray(Input input) {
        int size = input.readVarInt(true);
        List<BigDecimal> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(new BigDecimal(input.readString()));
        }
        return values;
    }

    static ID readID(Input input) {
        long msb = input.readLong();
        long lsb = input.readLong();
        return new LevelDBID(new UUID(msb, lsb));
    }

}
