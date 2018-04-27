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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Type;
import org.apache.jackrabbit.oak.kv.store.Value;

class Converters {

    private Converters() {
        // Prevent instantiation.
    }

    static final JsonDeserializer<ID> jsonToID = (element, type, context) ->
        new LevelDBID(context.deserialize(element, String.class));

    static final JsonSerializer<LevelDBID> idToJson = (id, type, context) ->
        context.serialize(id.getID());

    static final JsonSerializer<Value> valueToJson = (value, type, context) -> {
        JsonObject object = new JsonObject();
        object.add("type", context.serialize(value.getType()));
        object.add("isArray", context.serialize(value.isArray()));
        object.add("value", toJson(value, context));
        return object;
    };

    static final JsonDeserializer<Value> jsonToValue = (element, type, context) -> {
        if (element.isJsonObject()) {
            return toValue((JsonObject) element, context);
        }
        throw new JsonParseException("invalid element");
    };

    private static Value toValue(JsonObject object, JsonDeserializationContext context) {
        Type valueType = context.deserialize(object.get("type"), Type.class);
        boolean isArray = context.deserialize(object.get("isArray"), Boolean.class);
        return toValue(valueType, isArray, object.get("value"), context);
    }

    private static Value toValue(Type type, boolean isArray, JsonElement element, JsonDeserializationContext context) {
        if (isArray) {
            return toArray(type, element, context);
        }
        return toValue(type, element, context);
    }

    private static Value toValue(Type type, JsonElement element, JsonDeserializationContext context) {
        switch (type) {
            case STRING:
                return newStringValue(context.deserialize(element, String.class));
            case BINARY:
                return newBinaryValue(context.deserialize(element, String.class));
            case LONG:
                return newLongValue(context.deserialize(element, Long.TYPE));
            case DOUBLE:
                return newDoubleValue(context.deserialize(element, Double.TYPE));
            case DATE:
                return newDateValue(context.deserialize(element, String.class));
            case BOOLEAN:
                return newBooleanValue(context.deserialize(element, Boolean.TYPE));
            case NAME:
                return newNameValue(context.deserialize(element, String.class));
            case PATH:
                return newPathValue(context.deserialize(element, String.class));
            case REFERENCE:
                return newReferenceValue(context.deserialize(element, String.class));
            case WEAK_REFERENCE:
                return newWeakReferenceValue(context.deserialize(element, String.class));
            case URI:
                return newURIValue(context.deserialize(element, String.class));
            case DECIMAL:
                return newDecimalValue(new BigDecimal((String) context.deserialize(element, String.class)));
            default:
                throw new JsonParseException("value");
        }
    }

    private static Value toArray(Type type, JsonElement element, JsonDeserializationContext context) {
        switch (type) {
            case STRING:
                return newStringArray(context.deserialize(element, List.class));
            case BINARY:
                return newBinaryArray(context.deserialize(element, List.class));
            case LONG:
                return newLongArray(toLongArray(context.deserialize(element, List.class)));
            case DOUBLE:
                return newDoubleArray(context.deserialize(element, List.class));
            case DATE:
                return newDateArray(context.deserialize(element, List.class));
            case BOOLEAN:
                return newBooleanArray(context.deserialize(element, List.class));
            case NAME:
                return newNameArray(context.deserialize(element, List.class));
            case PATH:
                return newPathArray(context.deserialize(element, List.class));
            case REFERENCE:
                return newReferenceArray(context.deserialize(element, List.class));
            case WEAK_REFERENCE:
                return newWeakReferenceArray(context.deserialize(element, List.class));
            case URI:
                return newURIArray(context.deserialize(element, List.class));
            case DECIMAL:
                return newDecimalArray(toDecimalArray(context.deserialize(element, List.class)));
            default:
                throw new JsonParseException("value");
        }
    }

    private static JsonElement toJson(Value value, JsonSerializationContext context) {
        if (value.isArray()) {
            return toJsonArray(value, context);
        }
        return toJsonValue(value, context);
    }

    private static JsonElement toJsonArray(Value value, JsonSerializationContext context) {
        switch (value.getType()) {
            case STRING:
            case BINARY:
            case DATE:
            case NAME:
            case PATH:
            case REFERENCE:
            case WEAK_REFERENCE:
            case URI:
                return context.serialize(value.asStringArray());
            case LONG:
                return context.serialize(value.asLongArray());
            case DOUBLE:
                return context.serialize(value.asDoubleArray());
            case BOOLEAN:
                return context.serialize(value.asBooleanArray());
            case DECIMAL:
                return context.serialize(toStringArray(value.asDecimalArray()));
            default:
                throw new IllegalStateException("value");
        }
    }

    private static JsonElement toJsonValue(Value value, JsonSerializationContext context) {
        switch (value.getType()) {
            case STRING:
            case BINARY:
            case DATE:
            case NAME:
            case PATH:
            case REFERENCE:
            case WEAK_REFERENCE:
            case URI:
                return context.serialize(value.asStringValue());
            case LONG:
                return context.serialize(value.asLongValue());
            case DOUBLE:
                return context.serialize(value.asDoubleValue());
            case BOOLEAN:
                return context.serialize(value.asBooleanValue());
            case DECIMAL:
                return context.serialize(value.asDecimalValue().toString());
            default:
                throw new IllegalStateException("value");
        }
    }

    private static Iterable<String> toStringArray(Iterable<BigDecimal> values) {
        List<String> strings = new ArrayList<>();

        for (BigDecimal value : values) {
            strings.add(value.toString());
        }

        return strings;
    }

    private static Iterable<Long> toLongArray(Iterable<Double> doubles) {
        List<Long> longs = new ArrayList<>();

        for (double value : doubles) {
            longs.add((long) value);
        }

        return longs;
    }

    private static Iterable<BigDecimal> toDecimalArray(Iterable<String> strings) {
        List<BigDecimal> decimals = new ArrayList<>();

        for (String value : strings) {
            decimals.add(new BigDecimal(value));
        }

        return decimals;
    }

}
