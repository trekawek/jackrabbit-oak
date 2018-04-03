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

package org.apache.jackrabbit.oak.kv.store;

import java.math.BigDecimal;
import java.util.Collections;

import com.google.common.collect.Lists;

public class Value {

    public static Value newStringValue(String value) {
        return new Value(Type.STRING, false, value);
    }

    public static Value newBinaryValue(String value) {
        return new Value(Type.BINARY, false, value);
    }

    public static Value newLongValue(long value) {
        return new Value(Type.LONG, false, value);
    }

    public static Value newDoubleValue(double value) {
        return new Value(Type.DOUBLE, false, value);
    }

    public static Value newDateValue(String value) {
        return new Value(Type.DATE, false, value);
    }

    public static Value newBooleanValue(boolean value) {
        return new Value(Type.BOOLEAN, false, value);
    }

    public static Value newNameValue(String value) {
        return new Value(Type.NAME, false, value);
    }

    public static Value newPathValue(String value) {
        return new Value(Type.PATH, false, value);
    }

    public static Value newReferenceValue(String value) {
        return new Value(Type.REFERENCE, false, value);
    }

    public static Value newWeakReferenceValue(String value) {
        return new Value(Type.WEAK_REFERENCE, false, value);
    }

    public static Value newURIValue(String value) {
        return new Value(Type.URI, false, value);
    }

    public static Value newDecimalValue(BigDecimal value) {
        return new Value(Type.DECIMAL, false, value);
    }

    private static Object values(Iterable<?> values) {
        return Collections.unmodifiableList(Lists.newArrayList(values));
    }

    public static Value newStringArray(Iterable<String> values) {
        return new Value(Type.STRING, true, values(values));
    }

    public static Value newBinaryArray(Iterable<String> values) {
        return new Value(Type.BINARY, true, values(values));
    }

    public static Value newLongArray(Iterable<Long> values) {
        return new Value(Type.LONG, true, values(values));
    }

    public static Value newDoubleArray(Iterable<Double> values) {
        return new Value(Type.DOUBLE, true, values(values));
    }

    public static Value newDateArray(Iterable<String> values) {
        return new Value(Type.DATE, true, values(values));
    }

    public static Value newBooleanArray(Iterable<Boolean> values) {
        return new Value(Type.BOOLEAN, true, values(values));
    }

    public static Value newNameArray(Iterable<String> values) {
        return new Value(Type.NAME, true, values(values));
    }

    public static Value newPathArray(Iterable<String> values) {
        return new Value(Type.PATH, true, values(values));
    }

    public static Value newReferenceArray(Iterable<String> values) {
        return new Value(Type.REFERENCE, true, values(values));
    }

    public static Value newWeakReferenceArray(Iterable<String> values) {
        return new Value(Type.WEAK_REFERENCE, true, values(values));
    }

    public static Value newURIArray(Iterable<String> values) {
        return new Value(Type.URI, true, values(values));
    }

    public static Value newDecimalArray(Iterable<BigDecimal> values) {
        return new Value(Type.DECIMAL, true, values(values));
    }

    private final Type type;

    private final boolean array;

    private final Object value;

    private Value(Type type, boolean array, Object value) {
        this.type = type;
        this.array = array;
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public boolean isArray() {
        return array;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("Value{type=%s, array=%s, value=%s}", type, array, value);
    }

}
