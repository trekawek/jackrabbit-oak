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

import static com.google.common.collect.Lists.newArrayList;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

public abstract class Value {

    private static class StringValue extends Value {

        private final String value;

        StringValue(Type type, boolean array, String value) {
            super(type, array);
            this.value = value;
        }

        @Override
        public String asStringValue() {
            return value;
        }

        @Override
        protected Object getValue() {
            return value;
        }

    }

    private static class StringArray extends Value {

        private final List<String> values;

        StringArray(Type type, boolean array, Iterable<String> values) {
            super(type, array);
            this.values = newArrayList(values);
        }

        @Override
        public Iterable<String> asStringArray() {
            return values;
        }

        @Override
        protected Object getValue() {
            return values;
        }

    }

    private static class LongValue extends Value {

        private final long value;

        LongValue(Type type, boolean array, long value) {
            super(type, array);
            this.value = value;
        }

        @Override
        public long asLongValue() {
            return value;
        }

        @Override
        protected Object getValue() {
            return value;
        }

    }

    private static class LongArray extends Value {

        private final List<Long> values;

        LongArray(Type type, boolean array, Iterable<Long> values) {
            super(type, array);
            this.values = newArrayList(values);
        }

        @Override
        public Iterable<Long> asLongArray() {
            return values;
        }

        @Override
        protected Object getValue() {
            return values;
        }

    }

    private static class DoubleValue extends Value {

        private final double value;

        DoubleValue(Type type, boolean array, double value) {
            super(type, array);
            this.value = value;
        }

        @Override
        public double asDoubleValue() {
            return value;
        }

        @Override
        protected Object getValue() {
            return value;
        }

    }

    private static class DoubleArray extends Value {

        private final List<Double> values;

        DoubleArray(Type type, boolean array, Iterable<Double> values) {
            super(type, array);
            this.values = newArrayList(values);
        }

        @Override
        public Iterable<Double> asDoubleArray() {
            return values;
        }

        @Override
        protected Object getValue() {
            return values;
        }

    }

    private static class BooleanValue extends Value {

        private final boolean value;

        BooleanValue(Type type, boolean array, boolean value) {
            super(type, array);
            this.value = value;
        }

        @Override
        public boolean asBooleanValue() {
            return value;
        }

        @Override
        protected Object getValue() {
            return value;
        }

    }

    private static class BooleanArray extends Value {

        private final List<Boolean> values;

        BooleanArray(Type type, boolean array, Iterable<Boolean> values) {
            super(type, array);
            this.values = newArrayList(values);
        }

        @Override
        public Iterable<Boolean> asBooleanArray() {
            return values;
        }

        @Override
        protected Object getValue() {
            return values;
        }

    }

    private static class DecimalValue extends Value {

        private final BigDecimal value;

        DecimalValue(Type type, boolean array, BigDecimal value) {
            super(type, array);
            this.value = value;
        }

        @Override
        public BigDecimal asDecimalValue() {
            return value;
        }

        @Override
        protected Object getValue() {
            return value;
        }

    }

    private static class DecimalArray extends Value {

        private final List<BigDecimal> values;

        DecimalArray(Type type, boolean array, Iterable<BigDecimal> values) {
            super(type, array);
            this.values = newArrayList(values);
        }

        @Override
        public Iterable<BigDecimal> asDecimalArray() {
            return values;
        }

        @Override
        protected Object getValue() {
            return values;
        }

    }

    public static Value newStringValue(String value) {
        return new StringValue(Type.STRING, false, value);
    }

    public static Value newBinaryValue(String value) {
        return new StringValue(Type.BINARY, false, value);
    }

    public static Value newLongValue(long value) {
        return new LongValue(Type.LONG, false, value);
    }

    public static Value newDoubleValue(double value) {
        return new DoubleValue(Type.DOUBLE, false, value);
    }

    public static Value newDateValue(String value) {
        return new StringValue(Type.DATE, false, value);
    }

    public static Value newBooleanValue(boolean value) {
        return new BooleanValue(Type.BOOLEAN, false, value);
    }

    public static Value newNameValue(String value) {
        return new StringValue(Type.NAME, false, value);
    }

    public static Value newPathValue(String value) {
        return new StringValue(Type.PATH, false, value);
    }

    public static Value newReferenceValue(String value) {
        return new StringValue(Type.REFERENCE, false, value);
    }

    public static Value newWeakReferenceValue(String value) {
        return new StringValue(Type.WEAK_REFERENCE, false, value);
    }

    public static Value newURIValue(String value) {
        return new StringValue(Type.URI, false, value);
    }

    public static Value newDecimalValue(BigDecimal value) {
        return new DecimalValue(Type.DECIMAL, false, value);
    }

    public static Value newStringArray(Iterable<String> values) {
        return new StringArray(Type.STRING, true, values);
    }

    public static Value newBinaryArray(Iterable<String> values) {
        return new StringArray(Type.BINARY, true, values);
    }

    public static Value newLongArray(Iterable<Long> values) {
        return new LongArray(Type.LONG, true, values);
    }

    public static Value newDoubleArray(Iterable<Double> values) {
        return new DoubleArray(Type.DOUBLE, true, values);
    }

    public static Value newDateArray(Iterable<String> values) {
        return new StringArray(Type.DATE, true, values);
    }

    public static Value newBooleanArray(Iterable<Boolean> values) {
        return new BooleanArray(Type.BOOLEAN, true, values);
    }

    public static Value newNameArray(Iterable<String> values) {
        return new StringArray(Type.NAME, true, values);
    }

    public static Value newPathArray(Iterable<String> values) {
        return new StringArray(Type.PATH, true, values);
    }

    public static Value newReferenceArray(Iterable<String> values) {
        return new StringArray(Type.REFERENCE, true, values);
    }

    public static Value newWeakReferenceArray(Iterable<String> values) {
        return new StringArray(Type.WEAK_REFERENCE, true, values);
    }

    public static Value newURIArray(Iterable<String> values) {
        return new StringArray(Type.URI, true, values);
    }

    public static Value newDecimalArray(Iterable<BigDecimal> values) {
        return new DecimalArray(Type.DECIMAL, true, values);
    }

    protected final Type type;

    protected final boolean array;

    private Value(Type type, boolean array) {
        this.type = type;
        this.array = array;
    }

    public Type getType() {
        return type;
    }

    public boolean isArray() {
        return array;
    }

    public String asStringValue() {
        throw new IllegalStateException("invalid value");
    }

    public long asLongValue() {
        throw new IllegalStateException("invalid value");
    }

    public double asDoubleValue() {
        throw new IllegalStateException("invalid value");
    }

    public boolean asBooleanValue() {
        throw new IllegalStateException("invalid value");
    }

    public BigDecimal asDecimalValue() {
        throw new IllegalStateException("invalid value");
    }

    public Iterable<String> asStringArray() {
        throw new IllegalStateException("invalid value");
    }

    public Iterable<Long> asLongArray() {
        throw new IllegalStateException("invalid value");
    }

    public Iterable<Double> asDoubleArray() {
        throw new IllegalStateException("invalid value");
    }

    public Iterable<Boolean> asBooleanArray() {
        throw new IllegalStateException("invalid value");
    }

    public Iterable<BigDecimal> asDecimalArray() {
        throw new IllegalStateException("invalid value");
    }

    protected abstract Object getValue();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        return equals((Value) o);
    }

    private boolean equals(Value o) {
        return array == o.array && type == o.type && Objects.equals(getValue(), o.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, array, getValue());
    }

    @Override
    public String toString() {
        return String.format("Value{type=%s, array=%s, value=%s}", type, array, getValue());
    }

}
