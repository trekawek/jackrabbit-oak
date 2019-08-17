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
package org.apache.jackrabbit.oak.remote.common;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos;
import org.apache.jackrabbit.oak.remote.server.RemoteNodeStoreException;

import java.math.BigDecimal;

public final class PropertySerializer {

    private PropertySerializer() {
    }

    public static NodeValueProtos.Property toProtoProperty(PropertyState propertyState) {
        NodeValueProtos.Property.Builder propertyBuilder = NodeValueProtos.Property.newBuilder();
        propertyBuilder.setName(propertyState.getName());
        propertyBuilder.setIsArray(propertyState.isArray());
        if (propertyState.isArray()) {
            Type<?> type = propertyState.getType().getBaseType();
            propertyBuilder.setType(toProtoPropertyType(type));
            for (int i = 0; i < propertyState.count(); i++) {
                propertyBuilder.addValue(toProtoPropertyValue(type, propertyState.getValue(type, i)));
            }
        } else {
            Type<?> type = propertyState.getType();
            propertyBuilder.setType(toProtoPropertyType(type));
            propertyBuilder.addValue(toProtoPropertyValue(type, propertyState.getValue(type)));
        }
        return propertyBuilder.build();
    }

    private static NodeValueProtos.PropertyType toProtoPropertyType(Type<?> type) {
        return NodeValueProtos.PropertyType.valueOf(type.toString());
    }

    private static NodeValueProtos.PropertyValue toProtoPropertyValue(Type<?> type, Object value) {
        NodeValueProtos.PropertyValue.Builder builder = NodeValueProtos.PropertyValue.newBuilder();
        if (value instanceof Blob) {
            builder.setStringValue(((Blob) value).getContentIdentity());
        } else if (value instanceof String) {
            builder.setStringValue((String) value);
        } else if (value instanceof Double) {
            builder.setDoubleValue((Double) value);
        } else if (value instanceof Long) {
            builder.setLongValue((Long) value);
        } else if (value instanceof Boolean) {
            builder.setBoolValue((Boolean) value);
        } else if (value instanceof BigDecimal) {
            builder.setStringValue(value.toString());
        } else {
            throw new IllegalArgumentException("The value " + value + " has invalid type " + type.toString());
        }
        return builder.build();
    }
}
