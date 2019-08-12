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

    public static NodeValueProtos.Property toProtoProperty(PropertyState propertyState) throws RemoteNodeStoreException {
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

    private static NodeValueProtos.PropertyValue toProtoPropertyValue(Type<?> type, Object value) throws RemoteNodeStoreException {
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
            throw new RemoteNodeStoreException("The value " + value + " has invalid type " + type.toString());
        }
        return builder.build();
    }
}
