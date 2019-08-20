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
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DecimalPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.GenericPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiBooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiDecimalPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiDoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiGenericPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiLongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.remote.proto.NodeValueProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PropertyDeserializer {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyDeserializer.class);

    private final Function<String, Blob> blobProvider;

    public PropertyDeserializer(Function<String, Blob> blobProvider) {
        this.blobProvider = blobProvider;
    }

    public PropertyState toOakProperty(NodeValueProtos.Property property) {
        String name = property.getName();
        boolean isArray = property.getIsArray();
        PropertyState state;
        switch (property.getType()) {
            case STRING: {
                List<String> stringList = createStringValues(property);
                if (isArray) {
                    state = MultiStringPropertyState.stringProperty(name, stringList);
                } else {
                    state = StringPropertyState.stringProperty(name, stringList.get(0));
                }
            }
            break;

            case BINARY: {
                List<Blob> blobList = createBlobValues(property);
                if (isArray) {
                    state = MultiBinaryPropertyState.binaryPropertyFromBlob(name, blobList);
                } else {
                    state = BinaryPropertyState.binaryProperty(name, blobList.get(0));
                }
            }
            break;

            case LONG: {
                List<Long> longList = createLongValues(property);
                if (isArray) {
                    state = MultiLongPropertyState.createLongProperty(name, longList);
                } else {
                    state = LongPropertyState.createLongProperty(name, longList.get(0));
                }
            }
            break;

            case DOUBLE: {
                List<Double> doubleList = createDoubleValues(property);
                if (isArray) {
                    state = MultiDoublePropertyState.doubleProperty(name, doubleList);
                } else {
                    state = DoublePropertyState.doubleProperty(name, doubleList.get(0));
                }
            }
            break;

            case DATE: {
                List<String> stringList = createStringValues(property);
                if (isArray) {
                    state = MultiGenericPropertyState.dateProperty(name, stringList);
                } else {
                    state = GenericPropertyState.dateProperty(name, stringList.get(0));
                }
            }
            break;

            case BOOLEAN: {
                List<Boolean> boolList = createBoolValues(property);
                if (isArray) {
                    state = MultiBooleanPropertyState.booleanProperty(name, boolList);
                } else {
                    state = BooleanPropertyState.booleanProperty(name, boolList.get(0));
                }
            }
            break;

            case NAME: {
                List<String> stringList = createStringValues(property);
                if (isArray) {
                    state = MultiGenericPropertyState.nameProperty(name, stringList);
                } else {
                    state = GenericPropertyState.nameProperty(name, stringList.get(0));
                }
            }
            break;

            case PATH: {
                List<String> stringList = createStringValues(property);
                if (isArray) {
                    state = MultiGenericPropertyState.pathProperty(name, stringList);
                } else {
                    state = GenericPropertyState.pathProperty(name, stringList.get(0));
                }
            }
            break;

            case REFERENCE: {
                List<String> stringList = createStringValues(property);
                if (isArray) {
                    state = MultiGenericPropertyState.referenceProperty(name, stringList);
                } else {
                    state = GenericPropertyState.referenceProperty(name, stringList.get(0));
                }
            }
            break;

            case WEAKREFERENCE: {
                List<String> stringList = createStringValues(property);
                if (isArray) {
                    state = MultiGenericPropertyState.weakreferenceProperty(name, stringList);
                } else {
                    state = GenericPropertyState.weakreferenceProperty(name, stringList.get(0));
                }
            }
            break;

            case URI: {
                List<String> stringList = createStringValues(property);
                if (isArray) {
                    state = MultiGenericPropertyState.uriProperty(name, stringList);
                } else {
                    state = GenericPropertyState.uriProperty(name, stringList.get(0));
                }
            }
            break;

            case DECIMAL: {
                List<BigDecimal> decimalList = createDecimalValues(property);
                if (isArray) {
                    state = MultiDecimalPropertyState.decimalProperty(name, decimalList);
                } else {
                    state = DecimalPropertyState.decimalProperty(name, decimalList.get(0));
                }
            }
            break;

            default:
                throw new IllegalArgumentException("Invalid type " + property.getType().name());
        }
        return state;
    }

    private static List<String> createStringValues(NodeValueProtos.Property property) {
        return property.getValueList().stream()
                .map(NodeValueProtos.PropertyValue::getStringValue)
                .collect(Collectors.toList());
    }

    private static List<Long> createLongValues(NodeValueProtos.Property property) {
        return property.getValueList().stream()
                .map(NodeValueProtos.PropertyValue::getLongValue)
                .collect(Collectors.toList());
    }

    private static List<Double> createDoubleValues(NodeValueProtos.Property property) {
        return property.getValueList().stream()
                .map(NodeValueProtos.PropertyValue::getDoubleValue)
                .collect(Collectors.toList());
    }

    private static List<Boolean> createBoolValues(NodeValueProtos.Property property) {
        return property.getValueList().stream()
                .map(NodeValueProtos.PropertyValue::getBoolValue)
                .collect(Collectors.toList());
    }

    private List<Blob> createBlobValues(NodeValueProtos.Property property) {
        List<Blob> blobs = new ArrayList<>();
        for (NodeValueProtos.PropertyValue val : property.getValueList()) {
            switch (val.getValueCase()) {
                case STRINGVALUE:
                    blobs.add(blobProvider.apply(val.getStringValue()));
                    break;

                case BINARYVALUE:
                    blobs.add(new ArrayBasedBlob(val.getBinaryValue().toByteArray()));
                    break;

                default:
                    LOG.error("Can't read blob from property {}", property);
                    blobs.add(new ArrayBasedBlob(new byte[0]));
                    break;
            }
        }
        return blobs;
    }

    private static List<BigDecimal> createDecimalValues(NodeValueProtos.Property property) {
        return property.getValueList().stream()
                .map(NodeValueProtos.PropertyValue::getStringValue)
                .map(BigDecimal::new)
                .collect(Collectors.toList());
    }
}
