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
package org.apache.jackrabbit.oak.segment.dynamodb;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import static org.apache.jackrabbit.oak.segment.dynamodb.Configuration.PID;

@ObjectClassDefinition(
        pid = {PID},
        name = "Apache Jackrabbit Oak DynamoDB Segment Store Service",
        description = "DynamoDB backend for the Oak Segment Node Store")
@interface Configuration {

    String PID = "org.apache.jackrabbit.oak.segment.dynamodb.DynamoSegmentStoreService";

    @AttributeDefinition(
            name = "DynamoDB Region",
            description = "Region name to be used")
    String region() default "us-west-1";

    @AttributeDefinition(
            name = "DynamoDB access key",
            description = "Access key which should be used to authenticate on the account")
    String accessKey();

    @AttributeDefinition(
            name = "DynamoDB secret key",
            description = "Secret key which should be used to authenticate on the account")
    String secretKey();

    @AttributeDefinition(
            name = "Prefix",
            description = "Names of all the created tables will be prefixed with this string")
    String prefix() default "";

    @AttributeDefinition(
            name = "Endpoint",
            description = "An optional parameter describing the DynamoDB endpoint")
    String endpoint();
}