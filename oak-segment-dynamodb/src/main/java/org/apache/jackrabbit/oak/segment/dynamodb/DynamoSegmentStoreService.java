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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        configurationPid = {Configuration.PID})
public class DynamoSegmentStoreService {

    private static final Logger log = LoggerFactory.getLogger(DynamoSegmentStoreService.class);

    private ServiceRegistration registration;

    private SegmentNodeStorePersistence persistence;

    @Activate
    public void activate(ComponentContext context, Configuration config) throws IOException {
        persistence = createDynamoPersistence(config);
        registration = context.getBundleContext().registerService(SegmentNodeStorePersistence.class.getName(), persistence, new Properties());
    }

    @Deactivate
    public void deactivate() {
        if (registration != null) {
            registration.unregister();
            registration = null;
        }
        persistence = null;
    }

    private static SegmentNodeStorePersistence createDynamoPersistence(Configuration configuration) throws IOException {
        try {
            AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
            if (configuration.region() != null) {
                builder.withRegion(configuration.region());
            }
            if (configuration.accessKey() != null && configuration.secretKey() != null) {
                builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(configuration.accessKey(), configuration.secretKey())));
            }
            if (configuration.endpoint() != null && configuration.region() != null) {
                builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(configuration.endpoint(), configuration.region()));
            }
            return new DynamoPersistence(builder.build(), configuration.prefix());
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

}

