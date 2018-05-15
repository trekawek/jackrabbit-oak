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
package org.apache.jackrabbit.oak.kv.store.dynamo;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.arakelian.docker.junit.DockerRule;
import com.arakelian.docker.junit.model.ImmutableDockerConfig;
import com.spotify.docker.client.DefaultDockerClient;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class DynamoRule implements TestRule {

    private final DockerRule wrappedRule;

    public DynamoRule() {
        wrappedRule = new DockerRule(ImmutableDockerConfig.builder()
                .image("cnadiminti/dynamodb-local")
                .name("oak-test-dynamo")
                .ports("8000")
                .addStartedListener(container -> container.waitForPort("8000/tcp"))
                .alwaysRemoveContainer(true)
                .build());

    }

    public AmazonDynamoDB getDynamoDB()  {
        int mappedPort = wrappedRule.getContainer().getPortBinding("8000/tcp").getPort();
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:" + mappedPort, "us-west-2"))
                .build();
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        try {
            DefaultDockerClient client = DefaultDockerClient.fromEnv().connectTimeoutMillis(5000L).readTimeoutMillis(20000L).build();
            client.ping();
            client.close();
        } catch (Exception e) {
            Assume.assumeNoException(e);
        }

        return wrappedRule.apply(statement, description);
    }
}

