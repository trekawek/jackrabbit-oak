/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.resilience.remote;

import static org.apache.jackrabbit.oak.resilience.vagrant.VagrantVM.MQ_ID;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RemoteMessageProducer {

    private final Connection connection;

    private final Channel channel;

    private final String queueId;

    private static RemoteMessageProducer instance;

    private RemoteMessageProducer() throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try {
            connection = factory.newConnection();
        } catch (TimeoutException e) {
            throw new IOException(e);
        }

        queueId = System.getProperty(MQ_ID);

        channel = connection.createChannel();
        channel.queueDeclare(queueId, false, false, false, null);
    }

    public static RemoteMessageProducer getInstance() throws IOException {
        if (instance == null) {
            synchronized (RemoteMessageProducer.class) {
                if (instance == null) {
                    instance = new RemoteMessageProducer();
                }
            }
        }
        return instance;
    }

    public void publish(String message) throws IOException {
        channel.basicPublish("", queueId, null, message.getBytes());
    }

    static void close() throws IOException, TimeoutException {
        if (instance != null) {
            if (instance.channel != null) {
                instance.channel.close();
            }
            if (instance.connection != null) {
                instance.connection.close();
            }
        }
    }
}
