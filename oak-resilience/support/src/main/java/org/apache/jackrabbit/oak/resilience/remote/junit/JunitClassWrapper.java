package org.apache.jackrabbit.oak.resilience.remote.junit;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.jackrabbit.oak.resilience.remote.RemoteMessageProducer;
import org.junit.runner.JUnitCore;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class JunitClassWrapper {

    public static final String MQ_TEST_ID = "TEST_RESULTS";

    public static void main(String... args) throws ClassNotFoundException, IOException, TimeoutException {
        String mqId = System.getProperty(MQ_TEST_ID);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(mqId, false, false, false, null);

        JUnitCore core = new JUnitCore();
        core.addListener(new JunitBroadcaster(channel, mqId));

        Class<?>[] classes = new Class<?>[args.length];
        int i = 0;
        for (String className : args) {
            classes[i++] = Class.forName(className);
        }
        try {
            core.run(classes);
        } finally {
            RemoteMessageProducer.close();
        }

        channel.close();
        connection.close();
    }

}
