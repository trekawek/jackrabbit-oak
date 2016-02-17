package org.apache.jackrabbit.oak.resilience.junit;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.jackrabbit.oak.resilience.remote.junit.TestEventType;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class TestResultReader {

    private final Channel channel;

    private final String mqTestId;

    private final RunListener listener;

    private final Object monitor;

    private volatile boolean finished;

    public TestResultReader(Channel channel, String mqTestId, RunListener listener) throws IOException {
        this.channel = channel;
        this.mqTestId = mqTestId;
        this.listener = listener;
        this.monitor = new Object();
    }

    public void read() throws IOException {
        channel.queueDeclare(mqTestId, false, false, false, null);

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(mqTestId, consumer);
        try {
            while (!finished) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                ByteArrayInputStream bis = new ByteArrayInputStream(delivery.getBody());
                ObjectInputStream ois = new ObjectInputStream(bis);
                TestEventType type = (TestEventType) ois.readObject();
                Object payload = ois.readObject();
                handle(type, payload);
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void handle(TestEventType type, Object payload) throws Exception {
        switch (type) {
        case TEST_ASSUMPTION_FAILURE:
            listener.testAssumptionFailure((Failure) payload);
            break;
        case TEST_FAILURE:
            listener.testFailure((Failure) payload);
            break;
        case TEST_FINISHED:
            listener.testFinished((Description) payload);
            break;
        case TEST_IGNORED:
            listener.testIgnored((Description) payload);
            break;
        case TEST_RUN_FINISHED:
            listener.testRunFinished((Result) payload);
            synchronized (monitor) {
                finished = true;
                monitor.notifyAll();
            }
            break;
        case TEST_RUN_STARTED:
            listener.testRunStarted((Description) payload);
            break;
        case TEST_STARTED:
            listener.testStarted((Description) payload);
            break;
        default:
            break;
        }
    }
}
