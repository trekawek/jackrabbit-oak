package org.apache.jackrabbit.oak.resilience.junit;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.resilience.remote.junit.TestEventType;
import org.apache.jackrabbit.oak.resilience.vagrant.RemoteProcess;
import org.junit.internal.TextListener;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class JunitProcess {

    private final RemoteProcess process;

    private final Channel channel;

    private final String mqTestId;

    private final List<RunListener> listeners;

    private volatile Result result;

    public JunitProcess(RemoteProcess process, Channel channel, String mqTestId) throws IOException {
        this.process = process;
        this.channel = channel;
        this.mqTestId = mqTestId;
        this.listeners = new ArrayList<RunListener>();
        listeners.add(new RunListener() {
            @Override
            public void testRunFinished(Result result) {
                JunitProcess.this.result = result;
            }
        });
        listeners.add(new TextListener(System.out));
    }

    public void addListener(RunListener listener) {
        listeners.add(listener);
    }

    public Result read() throws IOException {
        try {
            QueueingConsumer consumer = new QueueingConsumer(channel);

            channel.queueDeclare(mqTestId, false, false, false, null);
            channel.basicConsume(mqTestId, true, consumer);
            while (result == null) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery(10);
                if (delivery == null) {
                    continue;
                }
                ByteArrayInputStream bis = new ByteArrayInputStream(delivery.getBody());
                ObjectInputStream ois = new ObjectInputStream(bis);
                TestEventType type = (TestEventType) ois.readObject();
                Object payload = ois.readObject();
                handle(type, payload);
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            process.waitForFinish();
        }
        return result;
    }

    private void handle(TestEventType type, Object payload) throws Exception {
        for (RunListener listener : listeners) {
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
}
