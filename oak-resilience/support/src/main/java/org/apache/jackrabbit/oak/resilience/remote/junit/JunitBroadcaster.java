package org.apache.jackrabbit.oak.resilience.remote.junit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import com.rabbitmq.client.Channel;

public class JunitBroadcaster extends RunListener {

    private final Channel channel;

    private final String queueId;

    public JunitBroadcaster(Channel channel, String queueId) {
        this.channel = channel;
        this.queueId = queueId;
    }

    @Override
    public void testRunStarted(Description description) throws Exception {
        send(TestEventType.TEST_RUN_STARTED, description);
    }

    @Override
    public void testRunFinished(Result result) throws Exception {
        send(TestEventType.TEST_RUN_FINISHED, result);
    }

    @Override
    public void testStarted(Description description) throws Exception {
        send(TestEventType.TEST_STARTED, description);
    }

    @Override
    public void testFinished(Description description) throws Exception {
        send(TestEventType.TEST_FINISHED, description);
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        send(TestEventType.TEST_FAILURE, failure);
    }

    @Override
    public void testAssumptionFailure(Failure failure) {
        try {
            send(TestEventType.TEST_ASSUMPTION_FAILURE, failure);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void testIgnored(Description description) throws Exception {
        send(TestEventType.TEST_IGNORED, description);
    }

    private void send(TestEventType eventType, Serializable payload) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(eventType);
        oos.writeObject(payload);
        oos.close();

        channel.basicPublish("", queueId, null, bos.toByteArray());
    }
}
