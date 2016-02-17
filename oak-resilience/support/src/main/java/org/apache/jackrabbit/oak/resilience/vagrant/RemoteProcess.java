package org.apache.jackrabbit.oak.resilience.vagrant;

import static java.lang.System.currentTimeMillis;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class RemoteProcess {

    private final Process process;

    private final Thread stdoutLogger;

    private final QueueingConsumer consumer;

    public RemoteProcess(Process process, Channel channel, String mqId) throws IOException {
        this.process = process;
        this.consumer = new QueueingConsumer(channel);

        channel.queueDeclare(mqId, false, false, false, null);
        channel.basicConsume(mqId, true, consumer);

        stdoutLogger = new Thread(new StreamLogger(process.getInputStream()));
        stdoutLogger.start();
    }

    public boolean waitForMessage(String message, long durationSec) throws IOException {
        long end = currentTimeMillis() + durationSec * 1000;
        while (true) {
            long timeout = end - currentTimeMillis();
            if (timeout <= 0) {
                return false;
            }
            Delivery delivery = null;
            try {
                delivery = consumer.nextDelivery(timeout);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            if (delivery == null) {
                return false;
            }
            if (message.equals(new String(delivery.getBody()))) {
                return true;
            }
        }
    }

    public String getMessage() throws IOException {
        try {
            Delivery delivery = consumer.nextDelivery();
            return new String(delivery.getBody());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public int waitForFinish() throws IOException {
        try {
            int result = process.waitFor();
            stdoutLogger.join();
            return result;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
