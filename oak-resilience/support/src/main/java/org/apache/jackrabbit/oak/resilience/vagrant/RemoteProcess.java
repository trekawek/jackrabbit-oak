package org.apache.jackrabbit.oak.resilience.vagrant;

import static java.lang.System.currentTimeMillis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class RemoteProcess {

    private static final Logger LOG = LoggerFactory.getLogger(VagrantVM.class);

    private final Process process;

    private final BufferedReader stdoutReader;

    private final QueueingConsumer consumer;

    private final Channel channel;

    public RemoteProcess(Process process, Channel channel, String mqId) throws IOException {
        this.process = process;
        this.channel = channel;
        this.consumer = new QueueingConsumer(channel);

        channel.queueDeclare(mqId, false, false, false, null);
        channel.basicConsume(mqId, consumer);

        this.stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    }

    public BufferedReader getStdout() {
        return stdoutReader;
    }

    public boolean waitForMessage(String message, long durationSec) throws IOException {
        long start = currentTimeMillis();
        while ((currentTimeMillis() - start) < durationSec * 1000) {
            logStdout(100);

            Delivery delivery;
            try {
                delivery = consumer.nextDelivery(10);
            } catch (Exception e) {
                throw new IOException(e);
            }
            if (delivery == null) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            } else {
                String line = new String(delivery.getBody());
                if (line != null) {
                    LOG.info("Got message: {}", line);
                }
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                if (message.equals(line)) {
                    return true;
                }
            }
        }
        return false;
    }

    public String getMessage() throws IOException {
        try {
            Delivery delivery = consumer.nextDelivery();
            return new String(delivery.getBody());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public boolean waitForFinish() throws IOException {
        try {
            logStdout(10000);
            return process.waitFor(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private void logStdout(long maxDurationMillis) throws IOException {
        long start = currentTimeMillis();
        while ((currentTimeMillis() - start) < maxDurationMillis) {
            String line = stdoutReader.readLine();
            if (line == null) {
                return;
            }
            LOG.info(line);
        }
    }
}
