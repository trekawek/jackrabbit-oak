package org.apache.jackrabbit.oak.resilience.vagrant;

import static java.lang.System.currentTimeMillis;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class RemoteJvmProcess {

    private final Process process;

    private final Thread stdoutLogger;

    private final QueueingConsumer consumer;

    private final Channel channel;

    private final String mqId;

    public RemoteJvmProcess(Process process, Channel channel, String mqId) throws IOException {
    private final int pid;

        this.process = process;
        this.consumer = new QueueingConsumer(channel);
        this.channel = channel;
        this.mqId = mqId;

        channel.queueDeclare(mqId, false, false, false, null);
        channel.basicConsume(mqId, true, consumer);
        pid = Integer.valueOf(getMessage());

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

    public void fillMemory(int memorySize, MemoryUnit memoryUnit, int period, TimeUnit timeUnit, boolean onHeap) throws IOException {
        long bytes = memoryUnit.toByte(memorySize);
        if (bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("memorySize is too big");
        }
        channel.basicPublish("", mqId, null, String
                .format("fill_memory\t%d\t%d\t%s", memoryUnit.toByte(memorySize), timeUnit.toMillis(period), Boolean.toString(onHeap)).getBytes());
    }

    public boolean isResponding() {
        try {
            channel.basicPublish("", mqId, null, "ping".getBytes());
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public void kill() {
        process.destroy();
    }
}
