package org.apache.jackrabbit.oak.resilience.vagrant;

import static java.lang.System.currentTimeMillis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class RemoteJvmProcess {

    private final Process process;

    private final Thread stdoutLogger;

    private final QueueingConsumer consumer;

    private final Channel channel;

    private final String mqId;

    private final String controlMqId;

    private final VagrantVM vm;

    private final int pid;

    public RemoteJvmProcess(Process process, Channel channel, String mqId, VagrantVM vm) throws IOException {
        this.process = process;
        this.consumer = new QueueingConsumer(channel);
        this.channel = channel;
        this.mqId = mqId;
        this.controlMqId = mqId + "-control";
        this.vm = vm;

        channel.queueDeclare(mqId, false, false, false, null);
        channel.queueDeclare(controlMqId, false, false, false, null);
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
        channel.basicPublish("", controlMqId, null, String
                .format("fill_memory\t%d\t%d\t%s", memoryUnit.toByte(memorySize), timeUnit.toMillis(period), Boolean.toString(onHeap)).getBytes());
    }

    public int getDescriptorLimit() throws IOException {
        Process process = vm.execProcess(vm.vagrantExecutable, "ssh", "--", "prlimit", "--nofile", "-p", String.valueOf(pid));
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        try {
            reader.readLine();
            Matcher m = Pattern.compile("^NOFILE   max number of open files (\\d+) (\\d+)$").matcher(reader.readLine());
            if (!m.matches()) {
                return -1;
            }
            return Integer.parseInt(m.group(1));
        } finally {
            reader.close();
        }
    }

    public void setDescriptorLimit(int limit) throws IOException {
        vm.ssh("sudo", "prlimit", "--nofile=" + limit, "-p", String.valueOf(pid));
    }

    public boolean isResponding() {
        try {
            channel.basicPublish("", controlMqId, null, "ping".getBytes());
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public void kill() {
        process.destroy();
    }
}
