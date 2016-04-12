package org.apache.jackrabbit.oak.resilience.remote;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.oak.resilience.remote.operations.FillMemoryOperation;

import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

public class MainClassWrapper {

    public static void main(String[] args) throws Throwable {
        Class<?> clazz = Class.forName(args[0]);
        String[] mainArgs = new String[args.length - 1];
        for (int i = 0; i < mainArgs.length; i++) {
            mainArgs[i] = args[i + 1];
        }

        final QueueingConsumer consumer = RemoteMessageProducer.getInstance().createConsumer();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        handleDelivery(consumer.nextDelivery());
                    } catch (Exception e) {
                        if (!(e instanceof ShutdownSignalException)) {
                            e.printStackTrace();
                        }
                        break;
                    }
                }
            }
        }).start();

        Method method = clazz.getMethod("main", args.getClass());
        try {
            method.invoke(null, (Object) mainArgs);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } finally {
            RemoteMessageProducer.close();
        }
    }

    private static void handleDelivery(Delivery delivery) {
        if (delivery == null) {
            return;
        }
        String[] message = StringUtils.split(new String(delivery.getBody()), '\t');
        Runnable runnable = JvmOperation.valueOf(message[0].toUpperCase()).getRunnable(message);
        if (runnable != null) {
            new Thread(runnable, "JvmOperation [" + message + "]").start();
        }
    }

    private enum JvmOperation {
        FILL_MEMORY {
            @Override
            protected Runnable getRunnable(String[] message) {
                return new FillMemoryOperation(Integer.parseInt(message[1]), Long.parseLong(message[2]), Boolean.parseBoolean(message[3]));
            }
        },
        PING {
            @Override
            protected Runnable getRunnable(String[] message) {
                return null;
            }
        };
        protected abstract Runnable getRunnable(String[] message);
    }

}