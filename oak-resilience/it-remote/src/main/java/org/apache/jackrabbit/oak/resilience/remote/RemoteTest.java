package org.apache.jackrabbit.oak.resilience.remote;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RemoteTest {

    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println("Hello world");

        RemoteMessageProducer producer = new RemoteMessageProducer();
        producer.publish("that's fine");
        producer.close();

        System.out.println("Finished");
    }

}
