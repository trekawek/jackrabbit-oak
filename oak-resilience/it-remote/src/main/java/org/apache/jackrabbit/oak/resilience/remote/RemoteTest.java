package org.apache.jackrabbit.oak.resilience.remote;

import java.io.IOException;

public class RemoteTest {

    public static void main(String[] args) throws IOException {
        System.out.println("Hello world");
        MessageProducer.write("that's fine");
    }

}
