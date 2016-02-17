package org.apache.jackrabbit.oak.resilience.junit;

import java.io.IOException;

import com.rabbitmq.client.Channel;

public class JunitProcess {

    private final TestResultReader reader;

    public JunitProcess(Channel channel, String mqTestId) throws IOException {
        this.reader = new TestResultReader(channel, mqTestId, new LoggingRunListener());
    }

    public void waitForFinish() throws IOException {
        reader.read();
    }
}
