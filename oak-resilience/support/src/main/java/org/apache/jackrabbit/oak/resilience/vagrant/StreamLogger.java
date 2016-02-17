package org.apache.jackrabbit.oak.resilience.vagrant;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamLogger implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamLogger.class);

    private final InputStream is;

    public StreamLogger(InputStream is) {
        this.is = is;
    }

    @Override
    public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                LOG.info(line);
            }
        } catch (IOException e) {
            LOG.error("Can't read stream", e);
        }
    }

}
