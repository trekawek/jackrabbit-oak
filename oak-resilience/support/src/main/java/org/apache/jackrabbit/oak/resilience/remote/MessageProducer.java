package org.apache.jackrabbit.oak.resilience.remote;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducer {

    private static final Logger LOG = LoggerFactory.getLogger(MessageProducer.class);

    public static void write(String message) {
        File file = new File(System.getProperty("MQ_FILE"));
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(new FileOutputStream(file, true));
            writer.println(message);
            writer.flush();
        } catch (FileNotFoundException e) {
            LOG.error("Can't write message", e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
}
