package org.apache.jackrabbit.oak.resilience.vagrant;

import static java.lang.System.currentTimeMillis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteProcess {

    private static final Logger LOG = LoggerFactory.getLogger(VagrantVM.class);

    private final Process process;

    private final BufferedReader stdoutReader;

    private final BufferedReader mqReader;

    public RemoteProcess(Process process, File mqFile) throws IOException {
        this.process = process;

        mqFile.createNewFile();
        this.stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        this.mqReader = new BufferedReader(new FileReader(mqFile));
    }

    public BufferedReader getStdout() {
        return stdoutReader;
    }

    public boolean waitForMessage(String message, long durationSec) throws IOException {
        long start = currentTimeMillis();
        while ((currentTimeMillis() - start) < durationSec * 1000) {
            logStdout(100);

            String line = mqReader.readLine();
            if (message.equals(line)) {
                return true;
            }

            if (line != null) {
                LOG.info("Got message: {}", line);
                continue;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return false;
    }

    public String getMessage() throws IOException {
        return mqReader.readLine();
    }

    public int waitFor() throws IOException {
        try {
            logStdout(Integer.MAX_VALUE);
            return process.waitFor();
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
