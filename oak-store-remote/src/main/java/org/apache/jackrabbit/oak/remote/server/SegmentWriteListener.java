package org.apache.jackrabbit.oak.remote.server;

import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;

import java.io.File;
import java.util.function.Consumer;

public class SegmentWriteListener extends IOMonitorAdapter {

    private Consumer<String> delegate = s -> {};

    @Override
    public void afterSegmentWrite(File file, long msb, long lsb, int length, long elapsed) {
        delegate.accept(file.toString());
    }

    public void setDelegate(Consumer<String> delegate) {
        this.delegate = delegate;
    }

}
