package org.apache.jackrabbit.oak.remote.server;

import org.apache.jackrabbit.oak.remote.proto.SegmentProtos;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;

import java.io.File;
import java.util.function.Consumer;

public class SegmentWriteListener extends IOMonitorAdapter {

    private Consumer<SegmentProtos.SegmentBlob> delegate = s -> {};

    @Override
    public void afterSegmentWrite(File file, long msb, long lsb, int length, long elapsed) {
        SegmentProtos.SegmentBlob.Builder builder = SegmentProtos.SegmentBlob.newBuilder();
        builder
                .setBlobName(file.toString())
                .getSegmentIdBuilder()
                    .setMsb(msb)
                    .setLsb(lsb);
        delegate.accept(builder.build());
    }

    public void setDelegate(Consumer<SegmentProtos.SegmentBlob> delegate) {
        this.delegate = delegate;
    }

}
