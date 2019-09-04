package org.apache.jackrabbit.oak.remote.common;

import org.apache.jackrabbit.oak.remote.proto.SegmentProtos;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.function.Consumer;

public class SegmentWriteListener extends IOMonitorAdapter {

    private static final Logger log = LoggerFactory.getLogger(SegmentWriteListener.class);

    private Consumer<SegmentProtos.SegmentBlob> delegate = s -> {};

    @Override
    public void afterSegmentWrite(File file, long msb, long lsb, int length, long elapsed) {
        SegmentProtos.SegmentBlob.Builder builder = SegmentProtos.SegmentBlob.newBuilder();
        builder
                .setBlobName(file.toString())
                .getSegmentIdBuilder()
                    .setMsb(msb)
                    .setLsb(lsb);

        try {
            delegate.accept(builder.build());
        } catch (Exception e) {
            log.error("Can't pass the blob to delegate", e);
        }
    }

    public void setDelegate(Consumer<SegmentProtos.SegmentBlob> delegate) {
        this.delegate = delegate;
    }

}
