package org.apache.jackrabbit.oak.remote.server;

import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.remote.proto.SegmentServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SegmentService extends SegmentServiceGrpc.SegmentServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(SegmentService.class);

    private final Set<StreamObserver<StringValue>> observers = Collections.synchronizedSet(new HashSet<>());

    public SegmentService(SegmentWriteListener segmentWriteListener) {
        segmentWriteListener.setDelegate(this::onNewSegment);
    }

    public void onNewSegment(String segmentBlobName) {
        StringValue wrapped = StringValue.newBuilder().setValue(segmentBlobName).build();
        for (StreamObserver<StringValue> o : observers) {
            try {
                o.onNext(wrapped);
            } catch (Exception e) {
                log.error("Can't send event", e);
            }
        }
    }

    @Override
    public StreamObserver<Empty> observeSegments(StreamObserver<StringValue> responseObserver) {
        observers.add(responseObserver);
        return new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty empty) {
            }

            @Override
            public void onError(Throwable throwable) {
                observers.remove(responseObserver);
            }

            @Override
            public void onCompleted() {
                observers.remove(responseObserver);
            }
        };
    }


}
