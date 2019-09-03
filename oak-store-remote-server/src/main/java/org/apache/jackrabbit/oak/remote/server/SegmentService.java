package org.apache.jackrabbit.oak.remote.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.remote.common.SegmentWriteListener;
import org.apache.jackrabbit.oak.remote.proto.SegmentProtos;
import org.apache.jackrabbit.oak.remote.proto.SegmentProtos.PrivateSegment;
import org.apache.jackrabbit.oak.remote.proto.SegmentProtos.SegmentBlob;
import org.apache.jackrabbit.oak.remote.proto.SegmentServiceGrpc;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

public class SegmentService extends SegmentServiceGrpc.SegmentServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(SegmentService.class);

    private final Set<StreamObserver<SegmentBlob>> observers = Collections.synchronizedSet(new HashSet<>());

    private final FileStore fileStore;

    private final PrivateFileStores privateFileStores;

    public SegmentService(SegmentWriteListener segmentWriteListener, FileStore fileStore, PrivateFileStores privateFileStores) {
        this.fileStore = fileStore;
        this.privateFileStores = privateFileStores;
        segmentWriteListener.setDelegate(this::onNewSegment);
    }

    private void onNewSegment(SegmentBlob segmentBlob) {
        for (StreamObserver<SegmentBlob> o : observers) {
            try {
                privateFileStores.onNewSharedSegment(segmentBlob);
                o.onNext(segmentBlob);
            } catch (Exception e) {
                log.error("Can't send event", e);
            }
        }
    }

    @Override
    public StreamObserver<Empty> observeSegments(StreamObserver<SegmentBlob> responseObserver) {
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

    @Override
    public void getSegment(SegmentProtos.SegmentId request, StreamObserver<SegmentProtos.Segment> responseObserver) {
        try {
            SegmentIdProvider segmentIdProvider = fileStore.getSegmentIdProvider();
            SegmentId segmentId = segmentIdProvider.newSegmentId(request.getMsb(), request.getLsb());
            while (!fileStore.containsSegment(segmentId)) {
                Thread.sleep(100);
            }

            Segment segment = fileStore.readSegment(segmentId);
            ByteString.Output output = ByteString.newOutput(segment.size());
            segment.writeTo(output);

            SegmentProtos.Segment.Builder responseBuilder = SegmentProtos.Segment.newBuilder();
            responseBuilder
                    .setSegmentData(output.toByteString())
                    .getSegmentIdBuilder()
                        .setMsb(segmentId.getMostSignificantBits())
                        .setLsb(segmentId.getLeastSignificantBits());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (IOException | InterruptedException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void newPrivateSegment(PrivateSegment request, StreamObserver<Empty> responseObserver) {
        privateFileStores.onNewPrivateSegment(request.getSegmentStoreDir(), request.getSegmentBlob());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
