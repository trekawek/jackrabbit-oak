package org.apache.jackrabbit.oak.remote.client.persistence;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SegmentTailingReader implements SegmentArchiveReader {

    private static final Logger log = LoggerFactory.getLogger(ArchiveTailingReader.class);

    private final SegmentArchiveManager archiveManager;

    private final String archiveName;

    private final CloudBlobDirectory segmentStoreDirectory;

    private volatile SegmentArchiveReader delegate;


    public SegmentTailingReader(SegmentArchiveManager archiveManager, String archiveName, CloudBlobDirectory segmentStoreDirectory) throws IOException {
        this.archiveManager = archiveManager;
        this.archiveName = archiveName;
        this.segmentStoreDirectory = segmentStoreDirectory;
        updateDelegate();
    }

    private void updateDelegate() throws IOException {
        delegate = archiveManager.forceOpen(archiveName);
    }

    public boolean waitForSegment(long msb, long lsb) throws IOException {
        long start = System.currentTimeMillis();
        while (!delegate.containsSegment(msb, lsb)) {
            if (System.currentTimeMillis() - start > TimeUnit.MINUTES.toMillis(10)) {
                return false;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
            updateDelegate();
        }
        return true;
    }

    public boolean isClosed() throws IOException {
        try {
            return segmentStoreDirectory.getDirectoryReference(archiveName).getBlockBlobReference("closed").exists();
        } catch (StorageException | URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    @Nullable
    public Buffer readSegment(long msb, long lsb) throws IOException {
        return delegate.readSegment(msb, lsb);
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return delegate.containsSegment(msb, lsb);
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        return delegate.listSegments();
    }

    @Override
    @Nullable
    public Buffer getGraph() throws IOException {
        return delegate.getGraph();
    }

    @Override
    public boolean hasGraph() {
        return delegate.hasGraph();
    }

    @Override
    @NotNull
    public Buffer getBinaryReferences() throws IOException {
        return delegate.getBinaryReferences();
    }

    @Override
    public long length() {
        return delegate.length();
    }

    @Override
    public @NotNull String getName() {
        return delegate.getName();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public int getEntrySize(int size) {
        return delegate.getEntrySize(size);
    }

}
