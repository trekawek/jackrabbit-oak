package org.apache.jackrabbit.oak.remote.client.persistence;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.jackrabbit.oak.segment.azure.AzureBlobMetadata;
import org.apache.jackrabbit.oak.segment.azure.AzureSegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.azure.AzureUtilities;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.getSegmentFileName;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.readBufferFully;

public class SegmentTailingReader implements SegmentArchiveReader  {

    private static final Logger log = LoggerFactory.getLogger(SegmentTailingReader.class);

    private final SegmentArchiveManager archiveManager;

    private final String archiveName;

    private final CloudBlobDirectory segmentStoreDirectory;

    private final Map<UUID, AzureSegmentArchiveEntry> index = new ConcurrentHashMap<>();

    private final AtomicLong length = new AtomicLong();

    private boolean closed = false;

    public SegmentTailingReader(SegmentArchiveManager archiveManager, String archiveName, CloudBlobDirectory segmentStoreDirectory) throws IOException, URISyntaxException {
        this.archiveManager = archiveManager;
        this.archiveName = archiveName;
        this.segmentStoreDirectory = segmentStoreDirectory;
    }

    public void onNewSegment(String blobName) {
        String relativePath = blobName.substring(segmentStoreDirectory.getPrefix().length());
        String[] split = relativePath.split("/");
        String archive = split[0];
        String segmentName = split[1];
        if (!archive.equals(this.archiveName)) {
            closed = false;
        }
        try {
            CloudBlob cloudBlob = getBlob(segmentName);
            log.info("Blob name: {} from {}", cloudBlob.getName(), blobName);
            cloudBlob.downloadAttributes();
            addNewSegment(cloudBlob);
        } catch (IOException | StorageException e) {
            log.error("Can't add blob", e);
        }
    }

    public void init() throws URISyntaxException, IOException {
        for (CloudBlob blob : AzureUtilities.getBlobs(segmentStoreDirectory.getDirectoryReference(archiveName))) {
            addNewSegment(blob);
        }
    }

    private void addNewSegment(CloudBlob blob) {
        Map<String, String> metadata = blob.getMetadata();
        if (AzureBlobMetadata.isSegment(metadata)) {
            AzureSegmentArchiveEntry indexEntry = AzureBlobMetadata.toIndexEntry(metadata, (int) blob.getProperties().getLength());
            index.put(new UUID(indexEntry.getMsb(), indexEntry.getLsb()), indexEntry);
        }
        length.addAndGet(blob.getProperties().getLength());
    }

    @Override
    public @Nullable Buffer readSegment(long msb, long lsb) throws IOException {
        AzureSegmentArchiveEntry indexEntry = index.get(new UUID(msb, lsb));
        if (indexEntry == null) {
            return null;
        }
        Buffer buffer = Buffer.allocate(indexEntry.getLength());
        readBufferFully(getBlob(getSegmentFileName(indexEntry)), buffer);
        return buffer;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return index.containsKey(new UUID(msb, lsb));
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        return new ArrayList<>(index.values());
    }

    @Override
    @Nullable
    public Buffer getGraph() throws IOException {
        return null;
    }

    @Override
    public boolean hasGraph() {
        return false;
    }

    @Override
    public @NotNull Buffer getBinaryReferences() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long length() {
        return length.get();
    }

    @Override
    public @NotNull String getName() {
        return archiveName;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public int getEntrySize(int size) {
        return 0;
    }

    public boolean isClosed() {
        return closed;
    }

    private CloudBlockBlob getBlob(String name) throws IOException {
        try {
            return segmentStoreDirectory.getDirectoryReference(archiveName).getBlockBlobReference(name);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    private Buffer readBlob(String name) throws IOException {
        try {
            CloudBlockBlob blob = getBlob(name);
            if (!blob.exists()) {
                return null;
            }
            long length = blob.getProperties().getLength();
            Buffer buffer = Buffer.allocate((int) length);
            AzureUtilities.readBufferFully(blob, buffer);
            return buffer;
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }
}
