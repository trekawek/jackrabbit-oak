/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_STRING_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_TEMPLATE_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener.LOG_SNFE;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_NODE_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_STRING_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_TEMPLATE_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.CacheWeights.NodeCacheWeigher;
import org.apache.jackrabbit.oak.segment.CacheWeights.StringCacheWeigher;
import org.apache.jackrabbit.oak.segment.CacheWeights.TemplateCacheWeigher;
import org.apache.jackrabbit.oak.segment.RecordCache;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener;
import org.apache.jackrabbit.oak.segment.SegmentVersion;
import org.apache.jackrabbit.oak.segment.WriterCacheManager;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.proc.Proc;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.gc.DelegatingGCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.gc.LoggingGCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for creating {@link FileStore} instances.
 */
public class FileStoreBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

    private static final boolean MEMORY_MAPPING_DEFAULT =
            "64".equals(System.getProperty("sun.arch.data.model", "32"));

    public static final int DEFAULT_MAX_FILE_SIZE = 256;

    @Nonnull
    private final File directory;

    @CheckForNull
    private BlobStore blobStore;   // null ->  store blobs inline

    private int maxFileSize = DEFAULT_MAX_FILE_SIZE;

    private int segmentCacheSize = DEFAULT_SEGMENT_CACHE_MB;

    private int stringCacheSize = DEFAULT_STRING_CACHE_MB;

    private int templateCacheSize = DEFAULT_TEMPLATE_CACHE_MB;

    private int stringDeduplicationCacheSize = DEFAULT_STRING_CACHE_SIZE;

    private int templateDeduplicationCacheSize = DEFAULT_TEMPLATE_CACHE_SIZE;

    private int nodeDeduplicationCacheSize = DEFAULT_NODE_CACHE_SIZE;

    private boolean memoryMapping = MEMORY_MAPPING_DEFAULT;

    private SegmentNodeStorePersistence persistence;

    @Nonnull
    private StatisticsProvider statsProvider = StatisticsProvider.NOOP;

    @Nonnull
    private SegmentGCOptions gcOptions = defaultGCOptions();

    @CheckForNull
    private EvictingWriteCacheManager cacheManager;

    private class FileStoreGCListener extends DelegatingGCMonitor implements GCListener {
        @Override
        public void compactionSucceeded(@Nonnull GCGeneration newGeneration) {
            compacted();
            if (cacheManager != null) {
                cacheManager.evictOldGeneration(newGeneration.getGeneration());
            }
        }

        @Override
        public void compactionFailed(@Nonnull GCGeneration failedGeneration) {
            if (cacheManager != null) {
                cacheManager.evictGeneration(failedGeneration.getGeneration());
            }
        }
    }

    @Nonnull
    private final FileStoreGCListener gcListener = new FileStoreGCListener();

    @Nonnull
    private SegmentNotFoundExceptionListener snfeListener = LOG_SNFE;

    private IOMonitor ioMonitor = new IOMonitorAdapter();

    private boolean strictVersionCheck;
    
    private boolean built;

    /**
     * Create a new instance of a {@code FileStoreBuilder} for a file store.
     * @param directory  directory where the tar files are stored
     * @return a new {@code FileStoreBuilder} instance.
     */
    @Nonnull
    public static FileStoreBuilder fileStoreBuilder(@Nonnull File directory) {
        return new FileStoreBuilder(directory);
    }

    private FileStoreBuilder(@Nonnull File directory) {
        this.directory = checkNotNull(directory);
        this.gcListener.registerGCMonitor(new LoggingGCMonitor(LOG));
        this.persistence = new TarPersistence(directory);
    }

    /**
     * Specify the {@link BlobStore}.
     * @param blobStore
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withBlobStore(@Nonnull BlobStore blobStore) {
        this.blobStore = checkNotNull(blobStore);
        return this;
    }

    /**
     * Maximal size of the generated tar files in MB.
     * @param maxFileSize
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withMaxFileSize(int maxFileSize) {
        this.maxFileSize = maxFileSize;
        return this;
    }

    /**
     * Size of the segment cache in MB.
     * @param segmentCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withSegmentCacheSize(int segmentCacheSize) {
        this.segmentCacheSize = segmentCacheSize;
        return this;
    }

    /**
     * Size of the string cache in MB.
     * @param stringCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withStringCacheSize(int stringCacheSize) {
        this.stringCacheSize = stringCacheSize;
        return this;
    }

    /**
     * Size of the template cache in MB.
     * @param templateCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withTemplateCacheSize(int templateCacheSize) {
        this.templateCacheSize = templateCacheSize;
        return this;
    }

    /**
     * Number of items to keep in the string deduplication cache
     * @param stringDeduplicationCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withStringDeduplicationCacheSize(int stringDeduplicationCacheSize) {
        this.stringDeduplicationCacheSize = stringDeduplicationCacheSize;
        return this;
    }

    /**
     * Number of items to keep in the template deduplication cache
     * @param templateDeduplicationCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withTemplateDeduplicationCacheSize(int templateDeduplicationCacheSize) {
        this.templateDeduplicationCacheSize = templateDeduplicationCacheSize;
        return this;
    }

    /**
     * Number of items to keep in the node deduplication cache
     * @param nodeDeduplicationCacheSize  None negative cache size. Must be a power of 2.
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withNodeDeduplicationCacheSize(int nodeDeduplicationCacheSize) {
        this.nodeDeduplicationCacheSize = nodeDeduplicationCacheSize;
        return this;
    }

    /**
     * Turn memory mapping on or off
     * @param memoryMapping
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withMemoryMapping(boolean memoryMapping) {
        this.memoryMapping = memoryMapping;
        return this;
    }

    /**
     * Set memory mapping to the default value based on OS properties
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withDefaultMemoryMapping() {
        this.memoryMapping = MEMORY_MAPPING_DEFAULT;
        return this;
    }

    /**
     * {@link GCMonitor} for monitoring this files store's gc process.
     * @param gcMonitor
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withGCMonitor(@Nonnull GCMonitor gcMonitor) {
        this.gcListener.registerGCMonitor(checkNotNull(gcMonitor));
        return this;
    }

    /**
     * {@link StatisticsProvider} for collecting statistics related to FileStore
     * @param statisticsProvider
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withStatisticsProvider(@Nonnull StatisticsProvider statisticsProvider) {
        this.statsProvider = checkNotNull(statisticsProvider);
        return this;
    }

    /**
     * {@link SegmentGCOptions} the garbage collection options of the store
     * @param gcOptions
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withGCOptions(SegmentGCOptions gcOptions) {
        this.gcOptions = checkNotNull(gcOptions);
        return this;
    }

    /**
     * {@link SegmentNotFoundExceptionListener} listener for  {@code SegmentNotFoundException}
     * @param snfeListener, the actual listener
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withSnfeListener(@Nonnull SegmentNotFoundExceptionListener snfeListener) {
        this.snfeListener = checkNotNull(snfeListener);
        return this;
    }

    @Nonnull
    public FileStoreBuilder withIOMonitor(IOMonitor ioMonitor) {
        this.ioMonitor = checkNotNull(ioMonitor);
        return this;
    }

    /**
     * Enable strict version checking. With strict version checking enabled Oak
     * will fail to start if the store version does not exactly match this Oak version.
     * This is useful to e.g. avoid inadvertent upgrades during when running offline
     * compaction accidentally against an older version of a store.
     * @param strictVersionCheck  enables strict version checking iff {@code true}.
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withStrictVersionCheck(boolean strictVersionCheck) {
        this.strictVersionCheck = strictVersionCheck;
        return this;
    }
    
    public FileStoreBuilder withCustomPersistence(SegmentNodeStorePersistence persistence) throws IOException {
        this.persistence = persistence;
        return this;
    }

    public Proc.Backend buildProcBackend(AbstractFileStore fileStore) throws IOException {
        SegmentArchiveManager archiveManager = persistence.createArchiveManager(true, new IOMonitorAdapter(), new FileStoreMonitorAdapter());

        return new Proc.Backend() {

            @Override
            public boolean tarExists(String name) {
                try {
                    return archiveManager.listArchives().contains(name);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Optional<Long> getTarSize(String name) {
                try (SegmentArchiveReader reader = archiveManager.open(name)) {
                    return Optional.ofNullable(reader).map(SegmentArchiveReader::length);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Iterable<String> getTarNames() {
                try {
                    return archiveManager.listArchives();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean segmentExists(String name, String segmentId) {
                try (SegmentArchiveReader reader = archiveManager.open(name)) {
                    if (reader == null) {
                        return false;
                    }
                    return segmentExists(reader, UUID.fromString(segmentId));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            boolean segmentExists(SegmentArchiveReader reader, UUID id) {
                return reader.containsSegment(id.getMostSignificantBits(), id.getLeastSignificantBits());
            }

            @Override
            public Iterable<String> getSegmentIds(String name) {
                try (SegmentArchiveReader reader = archiveManager.open(name)) {
                    if (reader == null) {
                        return Collections.emptyList();
                    }
                    return getSegmentIds(reader);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private Iterable<String> getSegmentIds(SegmentArchiveReader reader) {
                List<String> ids = new ArrayList<>();
                for (SegmentArchiveEntry entry : reader.listSegments()) {
                    ids.add(new UUID(entry.getMsb(), entry.getLsb()).toString());
                }
                return ids;
            }

            private Optional<org.apache.jackrabbit.oak.segment.Segment> readSegment(String id) {
                return readSegment(UUID.fromString(id));
            }

            private Optional<org.apache.jackrabbit.oak.segment.Segment> readSegment(UUID id) {
                return readSegment(fileStore.getSegmentIdProvider().newSegmentId(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits()
                ));
            }

            private Optional<org.apache.jackrabbit.oak.segment.Segment> readSegment(SegmentId id) {
                try {
                    return Optional.of(fileStore.readSegment(id));
                } catch (SegmentNotFoundException e) {
                    return Optional.empty();
                }
            }

            @Override
            public Optional<Segment> getSegment(String id) {
                return readSegment(id).map(segment -> new Segment() {

                    @Override
                    public int getGeneration() {
                        return segment.getGcGeneration().getGeneration();
                    }

                    @Override
                    public int getFullGeneration() {
                        return segment.getGcGeneration().getFullGeneration();
                    }

                    @Override
                    public boolean isCompacted() {
                        return segment.getGcGeneration().isCompacted();
                    }

                    @Override
                    public int getLength() {
                        return segment.size();
                    }

                    @Override
                    public int getVersion() {
                        return SegmentVersion.asByte(segment.getSegmentVersion());
                    }

                    @Override
                    public boolean isDataSegment() {
                        return segment.getSegmentId().isDataSegmentId();
                    }

                    @Override
                    public Optional<String> getInfo() {
                        return Optional.ofNullable(segment.getSegmentInfo());
                    }

                });
            }

            @Override
            public Optional<InputStream> getSegmentData(String segmentId) {
                return readSegment(segmentId).map(segment -> {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();

                    try {
                        segment.writeTo(out);
                    } catch (IOException e) {
                        return null;
                    }

                    return new ByteArrayInputStream(out.toByteArray());
                });
            }

            @Override
            public Optional<Iterable<String>> getSegmentReferences(String segmentId) {
                return readSegment(segmentId).map(segment -> {
                    List<String> references = new ArrayList<>(segment.getReferencedSegmentIdCount());

                    for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
                        references.add(segment.getReferencedSegmentId(i).toString());
                    }

                    return references;
                });
            }

            @Override
            public Optional<Iterable<Record>> getSegmentRecords(String segmentId) {
                return readSegment(segmentId).map(segment -> {
                    List<Record> records = new ArrayList<>();

                    segment.forEachRecord((number, type, offset) -> {
                        records.add(new Record() {

                            @Override
                            public int getNumber() {
                                return number;
                            }

                            @Override
                            public int getOffset() {
                                return offset;
                            }

                            @Override
                            public String getType() {
                                return type.name();
                            }

                        });
                    });

                    return records;
                });
            }

            @Override
            public boolean commitExists(String handle) {
                long timestamp = Long.parseLong(handle);

                try (JournalReader reader = new JournalReader(persistence.getJournalFile())) {
                    for (JournalEntry entry : iterable(reader)) {
                        if (entry.getTimestamp() == timestamp) {
                            return true;
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                return false;
            }

            private <T> Iterable<T> iterable(Iterator<T> i) {
                return () -> i;
            }

            @Override
            public Iterable<String> getCommitHandles() {
                try (JournalReader reader = new JournalReader(persistence.getJournalFile())) {
                    List<String> handles = new ArrayList<>();
                    for (JournalEntry entry : iterable(reader)) {
                        handles.add(Long.toString(entry.getTimestamp()));
                    }
                    return handles;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Optional<Commit> getCommit(String handle) {
                JournalEntry entry;

                try (JournalReader reader = new JournalReader(persistence.getJournalFile())) {
                    entry = getEntry(reader, Long.parseLong(handle));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                if (entry == null) {
                    return Optional.empty();
                }

                return Optional.of(new Commit() {

                    @Override
                    public long getTimestamp() {
                        return entry.getTimestamp();
                    }

                    @Override
                    public String getRevision() {
                        return entry.getRevision();
                    }

                    @Override
                    public Optional<NodeState> getRoot() {
                        RecordId id = RecordId.fromString(fileStore.getSegmentIdProvider(), entry.getRevision());
                        return Optional.of(fileStore.getReader().readNode(id));
                    }

                });
            }

            private JournalEntry getEntry(JournalReader reader, long timestamp) {
                for (JournalEntry entry : iterable(reader)) {
                    if (entry.getTimestamp() == timestamp) {
                        return entry;
                    }
                }
                return null;
            }

        };
    }

    /**
     * Create a new {@link FileStore} instance with the settings specified in this
     * builder. If none of the {@code with} methods have been called before calling
     * this method, a file store with the following default settings is returned:
     * <ul>
     * <li>blob store: inline</li>
     * <li>max file size: 256MB</li>
     * <li>cache size: 256MB</li>
     * <li>memory mapping: on for 64 bit JVMs off otherwise</li>
     * <li>whiteboard: none. No {@link GCMonitor} tracking</li>
     * <li>statsProvider: {@link StatisticsProvider#NOOP}</li>
     * <li>GC options: {@link SegmentGCOptions#defaultGCOptions()}</li>
     * </ul>
     *
     * @return a new file store instance
     * @throws IOException
     */
    @Nonnull
    public FileStore build() throws InvalidFileStoreVersionException, IOException {
        checkState(!built, "Cannot re-use builder");
        built = true;
        directory.mkdirs();
        TarRevisions revisions = new TarRevisions(persistence);
        LOG.info("Creating file store {}", this);
        FileStore store;
        try {
            store = new FileStore(this);
        } catch (InvalidFileStoreVersionException | IOException e) {
            try {
                revisions.close();
            } catch (IOException re) {
                LOG.warn("Unable to close TarRevisions", re);
            }
            throw e;
        }
        store.bind(revisions);
        return store;
    }

    /**
     * Create a new {@link ReadOnlyFileStore} instance with the settings specified in this
     * builder. If none of the {@code with} methods have been called before calling
     * this method, a file store with the following default settings is returned:
     * <ul>
     * <li>blob store: inline</li>
     * <li>max file size: 256MB</li>
     * <li>cache size: 256MB</li>
     * <li>memory mapping: on for 64 bit JVMs off otherwise</li>
     * <li>whiteboard: none. No {@link GCMonitor} tracking</li>
     * <li>statsProvider: {@link StatisticsProvider#NOOP}</li>
     * <li>GC options: {@link SegmentGCOptions#defaultGCOptions()}</li>
     * </ul>
     *
     * @return a new file store instance
     * @throws IOException
     */
    @Nonnull
    public ReadOnlyFileStore buildReadOnly() throws InvalidFileStoreVersionException, IOException {
        checkState(!built, "Cannot re-use builder");
        checkState(directory.exists() && directory.isDirectory(),
                "%s does not exist or is not a directory", directory);
        built = true;
        ReadOnlyRevisions revisions = new ReadOnlyRevisions(persistence);
        LOG.info("Creating file store {}", this);
        ReadOnlyFileStore store;
        try {
            store = new ReadOnlyFileStore(this);
        } catch (InvalidFileStoreVersionException | IOException e) {
            try {
                revisions.close();
            } catch (IOException re) {
                LOG.warn("Unable to close ReadOnlyRevisions", re);
            }
            throw e;
        }
        store.bind(revisions);
        return store;
    }

    @Nonnull
    File getDirectory() {
        return directory;
    }

    @CheckForNull
    BlobStore getBlobStore() {
        return blobStore;
    }

    public int getMaxFileSize() {
        return maxFileSize;
    }

    int getSegmentCacheSize() {
        return segmentCacheSize;
    }

    int getStringCacheSize() {
        return stringCacheSize;
    }

    int getTemplateCacheSize() {
        return templateCacheSize;
    }

    boolean getMemoryMapping() {
        return memoryMapping;
    }

    @Nonnull
    GCListener getGcListener() {
        return gcListener;
    }

    @Nonnull
    StatisticsProvider getStatsProvider() {
        return statsProvider;
    }

    @Nonnull
    SegmentGCOptions getGcOptions() {
        return gcOptions;
    }
    
    @Nonnull
    SegmentNotFoundExceptionListener getSnfeListener() {
        return snfeListener;
    }

    SegmentNodeStorePersistence getPersistence() {
        return persistence;
    }

    /**
     * @return  creates or returns the {@code WriterCacheManager} this builder passes or
     *          passed to the store on {@link #build()}.
     *
     * @see #withNodeDeduplicationCacheSize(int)
     * @see #withStringDeduplicationCacheSize(int)
     * @see #withTemplateDeduplicationCacheSize(int)
     */
    @Nonnull
    public WriterCacheManager getCacheManager() {
        if (cacheManager == null) {
            cacheManager = new EvictingWriteCacheManager(stringDeduplicationCacheSize,
                    templateDeduplicationCacheSize, nodeDeduplicationCacheSize);
        }
        return cacheManager;
    }

    IOMonitor getIOMonitor() {
        return ioMonitor;
    }

    boolean getStrictVersionCheck() {
        return strictVersionCheck;
    }

    @Override
    public String toString() {
        return "FileStoreBuilder{" +
                "version=" + getClass().getPackage().getImplementationVersion() +
                ", directory=" + directory +
                ", blobStore=" + blobStore +
                ", maxFileSize=" + maxFileSize +
                ", segmentCacheSize=" + segmentCacheSize +
                ", stringCacheSize=" + stringCacheSize +
                ", templateCacheSize=" + templateCacheSize +
                ", stringDeduplicationCacheSize=" + stringDeduplicationCacheSize +
                ", templateDeduplicationCacheSize=" + templateDeduplicationCacheSize +
                ", nodeDeduplicationCacheSize=" + nodeDeduplicationCacheSize +
                ", memoryMapping=" + memoryMapping +
                ", gcOptions=" + gcOptions +
                '}';
    }

    private static class EvictingWriteCacheManager extends WriterCacheManager.Default {
        public EvictingWriteCacheManager(
                int stringCacheSize,
                int templateCacheSize,
                int nodeCacheSize) {
            super(RecordCache.factory(stringCacheSize, new StringCacheWeigher()),
                RecordCache.factory(templateCacheSize, new TemplateCacheWeigher()),
                PriorityCache.factory(nodeCacheSize, new NodeCacheWeigher()));
        }

        void evictOldGeneration(final int newGeneration) {
            evictCaches(new Predicate<Integer>() {
                @Override
                public boolean apply(Integer generation) {
                    return generation < newGeneration;
                }
            });
        }

        void evictGeneration(final int newGeneration) {
            evictCaches(new Predicate<Integer>() {
                @Override
                public boolean apply(Integer generation) {
                    return generation == newGeneration;
                }
            });
        }
    }
}
