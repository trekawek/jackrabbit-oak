/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.base.Function;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Lists.transform;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.FULL_TEXT_ENABLED;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_HYBRID_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LuceneHybridTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(LuceneHybridTest.class);

    private static final int ASYNC_LUCENE_DELAY = 2;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private DocumentStore documentStore;

    private BlobStore blobStore;

    private boolean initialized;

    private int clusterId;

    private MemoryDirectoryStorage directoryStorage;

    @Before
    @Override
    public void before() throws Exception {
        documentStore = new MemoryDocumentStore();
        blobStore = new MemoryBlobStore();
        initialized = false;
        clusterId = 1;

        session = createRepository().login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
        createTestIndexNode();
    }

    @Override
    protected ContentRepository createRepository() {
        MemoryDirectoryStorage directoryStorage = new MemoryDirectoryStorage();
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(null, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100), null);

        MonitoringBackgroundObserver monitoringMemoryObserver = new MonitoringBackgroundObserver(Executors.newSingleThreadExecutor());
        IndexTracker tracker = new IndexTracker(directoryStorage, null, monitoringMemoryObserver);
        LuceneMemoryUpdater memoryObserver = new LuceneMemoryUpdater(directoryStorage, tracker);
        monitoringMemoryObserver.addObserver(memoryObserver);

        LuceneIndexProvider provider = new LuceneIndexProvider(tracker);

        if (this.directoryStorage == null) {
            this.directoryStorage = directoryStorage;
        }

        DocumentMK.Builder builder = new DocumentMK.Builder();
        builder.setDocumentStore(documentStore);
        builder.setClusterId(clusterId++);
        builder.setLeaseCheck(false);
        builder.setBlobStore(blobStore);

        NodeStore ns = builder.getNodeStore();

        Oak oak = new Oak(ns);
        if (!initialized) {
            initialized = true;
            oak.with(new InitialContent());
            oak.with(new RepositoryInitializer() {
                @Override
                public void initialize(@Nonnull NodeBuilder builder) {
                    createIndexes(builder);
                }

            });
            oak.withAsyncIndexing("async-lucene", ASYNC_LUCENE_DELAY);
        } else {
            oak.withAsyncIndexing("async-lucene", Integer.MAX_VALUE);
        }

        return oak
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(monitoringMemoryObserver)
                .createContentRepository();
    }

    private void createIndexes(NodeBuilder root) {
        NodeBuilder asyncDef = createIndex(root, "test-index-async", of("myprop"), TYPE_LUCENE, "async-lucene");
        asyncDef.setProperty(PROP_HYBRID_INDEX, true);
    }

    @Test
    public void testHybridIndex() throws Exception {
        ContentRepository extRepo = createRepository();
        ContentSession extSession = extRepo.login(null, null);

        Tree localTest = root.getTree("/").addChild("test-local");
        localTest.addChild("z").setProperty("myprop", "xyz", Type.STRING);
        root.commit();

        waitUntilTraverseIndexed("The initial node hasn't been indexed", 1);
        waitUntilHybridIndexed("The initial node hasn't been indexed", 1, 1, 1);

        localTest = root.getTree("/").addChild("test2");
        localTest.addChild("a").setProperty("myprop", "xyz", Type.STRING);
        localTest.addChild("b").setProperty("myprop", "xyz", Type.STRING);
        localTest.addChild("c").setProperty("myprop", "xyz", Type.STRING);
        root.commit();

        waitUntilHybridIndexed("The memory index hasn't been used for local changes", 4, 1, 4);
        waitUntilAsyncIndexed("The memory index hasn't been purged for local changes", 4);

        Root extRoot = extSession.getLatestRoot();
        Tree test = extRoot.getTree("/").addChild("test-remote");
        test.addChild("a").setProperty("myprop", "xyz", Type.STRING);
        test.addChild("b").setProperty("myprop", "xyz", Type.STRING);
        test.addChild("c").setProperty("myprop", "xyz", Type.STRING);
        extRoot.commit();

        waitUntilHybridIndexed("The memory index hasn't been used for remote changes", 7, 4, 3);
        waitUntilAsyncIndexed("The memory index hasn't been purged for remote changes", 7);
    }

    @Test
    public void testResultsOrder() throws Exception {
        Tree localTest = root.getTree("/").addChild("test-local");
        localTest.addChild("fourth").setProperty("myprop", "aaa004", Type.STRING);
        root.commit();
        waitUntilTraverseIndexed("The initial node hasn't been indexed", 1);
        waitUntilHybridIndexed("The initial node hasn't been indexed", 1, 1, 1);

        localTest.addChild("fourth").setProperty("myprop", "aaa004", Type.STRING);
        localTest.addChild("second").setProperty("myprop", "aaa002", Type.STRING);
        root.commit();
        waitUntilAsyncIndexed("Asynchronous nodes hasn't been indexed", 2);

        localTest.addChild("third").setProperty("myprop", "aaa003", Type.STRING);
        localTest.addChild("first").setProperty("myprop", "aaa001", Type.STRING);
        root.commit();

        List<String> paths = waitUntilIndexed("Hybrid index hasn't been used", 4, "lucene:hybrid:", 2, 2, "order by s.myprop");
        assertEquals(asList("first", "second", "third", "fourth"), transform(paths, new Function<String, String>() {
            @Nullable
            @Override
            public String apply(@Nullable String input) {
                return substringAfterLast(input, "/");
            }
        }));
    }

    @Test
    public void testChangesVisibility() throws Exception {
        Tree localTest = root.getTree("/").addChild("test-local");
        localTest.addChild("z").setProperty("myprop", "xyz", Type.STRING);
        root.commit();

        waitUntilTraverseIndexed("The initial node hasn't been indexed", 1);
        waitUntilHybridIndexed("The initial node hasn't been indexed", 1, 1, 1);

        localTest = root.getTree("/").addChild("test2");
        for (int i = 0; i < 500; i++) {
            String query = "select [jcr:path] from [nt:base] as s where s.[myprop] = 'xyz" + i + "'";

            localTest.addChild("c-" + i).setProperty("myprop", "xyz" + i, Type.STRING);
            root.commit();
            root.refresh();
            List<String> result = executeQuery(query, "JCR-SQL2");
            if (!singletonList("/test2/c-" + i).equals(result)) {
                fail("The changes are not visible immediately");
            }
        }
    }


    private List<String> waitUntilTraverseIndexed(String failMessage, int howManyResults) throws InterruptedException {
        return waitUntilIndexed(failMessage, howManyResults, "traverse", -1, -1, null);
    }

    private List<String> waitUntilAsyncIndexed(String failMessage, int howManyResults) throws InterruptedException {
        return waitUntilIndexed(failMessage, howManyResults, "lucene:test-index-async", -1, -1, null);
    }

    private List<String> waitUntilHybridIndexed(String failMessage, int howManyResults, int asyncCount, int memoryCount) throws InterruptedException {
        return waitUntilIndexed(failMessage, howManyResults, "lucene:hybrid:", asyncCount, memoryCount, null);
    }

    private List<String> waitUntilIndexed(String failMessage, int howManyResults, String indexType, int asyncCount, int memoryCount, String order) throws InterruptedException {
        StringBuilder builder = new StringBuilder("select [jcr:path] from [nt:base] as s where s.[myprop] IS NOT NULL");
        if (order != null) {
            builder.append(" ").append(order);
        }
        String query = builder.toString();

        long start = System.currentTimeMillis();

        String description = null;
        List<String> paths = Collections.emptyList();

        while (System.currentTimeMillis() - start < ASYNC_LUCENE_DELAY * 1000 + 500) {
            root.refresh();

            description = explain(query);
            paths = executeQuery(query, "JCR-SQL2");

            boolean valid = true;
            valid &= paths.size() == howManyResults;
            valid &= description.contains(indexType);
            if (asyncCount > -1 && memoryCount > -1) {
                valid &= description.contains(String.format("):%d,%d]", asyncCount, memoryCount));
            }

            if (valid) {
                log.info("Valid results: {}", paths);
                log.info("Valid explanation: {}", description);
                return paths;
            }

            Thread.sleep(100);
        }

        log.info(description);
        log.info(paths.toString());

        fail(failMessage);
        return paths;
    }

    private String explain(String query) {
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    private static NodeBuilder createIndex(NodeBuilder root, String name, Set<String> includes, String type, String async) {
        NodeBuilder defParent = root.getChildNode(INDEX_DEFINITIONS_NAME);
        NodeBuilder def = defParent.child(name);
        def.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME);
        def.setProperty(PROP_NAME, name);
        def.setProperty(TYPE_PROPERTY_NAME, type);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FULL_TEXT_ENABLED, false);
        def.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, includes, STRINGS));
        def.setProperty(INDEX_PATH, String.format("/%s/%s", INDEX_DEFINITIONS_NAME, name));
        if (async != null) {
            def.setProperty(ASYNC_PROPERTY_NAME, async);
        }
        NodeBuilder updatedDef = IndexDefinition.updateDefinition(def.getNodeState().builder());
        defParent.setChildNode(name, updatedDef.getNodeState());
        return defParent.getChildNode(name);
    }
}