/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.atomic;

import static org.apache.felix.scr.annotations.ReferenceCardinality.OPTIONAL_UNARY;
import static org.apache.felix.scr.annotations.ReferencePolicy.DYNAMIC;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Provide an instance of {@link AtomicCounterEditor}. See {@link AtomicCounterEditor} for
 * behavioural details.
 */
@Component
@Property(name = "type", value = "atomicCounter", propertyPrivate = true)
@Service(EditorProvider.class)
public class AtomicCounterEditorProvider implements EditorProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AtomicCounterEditorProvider.class);

    @Reference(policy = ReferencePolicy.DYNAMIC, cardinality = ReferenceCardinality.OPTIONAL_UNARY, referenceInterface = Clusterable.class)
    private AtomicReference<Clusterable> cluster = new AtomicReference<Clusterable>();

    @Reference(policy = DYNAMIC, cardinality = OPTIONAL_UNARY, referenceInterface = NodeStore.class)
    private volatile AtomicReference<NodeStore> store = new AtomicReference<NodeStore>();    

    private volatile AtomicReference<ScheduledExecutorService> scheduler = new AtomicReference<ScheduledExecutorService>();
    private volatile AtomicReference<Whiteboard> whiteboard = new AtomicReference<Whiteboard>();
    
    private final Supplier<Clusterable> clusterSupplier;
    private final Supplier<ScheduledExecutorService> schedulerSupplier;
    private final Supplier<NodeStore> storeSupplier;
    private final Supplier<Whiteboard> wbSupplier;
    
    /**
     * OSGi oriented constructor where all the required dependencies will be taken care of.
     */
    public AtomicCounterEditorProvider() {
        clusterSupplier = new Supplier<Clusterable>() {
            @Override
            public Clusterable get() {
                return cluster.get();
            }
        };
        schedulerSupplier = new Supplier<ScheduledExecutorService>() {
            @Override
            public ScheduledExecutorService get() {
                return scheduler.get();
            }
        };
        storeSupplier = new Supplier<NodeStore>() {
            @Override
            public NodeStore get() {
                return store.get();
            }
        };
        wbSupplier = new Supplier<Whiteboard>() {
            @Override
            public Whiteboard get() {
                return whiteboard.get();
            }
        };
    }

    /**
     * <p>
     * Plain Java oriented constructor. Refer to
     * {@link AtomicCounterEditor#AtomicCounterEditor(NodeBuilder, String, ScheduledExecutorService, NodeStore)}
     * for constructions details of the actual editor.
     * </p>
     * 
     * <p>
     * Based on the use case this may need an already set of the constructor parameters during the
     * repository construction. Please ensure they're registered before this provider is registered.
     * </p>
     * 
     * @param clusterInfo cluster node information
     * @param executor the executor for running asynchronously.
     * @param store reference to the NodeStore.
     * @param whiteboard the underlying board for picking up the registered {@link CommitHook}
     */
    public AtomicCounterEditorProvider(@Nullable Supplier<Clusterable> clusterInfo, 
                                       @Nullable Supplier<ScheduledExecutorService> executor,
                                       @Nullable Supplier<NodeStore> store,
                                       @Nullable Supplier<Whiteboard> whiteboard) {
        this.clusterSupplier = clusterInfo;
        this.schedulerSupplier = executor;
        this.storeSupplier = store;
        this.wbSupplier = whiteboard;
    }
    
    /**
     * convenience method wrapping logic around {@link AtomicReference}
     * 
     * @return
     */
    private String getInstanceId() {
        Clusterable c = clusterSupplier.get();
        if (c == null) {
            return null;
        } else {
            return c.getInstanceId();
        }
    }
    
    /**
     * convenience method wrapping logic around {@link AtomicReference}
     * 
     * @return
     */
    private ScheduledExecutorService getScheduler() {
        return schedulerSupplier.get();
    }
    
    /**
     * convenience method wrapping logic around {@link AtomicReference}
     * 
     * @return
     */
    private NodeStore getStore() {
        return storeSupplier.get();
    }
    
    /**
     * Convenience method wrapping logic around {@link AtomicReference}
     * 
     * @return
     */
    private Whiteboard getBoard() {
        return wbSupplier.get();
    }
    
    @Activate
    public void activate(BundleContext context) {
        whiteboard.set(new OsgiWhiteboard(context));
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("atomic-counter-%d").build();
        scheduler.set(Executors.newScheduledThreadPool(10, tf));
    }
    
    @Deactivate
    public void deactivate() {
        ScheduledExecutorService ses = getScheduler();
        if (ses == null) {
            LOG.debug("No ScheduledExecutorService found");
        } else {
            LOG.debug("Shutting down ScheduledExecutorService");
            try {
                ses.shutdown();
                ses.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error("InterruptedException white shutting down ScheduledExecutorService", e);
            } finally {
                if (!ses.isTerminated()) {
                    LOG.debug("ScheduledExecutorService not yet shutdown. Cancelling tasks and forcing quit.");
                }
                ses.shutdownNow();
            }
        }
    }

    protected void bindCluster(Clusterable store) {
        this.cluster.set(store);
    }

    protected void unbindCluster(Clusterable store) {
        this.cluster.compareAndSet(store, null);
    }

    protected void bindStore(NodeStore store) {
        this.store.set(store);
    }
    
    protected void unbindStore(NodeStore store) {
        this.store.compareAndSet(store, null);
    }

    @Override
    public Editor getRootEditor(final NodeState before, final NodeState after,
                                final NodeBuilder builder, final CommitInfo info)
                                    throws CommitFailedException {
        return new AtomicCounterEditor(builder, getInstanceId(), getScheduler(), getStore(),
            getBoard());
    }
}
