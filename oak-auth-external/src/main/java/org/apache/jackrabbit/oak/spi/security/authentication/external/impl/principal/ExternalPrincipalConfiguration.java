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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModuleFactory;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalManagerImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@code PrincipalConfiguration} interface that provides
 * principal management for {@link Group principals} associated with
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity external identities}
 * managed outside of the scope of the repository by an
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider}.
 *
 * @since Oak 1.5.3
 * @see <a href="https://issues.apache.org/jira/browse/OAK-4101">OAK-4101</a>
 */
@Component(
        metatype = true,
        label = "Apache Jackrabbit Oak External PrincipalConfiguration",
        immediate = true
)
@Service({PrincipalConfiguration.class, SecurityConfiguration.class})
public class ExternalPrincipalConfiguration extends ConfigurationBase implements PrincipalConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ExternalPrincipalConfiguration.class);

    private SyncConfigTracker syncConfigTracker;
    private SyncHandlerMappingTracker syncHandlerMappingTracker;

    @SuppressWarnings("UnusedDeclaration")
    public ExternalPrincipalConfiguration() {
        super();
    }

    public ExternalPrincipalConfiguration(SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    //---------------------------------------------< PrincipalConfiguration >---
    @Nonnull
    @Override
    public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
        return new PrincipalManagerImpl(getPrincipalProvider(root, namePathMapper));
    }

    @Nonnull
    @Override
    public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
        if (dynamicMembershipEnabled()) {
            UserConfiguration uc = getSecurityProvider().getConfiguration(UserConfiguration.class);
            return new ExternalGroupPrincipalProvider(root, uc, namePathMapper, syncConfigTracker.getAutoMembership());
        } else {
            return EmptyPrincipalProvider.INSTANCE;
        }
    }

    //----------------------------------------------< SecurityConfiguration >---
    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        return new ExternalIdentityRepositoryInitializer();
    }

    @Nonnull
    @Override
    public List<? extends ValidatorProvider> getValidators(@Nonnull String workspaceName, @Nonnull Set<Principal> principals, @Nonnull MoveTracker moveTracker) {
        return ImmutableList.of(new ExternalIdentityValidatorProvider(principals));
    }

    @Nonnull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return ImmutableList.<ProtectedItemImporter>of(new ExternalIdentityImporter());
    }

    //----------------------------------------------------< SCR integration >---
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(BundleContext bundleContext, Map<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));
        syncHandlerMappingTracker = new SyncHandlerMappingTracker(bundleContext);
        syncHandlerMappingTracker.open();

        syncConfigTracker = new SyncConfigTracker(bundleContext, syncHandlerMappingTracker);
        syncConfigTracker.open();


    }

    @SuppressWarnings("UnusedDeclaration")
    @Deactivate
    private void deactivate() {
        if (syncConfigTracker != null) {
            syncConfigTracker.close();
        }
        if (syncHandlerMappingTracker != null) {
            syncHandlerMappingTracker.close();
        }
    }

    //------------------------------------------------------------< private >---

    private boolean dynamicMembershipEnabled() {
        return syncConfigTracker != null && syncConfigTracker.isEnabled;
    }

    /**
     * Implementation of the {@code PrincipalProvider} interface that never
     * returns any principals.
     */
    private static final class EmptyPrincipalProvider implements PrincipalProvider {

        private static final PrincipalProvider INSTANCE = new EmptyPrincipalProvider();

        private EmptyPrincipalProvider() {}

        @Override
        public Principal getPrincipal(@Nonnull String principalName) {
            return null;
        }

        @Nonnull
        @Override
        public Set<Group> getGroupMembership(@Nonnull Principal principal) {
            return ImmutableSet.of();
        }

        @Nonnull
        @Override
        public Set<? extends Principal> getPrincipals(@Nonnull String userID) {
            return ImmutableSet.of();
        }

        @Nonnull
        @Override
        public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
            return Iterators.emptyIterator();
        }

        @Nonnull
        @Override
        public Iterator<? extends Principal> findPrincipals(int searchType) {
            return Iterators.emptyIterator();
        }
    }

    /**
     * {@code ServiceTracker} to detect any {@link SyncHandler} that has
     * dynamic membership enabled.
     */
    private static final class SyncConfigTracker extends ServiceTracker {

        private final SyncHandlerMappingTracker mappingTracker;

        private Set<ServiceReference> enablingRefs = new HashSet<ServiceReference>();
        private boolean isEnabled = false;

        public SyncConfigTracker(@Nonnull BundleContext context, @Nonnull SyncHandlerMappingTracker mappingTracker) {
            super(context, SyncHandler.class.getName(), null);
            this.mappingTracker = mappingTracker;
        }

        @Override
        public Object addingService(ServiceReference reference) {
            if (hasDynamicMembership(reference)) {
                enablingRefs.add(reference);
                isEnabled = true;
            }
            return super.addingService(reference);
        }

        @Override
        public void modifiedService(ServiceReference reference, Object service) {
            if (hasDynamicMembership(reference)) {
                enablingRefs.add(reference);
                isEnabled = true;
            } else {
                enablingRefs.remove(reference);
                isEnabled = !enablingRefs.isEmpty();
            }
            super.modifiedService(reference, service);
        }

        @Override
        public void removedService(ServiceReference reference, Object service) {
            enablingRefs.remove(reference);
            isEnabled = !enablingRefs.isEmpty();
            super.removedService(reference, service);
        }

        private static boolean hasDynamicMembership(ServiceReference reference) {
            return PropertiesUtil.toBoolean(reference.getProperty(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP), DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP_DEFAULT);
        }

        private Map<String, String[]> getAutoMembership() {
            Map<String, String[]> autoMembership = new HashMap<String, String[]>();
            for (ServiceReference ref : enablingRefs) {
                String syncHandlerName = PropertiesUtil.toString(ref.getProperty(DefaultSyncConfigImpl.PARAM_NAME), DefaultSyncConfigImpl.PARAM_NAME_DEFAULT);
                String[] membership = PropertiesUtil.toStringArray(ref.getProperty(DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP), new String[0]);

                for (String idpName : mappingTracker.getIdpNames(syncHandlerName)) {
                    String[] previous = autoMembership.put(idpName, membership);
                    if (previous != null) {
                        String msg = (Arrays.equals(previous, membership)) ? "Duplicate" : "Colliding";
                        log.debug(msg + " auto-membership configuration for IDP '{}'; replacing previous values {} by {} defined by SyncHandler '{}'",
                                idpName, Arrays.toString(previous), Arrays.toString(membership), syncHandlerName);
                    }
                }
            }
            return autoMembership;
        }
    }

    /**
     * {@code ServiceTracker} to detect any {@link SyncHandler} that has
     * dynamic membership enabled.
     */
    private static final class SyncHandlerMappingTracker extends ServiceTracker {

        private Map<ServiceReference, String[]> referenceMap = new HashMap<ServiceReference, String[]>();

        public SyncHandlerMappingTracker(@Nonnull BundleContext context) {
            super(context, SyncHandlerMapping.class.getName(), null);
        }

        @Override
        public Object addingService(ServiceReference reference) {
            addMapping(reference);
            return super.addingService(reference);
        }

        @Override
        public void modifiedService(ServiceReference reference, Object service) {
            addMapping(reference);
            super.modifiedService(reference, service);
        }

        @Override
        public void removedService(ServiceReference reference, Object service) {
            referenceMap.remove(reference);
            super.removedService(reference, service);
        }

        private void addMapping(ServiceReference reference) {
            String idpName = PropertiesUtil.toString(reference.getProperty(ExternalLoginModuleFactory.PARAM_IDP_NAME), null);
            String syncHandlerName = PropertiesUtil.toString(reference.getProperty(ExternalLoginModuleFactory.PARAM_SYNC_HANDLER_NAME), null);

            if (idpName != null && syncHandlerName != null) {
                referenceMap.put(reference, new String[]{syncHandlerName, idpName});
            } else {
                log.warn("Ignoring SyncHandlerMapping with incomplete mapping of IDP '{}' and SyncHandler '{}'", idpName, syncHandlerName);
            }
        }

        private Iterable<String> getIdpNames(@Nonnull final String syncHandlerName) {
            return Iterables.filter(Iterables.transform(referenceMap.values(), new Function<String[], String>() {
                        @Nullable
                        @Override
                        public String apply(@Nullable String[] input) {
                            if (input != null && input.length == 2) {
                                if (syncHandlerName.equals(input[0])) {
                                    return input[1];
                                } // else: different sync-handler
                            } else {
                                log.warn("Unexpected value of reference map. Expected String[] with length = 2");
                            }
                            return null;
                        }
                    }
            ), Predicates.notNull());
        }
    }
}