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
package org.apache.jackrabbit.oak.security.authentication.token;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedPropertyImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.PropertyDefinition;

/**
 * This ProtectedPropertyImporter is responsible for importing the rep:Token
 * nodes. The rep:Token nodes are not protected, but all their properties are.
 *
 * It can work in two modes: if the provided Session is a system session, then
 * the importer will rewrite all the token properties. Such a change is normally
 * rejected by {@link TokenValidatorProvider}, but the validator accepts token
 * modifications property made within system session.
 *
 * If it's not the system session, the token nodes are removed, as their required
 * properties can't be set.
 */
public class TokenImporter implements ProtectedPropertyImporter, TokenConstants {

    private static final Logger log = LoggerFactory.getLogger(TokenImporter.class);

    private boolean isSystemSession;

    //----------------------------------------------< ProtectedItemImporter >---
    @Override
    public boolean init(@NotNull Session session, @NotNull Root root, @NotNull NamePathMapper namePathMapper, boolean isWorkspaceImport, int uuidBehavior, @NotNull ReferenceChangeTracker referenceTracker, @NotNull SecurityProvider securityProvider) {
        isSystemSession = root.getContentSession().getAuthInfo().getPrincipals().contains(SystemPrincipal.INSTANCE);
        if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW) {
            log.debug("ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW isn't for the token import");
            return false;
        }
        return true;
    }

    @Override
    public void processReferences() {
        // nothing to
    }

    //------------------------------------------< ProtectedPropertyImporter >---
    /**
     * Only the system session is allowed to update the rep:Token properties.
     * Otherwise the {@link org.apache.jackrabbit.oak.security.authentication.token.TokenValidatorProvider}
     * will reject the change.
     *
     * If this importer works on top of system session, the properties will be
     * set to parent. Otherwise method will do nothing and return false.
     *
     * @param parent The affected parent node.
     * @param protectedPropInfo The {@code PropInfo} to be imported.
     * @param def The property definition determined by the importer that
     * calls this method.
     * @return true if the parent is rep:Token and the importer was able to import
     * the property; false otherwise
     */
    @Override
    public boolean handlePropInfo(@NotNull Tree parent, @NotNull PropInfo protectedPropInfo, @NotNull PropertyDefinition def) throws RepositoryException {
        if (isTokenTree(parent)) {
            if (isSystemSession) {
                parent.setProperty(protectedPropInfo.asPropertyState(def));
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * If the importer doesn't work on system session, the whole rep:Token node
     * should be removed from the imported content.
     *
     * @param protectedParent The protected parent tree.
     */
    @Override
    public void propertiesCompleted(@NotNull Tree protectedParent) throws IllegalStateException {
        if (isTokenTree(protectedParent)) {
            if (!isSystemSession) {
                log.debug("Found {} node managed by system, => Removed from imported scope.", TOKEN_NT_NAME);
                protectedParent.remove();
            }
        }

    }

    private boolean isTokenTree(@NotNull Tree tree) {
        return TOKEN_NT_NAME.equals(TreeUtil.getPrimaryTypeName(tree));
    }
}
