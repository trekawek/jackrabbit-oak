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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.junit.Test;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Session;
import javax.jcr.nodetype.PropertyDefinition;
import java.security.PrivilegedActionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenImporterTest extends AbstractSecurityTest {

    private TokenImporter importer;

    @Override
    public void before() throws Exception {
        super.before();
        importer = new TokenImporter();
    }

    private boolean init(boolean systemSession, int uuidBehaviour) throws PrivilegedActionException {
        ContentSession session;
        if (systemSession) {
            session = createSystemSession();
        } else {
            session = adminSession;
        }
        return importer.init(mock(Session.class), session.getLatestRoot(), getNamePathMapper(), false, uuidBehaviour, new ReferenceChangeTracker(), getSecurityProvider());
    }

    @Test
    public void testInitUnsupportedUuidBehaviour() throws PrivilegedActionException {
        assertFalse(init(true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW));
    }

    @Test
    public void testInitSupportedUuidBehaviour() throws PrivilegedActionException {
        // the TokenImporter can be initialized multiple times
        assertTrue(init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING));
        assertTrue(init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING));
        assertTrue(init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW));
    }

    @Test
    public void testProcessReferencesIsNop() throws PrivilegedActionException {
        // no initialization required
        importer.processReferences();

        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
        importer.processReferences();
    }

    @Test
    public void testHandlePropInfoForTokenWithSystemSession() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);
        Tree tree = mockTreeWithPrimaryType(TokenConstants.TOKEN_NT_NAME);

        assertTrue(importer.handlePropInfo(tree, mock(PropInfo.class), mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandlePropInfoForNonTokenWithSystemSession() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);
        Tree tree = mockTreeWithPrimaryType(JcrConstants.NT_UNSTRUCTURED);

        assertFalse(importer.handlePropInfo(tree, mock(PropInfo.class), mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandlePropInfoNonSystemSession() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);
        Tree tree = mockTreeWithPrimaryType(TokenConstants.TOKEN_NT_NAME);

        assertFalse(importer.handlePropInfo(tree, mock(PropInfo.class), mock(PropertyDefinition.class)));
    }

    private static Tree mockTreeWithPrimaryType(String primaryType) {
        Tree tree = mock(Tree.class);
        PropertyState state = PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, primaryType, Type.NAME);
        when(tree.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(state);
        return tree;
    }

}