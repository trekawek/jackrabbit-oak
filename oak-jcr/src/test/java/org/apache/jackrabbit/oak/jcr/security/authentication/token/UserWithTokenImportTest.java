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
package org.apache.jackrabbit.oak.jcr.security.authentication.token;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.security.auth.Subject;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;

import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test XML import of users with persisted tokens.
 */
public class UserWithTokenImportTest extends AbstractRepositoryTest implements TokenConstants {

    private static final String XML_USER_WITH_TOKENS = "TokenImportTest-userWithTokens.xml";

    public UserWithTokenImportTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void importUserWithTokens() throws Exception {
        Session s = null;
        try {
            s = createSession(false);
            Node parent = doImport(s, UserConstants.DEFAULT_USER_PATH, XML_USER_WITH_TOKENS);
            Node user = parent.getNode("t");
            Node tokens = user.getNode(".tokens");
            assertTokensAreMissing(tokens, "88dccc3c-6698-464f-a095-db0a046aa37e");
            s.save();
        } finally {
            if (s != null) {
                s.logout();
            }
        }
    }

    @Test
    public void importUserWithTokensAsSystem() throws Exception {
        Session s = null;
        try {
            s = createSession(true);
            Node parent = doImport(s, UserConstants.DEFAULT_USER_PATH, XML_USER_WITH_TOKENS);
            Node user = parent.getNode("t");
            Node tokens = user.getNode(".tokens");
            assertTokenExists(tokens, "88dccc3c-6698-464f-a095-db0a046aa37e");
            s.save();
        } finally {
            if (s != null) {
                s.logout();
            }
        }
    }

    private void assertTokensAreMissing(Node tokens, String tokenUuid) throws RepositoryException {
        assertFalse(tokens.hasNode(tokenUuid));
    }

    private void assertTokenExists(Node tokens, String tokenUuid) throws RepositoryException {
        assertTrue(tokens.hasNode(tokenUuid));
        Node token = tokens.getNode(tokenUuid);
        assertEquals(tokenUuid, token.getProperty(JCR_UUID).getString());
        assertEquals("Custom property ip is missing", "127.0.0.1", token.getProperty("ip").getString());
        assertTrue("Token expiry is not in the future", token.getProperty(TOKEN_ATTRIBUTE_EXPIRY).getDate().after(Calendar.getInstance()));
        assertEquals("Invalid token key", "{SHA-256}eadf03e7e25fda1d-3aa620e6365002f595334046bc2baee85920321a4f9b4a6ff942dd137c0e3420", token.getProperty(TOKEN_ATTRIBUTE_KEY).getString());
    }

    private Session createSession(boolean isSystem) throws Exception {
        if (isSystem) {
            return Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<Session>) () -> getRepository().login(null, null));
        } else {
            return getAdminSession();
        }
    }

    private Node doImport(Session importSession, String parentPath, String xml) throws Exception {
        try (InputStream in = getClass().getResourceAsStream(xml)) {
            importSession.importXML(parentPath, in, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
            return importSession.getNode(parentPath);
        }
    }
}
