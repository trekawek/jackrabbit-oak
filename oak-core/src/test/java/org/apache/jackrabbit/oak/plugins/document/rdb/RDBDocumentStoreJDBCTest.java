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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static com.google.common.collect.ImmutableSet.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.UnsupportedEncodingException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentStoreTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.junit.Test;

/**
 * Tests checking certain JDBC related features.
 */
public class RDBDocumentStoreJDBCTest extends AbstractDocumentStoreTest {

    public RDBDocumentStoreJDBCTest(DocumentStoreFixture dsf) {
        super(dsf);
        assumeTrue(super.rdbDataSource != null);
    }

    @Test
    public void batchUpdateResult() throws SQLException {

        // https://issues.apache.org/jira/browse/OAK-3938
        assumeTrue(super.dsf != DocumentStoreFixture.RDB_ORACLE);

        String table = ((RDBDocumentStore) super.ds).getTable(Collection.NODES).getName();

        Connection con = super.rdbDataSource.getConnection();
        con.setReadOnly(false);
        try {
            PreparedStatement st = con.prepareStatement("DELETE FROM " + table + " WHERE ID in (?, ?, ?)");
            setIdInStatement(st, 1, "key-1");
            setIdInStatement(st, 2, "key-2");
            setIdInStatement(st, 3, "key-3");
            st.executeUpdate();
            st.close();
            con.commit();

            st = con.prepareStatement("INSERT INTO " + table + " (id) VALUES (?)");
            setIdInStatement(st, 1, "key-3");
            st.executeUpdate();
            st.close();
            con.commit();

            removeMe.add("key-3");

            PreparedStatement batchSt = con.prepareStatement("UPDATE " + table + " SET data = '{}' WHERE id = ?");
            setIdInStatement(batchSt, 1, "key-1");
            batchSt.addBatch();

            setIdInStatement(batchSt, 1, "key-2");
            batchSt.addBatch();

            setIdInStatement(batchSt, 1, "key-3");
            batchSt.addBatch();

            int[] batchResult = batchSt.executeBatch();
            batchSt.close();
            con.commit();

            // System.out.println(super.dsname + " " +
            // Arrays.toString(batchResult));

            assertEquals(3, batchResult.length);
            assertFalse("Row was updated although not present, status: " + batchResult[0], isSuccess(batchResult[0]));
            assertFalse("Row was updated although not present, status: " + batchResult[1], isSuccess(batchResult[1]));
            assertTrue("Row should be updated correctly.", isSuccess(batchResult[2]));
        } finally {
            con.close();
        }
    }

    @Test
    public void batchFailingInsertResult() throws SQLException {

        String table = ((RDBDocumentStore) super.ds).getTable(Collection.NODES).getName();

        Connection con = super.rdbDataSource.getConnection();
        con.setReadOnly(false);
        try {
            // remove key-1, key-2, key-3
            PreparedStatement st = con.prepareStatement("DELETE FROM " + table + " WHERE ID in (?, ?, ?)");
            setIdInStatement(st, 1, "key-1");
            setIdInStatement(st, 2, "key-2");
            setIdInStatement(st, 3, "key-3");
            st.executeUpdate();
            st.close();
            con.commit();

            removeMe.add("key-3");

            // insert key-3
            st = con.prepareStatement("INSERT INTO " + table + " (id) VALUES (?)");
            setIdInStatement(st, 1, "key-3");
            st.executeUpdate();
            st.close();
            con.commit();

            removeMe.add("key-1");
            removeMe.add("key-2");

            // try to insert key-1, key-2, key-3
            PreparedStatement batchSt = con.prepareStatement("INSERT INTO " + table + " (id) VALUES (?)");
            setIdInStatement(batchSt, 1, "key-1");
            batchSt.addBatch();

            setIdInStatement(batchSt, 1, "key-2");
            batchSt.addBatch();

            setIdInStatement(batchSt, 1, "key-3");
            batchSt.addBatch();

            int[] batchResult = null;
            try {
                batchSt.executeBatch();
                fail("Batch operation should fail");
            } catch (BatchUpdateException e) {
                batchResult = e.getUpdateCounts();
            }
            batchSt.close();
            con.commit();

            // System.out.println(super.dsname + " " + Arrays.toString(batchResult));

            boolean partialSuccess = false;

            if (batchResult.length >= 2) {
                if (isSuccess(batchResult[0]) && isSuccess(batchResult[1])) {
                    partialSuccess = true;
                }
            }

            if (batchResult.length == 3) {
                assertTrue("Row already exists, shouldn't be inserted.", !isSuccess(batchResult[2]));
            }

            PreparedStatement rst = con.prepareStatement("SELECT id FROM " + table + " WHERE id in (?, ?, ?)");
            setIdInStatement(rst, 1, "key-1");
            setIdInStatement(rst, 2, "key-2");
            setIdInStatement(rst, 3, "key-3");
            ResultSet results = rst.executeQuery();
            Set<String> ids = new HashSet<String>();
            while (results.next()) {
                ids.add(getIdFromRS(results, 1));
            }
            results.close();
            rst.close();

            if (partialSuccess) {
                assertEquals("Some of the rows weren't inserted.", of("key-1", "key-2", "key-3"), ids);
            }
            else {
                assertEquals("Failure reported, but rows inserted.", of("key-3"), ids);
            }
        } finally {
            con.close();
        }
    }

    private static boolean isSuccess(int result) {
        return result == 1 || result == Statement.SUCCESS_NO_INFO;
    }

    private void setIdInStatement(PreparedStatement stmt, int idx, String id) throws SQLException {
        boolean binaryId = ((RDBDocumentStore) super.ds).getTable(Collection.NODES).isIdBinary();
        if (binaryId) {
            try {
                stmt.setBytes(idx, id.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException ex) {
                throw new DocumentStoreException(ex);
            }
        } else {
            stmt.setString(idx, id);
        }
    }

    private String getIdFromRS(ResultSet rs, int idx) throws SQLException {
        boolean binaryId = ((RDBDocumentStore) super.ds).getTable(Collection.NODES).isIdBinary();
        if (binaryId) {
            try {
                return new String(rs.getBytes(idx), "UTF-8");
            } catch (UnsupportedEncodingException ex) {
                throw new DocumentStoreException(ex);
            }
        } else {
            return rs.getString(idx);
        }
    }
}
