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
package org.apache.jackrabbit.oak.query;

import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;

/**
 * A query result row that keeps all data in memory.
 */
public class Row implements Comparable<Row> {

    private final Query query;
    private final String[] paths;
    private final ScalarImpl[] values;
    private final ScalarImpl[] orderValues;

    Row(Query query, String[] paths, ScalarImpl[] values, ScalarImpl[] orderValues) {
        this.query = query;
        this.paths = paths;
        this.values = values;
        this.orderValues = orderValues;
    }

    public String getPath() {
        if (paths.length > 1) {
            throw new IllegalArgumentException("More than one selector");
        }
        return paths[0];
    }

    public String getPath(String selectorName) {
        int index = query.getSelectorIndex(selectorName);
        if (paths == null || index >= paths.length) {
            return null;
        }
        return paths[index];
    }

    public ScalarImpl getValue(String columnName) {
        return values[query.getColumnIndex(columnName)];
    }

    public ScalarImpl[] getValues() {
        ScalarImpl[] v2 = new ScalarImpl[values.length];
        System.arraycopy(values, 0, v2, 0, v2.length);
        return v2;
    }

    @Override
    public int compareTo(Row o) {
        return query.compareRows(orderValues, o.orderValues);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        for (SelectorImpl s : query.getSelectors()) {
            String n = s.getSelectorName();
            String p = getPath(n);
            if (p != null) {
                buff.append(n).append(": ").append(p).append(" ");
            }
        }
        ColumnImpl[] cols = query.getColumns();
        for (int i = 0; i < values.length; i++) {
            ColumnImpl c = cols[i];
            String n = c.getColumnName();
            if (n != null) {
                buff.append(n).append(": ").append(values[i]).append(" ");
            }
        }
        return buff.toString();
    }

}
