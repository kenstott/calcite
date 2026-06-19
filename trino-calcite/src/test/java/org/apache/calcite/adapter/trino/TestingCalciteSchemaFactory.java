/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.trino;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A self-contained Calcite {@link SchemaFactory} used only by the connector integration test. It
 * exposes a single in-memory {@code events} table with assorted typed columns, so the test can
 * drive the whole Trino -&gt; CalciteClient -&gt; Calcite JDBC path without any external data
 * source (and therefore no Hadoop/Parquet dependency).
 */
public class TestingCalciteSchemaFactory
        implements SchemaFactory
{
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand)
    {
        return new AbstractSchema()
        {
            @Override
            protected Map<String, Table> getTableMap()
            {
                return Map.of("events", new EventsTable());
            }
        };
    }

    /** Three-row, four-column in-memory table. */
    public static final class EventsTable
            extends AbstractTable
            implements ScannableTable
    {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory)
        {
            return typeFactory.builder()
                    .add("id", SqlTypeName.INTEGER)
                    .add("name", SqlTypeName.VARCHAR)
                    .add("active", SqlTypeName.BOOLEAN)
                    .add("amount", SqlTypeName.DOUBLE)
                    .build();
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root)
        {
            List<Object[]> rows = Arrays.asList(
                    new Object[] {1, "alpha", true, 10.5},
                    new Object[] {2, "beta", false, 20.0},
                    new Object[] {3, "gamma", true, 30.25});
            return Linq4j.asEnumerable(rows);
        }
    }
}
