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

import com.google.common.collect.ImmutableMap;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test that registers the generic {@link CalcitePlugin} against an in-memory Calcite
 * model ({@link TestingCalciteSchemaFactory}) and runs SQL through the full Trino stack. Exercises
 * the riskiest integration point: schema/table/column discovery over Avatica JDBC metadata plus
 * {@link CalciteClient}'s type mapping.
 */
@Tag("integration")
class TestCalciteConnector
        extends AbstractTestQueryFramework
{
    private static final String MODEL_JSON =
            "{\"version\":\"1.0\",\"defaultSchema\":\"test\",\"schemas\":[{"
                    + "\"name\":\"test\",\"type\":\"custom\","
                    + "\"factory\":\"org.apache.calcite.adapter.trino.TestingCalciteSchemaFactory\","
                    + "\"operand\":{}}]}";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("calcite")
                .setSchema("test")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new CalcitePlugin());
        queryRunner.createCatalog("calcite", "calcite", ImmutableMap.of(
                "connection-url", "jdbc:calcite:model=inline:" + MODEL_JSON,
                "case-insensitive-name-matching", "true"));
        return queryRunner;
    }

    @Test
    void testShowTables()
    {
        MaterializedResult tables = computeActual("SHOW TABLES FROM calcite.test");
        assertTrue(tables.getOnlyColumnAsSet().contains("events"),
                "expected 'events' table, got: " + tables.getOnlyColumnAsSet());
    }

    @Test
    void testSelectAll()
    {
        MaterializedResult result = computeActual("SELECT id, name, active, amount FROM events ORDER BY id");
        assertEquals(3, result.getRowCount());
        assertEquals(1, result.getMaterializedRows().get(0).getField(0));
        assertEquals("alpha", result.getMaterializedRows().get(0).getField(1));
        assertEquals(true, result.getMaterializedRows().get(0).getField(2));
        assertEquals(10.5, result.getMaterializedRows().get(0).getField(3));
    }

    @Test
    void testPredicateAndProjectionPushedDown()
    {
        // The connector pushes the WHERE predicate and the projection down to the Calcite JDBC
        // source: the scan carries a constraint on the filtered column and requests only the
        // projected column, with no Trino-side Filter node.
        String plan = (String) computeActual(
                "EXPLAIN SELECT name FROM events WHERE active = true").getOnlyValue();
        assertTrue(plan.contains("constraint on [active]"),
                "expected the predicate pushed into the JDBC scan, plan:\n" + plan);
        assertFalse(plan.contains("Filter["),
                "expected no Trino Filter node (predicate fully pushed down), plan:\n" + plan);
    }

    @Test
    void testFilterAndAggregate()
    {
        // Filtering happens in Trino (the Calcite adapter does not push down predicates).
        MaterializedResult count = computeActual("SELECT count(*) FROM events WHERE active = true");
        assertEquals(2L, count.getMaterializedRows().get(0).getField(0));

        MaterializedResult sum = computeActual("SELECT sum(amount) FROM events");
        assertEquals(60.75, sum.getMaterializedRows().get(0).getField(0));
    }
}
