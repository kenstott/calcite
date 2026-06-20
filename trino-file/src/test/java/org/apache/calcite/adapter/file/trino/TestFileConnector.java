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
package org.apache.calcite.adapter.file.trino;

import com.google.common.collect.ImmutableMap;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test that installs {@link FilePlugin} against a local directory of CSV files and runs
 * SQL through the full Trino stack. Exercises the riskiest integration point: the inline Calcite
 * model loads {@code FileSchemaFactory}, discovers the {@code events} table, and serves it over
 * Avatica JDBC with {@link org.apache.calcite.adapter.trino.CalciteClient}'s type mapping.
 */
@Tag("integration")
class TestFileConnector
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // The directory of Parquet files bundled as a test resource (.../test/resources/files).
        // Parquet is read natively by DuckDB with no Hadoop CSV->Parquet conversion (which fails on
        // the JDK 25 that Trino requires).
        String dir = Paths.get(getClass().getResource("/files").toURI()).toString();

        Session session = testSessionBuilder()
                .setCatalog("file")
                .setSchema("files")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new FilePlugin());
        queryRunner.createCatalog("file", "file", ImmutableMap.of(
                "glob", dir,
                // Uses the connector's default DUCKDB engine, which reads CSV natively without the
                // Hadoop dependency that breaks the PARQUET engine on the JDK 25 Trino requires.
                "case-insensitive-name-matching", "true"));
        return queryRunner;
    }

    @Test
    void testShowTables()
    {
        MaterializedResult tables = computeActual("SHOW TABLES FROM file.files");
        assertTrue(tables.getOnlyColumnAsSet().contains("events"),
                "expected 'events' table, got: " + tables.getOnlyColumnAsSet());
    }

    @Test
    void testSelectAll()
    {
        MaterializedResult result =
                computeActual("SELECT id, name, active, amount FROM events ORDER BY id");
        assertEquals(3, result.getRowCount());
        assertEquals("alpha", result.getMaterializedRows().get(0).getField(1));
        assertEquals("gamma", result.getMaterializedRows().get(2).getField(1));
    }
}
