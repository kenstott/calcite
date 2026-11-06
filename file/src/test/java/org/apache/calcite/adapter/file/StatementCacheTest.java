/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that Statement queries benefit from plan caching.
 */
@Tag("unit")
public class StatementCacheTest extends BaseFileTest {
    @TempDir
    java.nio.file.Path tempDir;

    @BeforeEach
    public void setUp() throws Exception {
        // Clear any static caches that might interfere with test isolation
        Sources.clearFileCache();
        // Force garbage collection to release any resources
        System.gc();
        // Wait longer to ensure cleanup is complete
        Thread.sleep(200);
    }

    @AfterEach
    public void tearDown() throws Exception {
        // Clear caches after test to prevent contamination
        Sources.clearFileCache();
        System.gc();
        Thread.sleep(200);
        // Clear system properties
        System.clearProperty("calcite.statement.cache.enabled");
        System.clearProperty("calcite.statement.cache.size");
    }

    @Test public void testStatementReusesCachedPlan() throws Exception {
        // Enable the cache
        System.setProperty("calcite.statement.cache.enabled", "true");
        System.setProperty("calcite.statement.cache.size", "100");

        String query = "SELECT 1 + 1 AS sum_value";
        try {
            try (Connection connection = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
                 CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

                SchemaPlus rootSchema = calciteConnection.getRootSchema();

                // Create a simple test schema
                Map<String, Object> operand = new LinkedHashMap<>();
                operand.put("directory", tempDir.toString());
                operand.put("baseDirectory", tempDir.toString());
                operand.put("ephemeralCache", true);  // Use ephemeral cache for test isolation
                String engine = getExecutionEngine();
                if (engine != null && !engine.isEmpty()) {
                    operand.put("executionEngine", engine.toLowerCase());
                }

                rootSchema.add("TEST",
                    FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));

                // A cached plan answers the repeated statement without parsing it again. A
                // timing ratio against PreparedStatement measured the runner, not the cache.
                assertEquals(1, parses(connection, query, 10),
                    "with the cache on, a repeated statement is planned once");
            }

            // Now test with cache disabled
            System.setProperty("calcite.statement.cache.enabled", "false");

            try (Connection conn2 = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER")) {
                assertEquals(10, parses(conn2, query, 10),
                    "with the cache off, every execution is planned");
            }
        } finally {
            System.clearProperty("calcite.statement.cache.enabled");
            System.clearProperty("calcite.statement.cache.size");
        }
    }

    /** Executes {@code query} {@code times} times on one Statement and returns how often it
     * was parsed, which happens only when the statement is planned rather than taken from the
     * plan cache. */
    private static int parses(Connection connection, String query, int times) throws Exception {
        AtomicInteger parses = new AtomicInteger();
        try (Hook.Closeable ignored =
                 Hook.PARSE_TREE.<Object[]>addThread(args -> parses.incrementAndGet());
             Statement stmt = connection.createStatement()) {
            for (int i = 0; i < times; i++) {
                try (ResultSet rs = stmt.executeQuery(query)) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                }
            }
        }
        return parses.get();
    }
}
