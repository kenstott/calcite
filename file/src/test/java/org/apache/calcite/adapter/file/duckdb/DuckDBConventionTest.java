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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link DuckDBConvention} covering construction and factory method.
 */
@Tag("unit")
class DuckDBConventionTest {

  @Test void testOfCreatesConvention() {
    SqlDialect dialect = DuckDBSqlDialect.DEFAULT;
    DuckDBConvention convention =
        DuckDBConvention.of(dialect,
        org.apache.calcite.linq4j.tree.Expressions.constant(null),
        "duckdb_test");
    assertNotNull(convention);
  }

  @Test void testConstructor() {
    SqlDialect dialect = DuckDBSqlDialect.DEFAULT;
    DuckDBConvention convention =
        new DuckDBConvention(dialect,
        org.apache.calcite.linq4j.tree.Expressions.constant(null),
        "duckdb_test2");
    assertNotNull(convention);
  }
}
