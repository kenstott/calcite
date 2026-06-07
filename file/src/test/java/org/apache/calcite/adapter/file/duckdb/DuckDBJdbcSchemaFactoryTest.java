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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlParser;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link DuckDBJdbcSchemaFactory} covering parser config and static utilities.
 */
@Tag("unit")
class DuckDBJdbcSchemaFactoryTest {

  @Test void testGetParserConfigNotNull() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertNotNull(config);
  }

  @Test void testGetParserConfigUnquotedCasing() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertEquals(Casing.TO_LOWER, config.unquotedCasing());
  }

  @Test void testGetParserConfigQuotedCasing() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertEquals(Casing.UNCHANGED, config.quotedCasing());
  }
}
