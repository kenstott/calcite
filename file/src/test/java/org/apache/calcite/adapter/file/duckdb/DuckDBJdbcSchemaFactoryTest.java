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
