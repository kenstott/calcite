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
package org.apache.calcite.adapter.file.schema;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link SchemaConfigParser}.
 */
@Tag("unit")
class SchemaConfigParserTest {

  @Test void testParseNullConfig() {
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(null);
    assertSame(SchemaStrategy.PARQUET_DEFAULT, result);
  }

  @Test void testParseEmptyConfig() {
    Map<String, Object> config = new HashMap<String, Object>();
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertSame(SchemaStrategy.PARQUET_DEFAULT, result);
  }

  @Test void testParseConfigWithoutSchemaStrategy() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("someOtherKey", "value");
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertSame(SchemaStrategy.PARQUET_DEFAULT, result);
  }

  @Test void testParseSimpleStrategyDefault() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", "default");
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertSame(SchemaStrategy.PARQUET_DEFAULT, result);
  }

  @Test void testParseSimpleStrategyParquetDefault() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", "parquet_default");
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertSame(SchemaStrategy.PARQUET_DEFAULT, result);
  }

  @Test void testParseSimpleStrategyConservative() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", "conservative");
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertSame(SchemaStrategy.CONSERVATIVE, result);
  }

  @Test void testParseSimpleStrategyAggressiveUnion() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", "aggressive_union");
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertSame(SchemaStrategy.AGGRESSIVE_UNION, result);
  }

  @Test void testParseSimpleStrategyUnion() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", "union");
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertSame(SchemaStrategy.AGGRESSIVE_UNION, result);
  }

  @Test void testParseSimpleStrategyLatest() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", "latest");
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertNotNull(result);
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS, result.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.LATEST_FILE, result.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.LATEST_FILE, result.getJsonStrategy());
  }

  @Test void testParseSimpleStrategyLatestSchemaWins() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", "latest_schema_wins");
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertNotNull(result);
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS, result.getParquetStrategy());
  }

  @Test void testParseSimpleStrategyUnionAll() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", "union_all");
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertNotNull(result);
    assertEquals(SchemaStrategy.ParquetStrategy.UNION_ALL_COLUMNS, result.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.UNION_COMMON, result.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.UNION_KEYS, result.getJsonStrategy());
  }

  @Test void testParseSimpleStrategyUnknown() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", "nonexistent_strategy");
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertSame(SchemaStrategy.PARQUET_DEFAULT, result);
  }

  @Test void testParseComplexStrategy() {
    Map<String, Object> strategyMap = new HashMap<String, Object>();
    strategyMap.put("parquet", "UNION_ALL_COLUMNS");
    strategyMap.put("csv", "LATEST_FILE");
    strategyMap.put("json", "UNION_KEYS");
    strategyMap.put("validation", "ERROR");

    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", strategyMap);
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);

    assertNotNull(result);
    assertEquals(SchemaStrategy.ParquetStrategy.UNION_ALL_COLUMNS, result.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.LATEST_FILE, result.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.UNION_KEYS, result.getJsonStrategy());
    assertEquals(SchemaStrategy.ValidationLevel.ERROR, result.getValidationLevel());
  }

  @Test void testParseComplexStrategyWithDefaults() {
    Map<String, Object> strategyMap = new HashMap<String, Object>();
    // All defaults - no keys specified

    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", strategyMap);
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);

    assertNotNull(result);
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS, result.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.RICHEST_FILE, result.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.LATEST_FILE, result.getJsonStrategy());
    assertEquals(SchemaStrategy.ValidationLevel.WARN, result.getValidationLevel());
  }

  @Test void testParseComplexStrategyWithFormatPriority() {
    Map<String, Object> strategyMap = new HashMap<String, Object>();
    List<String> priority = Arrays.asList("json", "csv", "parquet");
    strategyMap.put("formatPriority", priority);

    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", strategyMap);
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);

    assertNotNull(result);
    assertEquals(Arrays.asList("json", "csv", "parquet"), result.getFormatPriority());
  }

  @Test void testParseComplexStrategyInvalidParquet() {
    Map<String, Object> strategyMap = new HashMap<String, Object>();
    strategyMap.put("parquet", "INVALID_STRATEGY");

    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", strategyMap);
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);

    assertNotNull(result);
    // Should fall back to default
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS, result.getParquetStrategy());
  }

  @Test void testParseComplexStrategyInvalidCsv() {
    Map<String, Object> strategyMap = new HashMap<String, Object>();
    strategyMap.put("csv", "INVALID");

    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", strategyMap);
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);

    assertNotNull(result);
    assertEquals(SchemaStrategy.CsvStrategy.RICHEST_FILE, result.getCsvStrategy());
  }

  @Test void testParseComplexStrategyInvalidJson() {
    Map<String, Object> strategyMap = new HashMap<String, Object>();
    strategyMap.put("json", "INVALID");

    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", strategyMap);
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);

    assertNotNull(result);
    assertEquals(SchemaStrategy.JsonStrategy.LATEST_FILE, result.getJsonStrategy());
  }

  @Test void testParseComplexStrategyInvalidValidation() {
    Map<String, Object> strategyMap = new HashMap<String, Object>();
    strategyMap.put("validation", "INVALID");

    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", strategyMap);
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);

    assertNotNull(result);
    assertEquals(SchemaStrategy.ValidationLevel.WARN, result.getValidationLevel());
  }

  @Test void testParseInvalidStrategyType() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("schemaStrategy", Integer.valueOf(42));
    SchemaStrategy result = SchemaConfigParser.parseSchemaStrategy(config);
    assertSame(SchemaStrategy.PARQUET_DEFAULT, result);
  }
}
