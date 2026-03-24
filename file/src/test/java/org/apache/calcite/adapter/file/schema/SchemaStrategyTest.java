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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SchemaStrategy}.
 */
@Tag("unit")
class SchemaStrategyTest {

  @Test void testParquetDefaultStrategy() {
    SchemaStrategy strategy = SchemaStrategy.PARQUET_DEFAULT;
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS, strategy.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.RICHEST_FILE, strategy.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.LATEST_FILE, strategy.getJsonStrategy());
  }

  @Test void testConservativeStrategy() {
    SchemaStrategy strategy = SchemaStrategy.CONSERVATIVE;
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_FILE, strategy.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.RICHEST_FILE, strategy.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.LATEST_FILE, strategy.getJsonStrategy());
  }

  @Test void testAggressiveUnionStrategy() {
    SchemaStrategy strategy = SchemaStrategy.AGGRESSIVE_UNION;
    assertEquals(SchemaStrategy.ParquetStrategy.UNION_ALL_COLUMNS, strategy.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.UNION_COMMON, strategy.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.UNION_KEYS, strategy.getJsonStrategy());
  }

  @Test void testDefaultFormatPriority() {
    SchemaStrategy strategy = SchemaStrategy.PARQUET_DEFAULT;
    List<String> priority = strategy.getFormatPriority();
    assertNotNull(priority);
    assertEquals(3, priority.size());
    assertEquals("parquet", priority.get(0));
    assertEquals("csv", priority.get(1));
    assertEquals("json", priority.get(2));
  }

  @Test void testDefaultValidationLevel() {
    SchemaStrategy strategy = SchemaStrategy.PARQUET_DEFAULT;
    assertEquals(SchemaStrategy.ValidationLevel.WARN, strategy.getValidationLevel());
  }

  @Test void testCustomStrategy() {
    List<String> priority = Arrays.asList("json", "parquet");
    SchemaStrategy strategy = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.FIRST_FILE,
        SchemaStrategy.CsvStrategy.USER_DEFINED,
        SchemaStrategy.JsonStrategy.INTERSECTION_ONLY,
        priority,
        SchemaStrategy.ValidationLevel.ERROR);

    assertEquals(SchemaStrategy.ParquetStrategy.FIRST_FILE, strategy.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.USER_DEFINED, strategy.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.INTERSECTION_ONLY, strategy.getJsonStrategy());
    assertEquals(priority, strategy.getFormatPriority());
    assertEquals(SchemaStrategy.ValidationLevel.ERROR, strategy.getValidationLevel());
  }

  @Test void testThreeArgConstructorDefaultsFormatPriority() {
    SchemaStrategy strategy = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.UNION_WITH_PROMOTION,
        SchemaStrategy.CsvStrategy.UNION_COMMON,
        SchemaStrategy.JsonStrategy.UNION_KEYS);

    assertEquals(Arrays.asList("parquet", "csv", "json"), strategy.getFormatPriority());
    assertEquals(SchemaStrategy.ValidationLevel.WARN, strategy.getValidationLevel());
  }

  @Test void testToString() {
    SchemaStrategy strategy = SchemaStrategy.PARQUET_DEFAULT;
    String str = strategy.toString();
    assertNotNull(str);
    assertTrue(str.contains("LATEST_SCHEMA_WINS"));
    assertTrue(str.contains("RICHEST_FILE"));
    assertTrue(str.contains("LATEST_FILE"));
    assertTrue(str.contains("WARN"));
  }

  @Test void testParquetStrategyEnumValues() {
    SchemaStrategy.ParquetStrategy[] values = SchemaStrategy.ParquetStrategy.values();
    assertEquals(6, values.length);
  }

  @Test void testCsvStrategyEnumValues() {
    SchemaStrategy.CsvStrategy[] values = SchemaStrategy.CsvStrategy.values();
    assertEquals(4, values.length);
  }

  @Test void testJsonStrategyEnumValues() {
    SchemaStrategy.JsonStrategy[] values = SchemaStrategy.JsonStrategy.values();
    assertEquals(4, values.length);
  }

  @Test void testValidationLevelEnumValues() {
    SchemaStrategy.ValidationLevel[] values = SchemaStrategy.ValidationLevel.values();
    assertEquals(3, values.length);
  }

  @Test void testValidationLevelNone() {
    assertEquals(SchemaStrategy.ValidationLevel.NONE,
        SchemaStrategy.ValidationLevel.valueOf("NONE"));
  }

  @Test void testValidationLevelError() {
    assertEquals(SchemaStrategy.ValidationLevel.ERROR,
        SchemaStrategy.ValidationLevel.valueOf("ERROR"));
  }
}
