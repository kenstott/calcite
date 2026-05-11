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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link TableConstraints}.
 */
@Tag("unit")
class TableConstraintsTest {

  @Test void testFromConfigNoConstraints() {
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    List<String> columnNames = Arrays.asList("id", "name", "value");

    Statistic result = TableConstraints.fromConfig(tableConfig, columnNames, null);
    assertSame(Statistics.UNKNOWN, result);
  }

  @Test void testFromConfigNullConstraints() {
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", null);
    List<String> columnNames = Arrays.asList("id", "name");

    Statistic result = TableConstraints.fromConfig(tableConfig, columnNames, null);
    assertSame(Statistics.UNKNOWN, result);
  }

  @Test void testFromConfigWithPrimaryKey() {
    Map<String, Object> constraints = new HashMap<String, Object>();
    constraints.put("primaryKey", Arrays.asList("id"));

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("id", "name", "value");

    Statistic result = TableConstraints.fromConfig(tableConfig, columnNames, 100.0);
    assertNotNull(result);
    assertNotNull(result.getKeys());
    assertEquals(1, result.getKeys().size());
    assertTrue(result.getKeys().get(0).get(0)); // Column 0 (id) is in key
  }

  @Test void testFromConfigWithCompositePrimaryKey() {
    Map<String, Object> constraints = new HashMap<String, Object>();
    constraints.put("primaryKey", Arrays.asList("year", "state"));

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("year", "state", "value");

    Statistic result = TableConstraints.fromConfig(tableConfig, columnNames, null);
    assertNotNull(result);
    assertNotNull(result.getKeys());
    assertEquals(1, result.getKeys().size());
    // The single key is a composite key containing both columns
    assertTrue(result.getKeys().get(0).get(0)); // year at index 0
    assertTrue(result.getKeys().get(0).get(1)); // state at index 1
  }

  @Test void testFromConfigWithUniqueKeys() {
    Map<String, Object> constraints = new HashMap<String, Object>();
    List<List<String>> uniqueKeys = new ArrayList<List<String>>();
    uniqueKeys.add(Arrays.asList("email"));
    uniqueKeys.add(Arrays.asList("ssn"));
    constraints.put("uniqueKeys", uniqueKeys);

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("id", "email", "ssn", "name");

    Statistic result = TableConstraints.fromConfig(tableConfig, columnNames, null);
    assertNotNull(result);
    assertNotNull(result.getKeys());
    assertEquals(2, result.getKeys().size());
  }

  @Test void testFromConfigWithForeignKey() {
    Map<String, Object> fk = new HashMap<String, Object>();
    fk.put("columns", Arrays.asList("dept_id"));
    fk.put("targetTable", "departments");
    fk.put("targetColumns", Arrays.asList("id"));

    Map<String, Object> constraints = new HashMap<String, Object>();
    List<Map<String, Object>> fkList = new ArrayList<Map<String, Object>>();
    fkList.add(fk);
    constraints.put("foreignKeys", fkList);

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("id", "dept_id", "name");

    Statistic result = TableConstraints.fromConfig(
        tableConfig, columnNames, null, "myschema", "employees");
    assertNotNull(result);
    assertNotNull(result.getReferentialConstraints());
    assertEquals(1, result.getReferentialConstraints().size());
  }

  @Test void testFromConfigWithForeignKeyTargetSchema() {
    Map<String, Object> fk = new HashMap<String, Object>();
    fk.put("columns", Arrays.asList("dept_id"));
    fk.put("targetTable", "departments");
    fk.put("targetSchema", "hr");
    fk.put("targetColumns", Arrays.asList("id"));

    Map<String, Object> constraints = new HashMap<String, Object>();
    List<Map<String, Object>> fkList = new ArrayList<Map<String, Object>>();
    fkList.add(fk);
    constraints.put("foreignKeys", fkList);

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("id", "dept_id");

    Statistic result = TableConstraints.fromConfig(
        tableConfig, columnNames, null, "myschema", "employees");
    assertNotNull(result);
    assertNotNull(result.getReferentialConstraints());
    assertEquals(1, result.getReferentialConstraints().size());
  }

  @Test void testFromConfigWithInvalidForeignKey() {
    Map<String, Object> fk = new HashMap<String, Object>();
    // Missing required fields
    fk.put("columns", Arrays.asList("dept_id"));

    Map<String, Object> constraints = new HashMap<String, Object>();
    List<Map<String, Object>> fkList = new ArrayList<Map<String, Object>>();
    fkList.add(fk);
    constraints.put("foreignKeys", fkList);

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("id", "dept_id");

    Statistic result = TableConstraints.fromConfig(tableConfig, columnNames, null);
    assertNotNull(result);
    // Invalid FK should be skipped
    assertTrue(result.getReferentialConstraints() == null
        || result.getReferentialConstraints().isEmpty());
  }

  @Test void testFromConfigWithRowCount() {
    Map<String, Object> constraints = new HashMap<String, Object>();
    constraints.put("primaryKey", Arrays.asList("id"));

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("id", "name");

    Statistic result = TableConstraints.fromConfig(tableConfig, columnNames, 500.0);
    assertNotNull(result);
    assertEquals(500.0, result.getRowCount());
  }

  @Test void testPrimaryKeyHelper() {
    Map<String, Object> config = TableConstraints.primaryKey("id", "name");
    assertNotNull(config);
    assertTrue(config.containsKey("primaryKey"));
    @SuppressWarnings("unchecked")
    List<String> pk = (List<String>) config.get("primaryKey");
    assertEquals(2, pk.size());
    assertEquals("id", pk.get(0));
    assertEquals("name", pk.get(1));
  }

  @Test void testForeignKeyHelper() {
    Map<String, Object> config = TableConstraints.foreignKey(
        Arrays.asList("dept_id"),
        "hr", "departments",
        Arrays.asList("id"));

    assertNotNull(config);
    assertTrue(config.containsKey("columns"));
    assertTrue(config.containsKey("targetTable"));
    assertTrue(config.containsKey("targetColumns"));
  }

  @Test void testValidateForeignKeysAllValid() {
    // Create a statistic with FK constraints
    Map<String, Object> fk = new HashMap<String, Object>();
    fk.put("columns", Arrays.asList("dept_id"));
    fk.put("targetTable", "departments");
    fk.put("targetColumns", Arrays.asList("id"));

    Map<String, Object> constraints = new HashMap<String, Object>();
    List<Map<String, Object>> fkList = new ArrayList<Map<String, Object>>();
    fkList.add(fk);
    constraints.put("foreignKeys", fkList);

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("id", "dept_id");
    Statistic stat = TableConstraints.fromConfig(
        tableConfig, columnNames, null, "myschema", "employees");

    // All tables exist
    Statistic validated = TableConstraints.validateForeignKeys(
        stat, "employees", (schema, table) -> true);

    assertNotNull(validated);
    assertSame(stat, validated); // No changes needed
  }

  @Test void testValidateForeignKeysNullStatistic() {
    Statistic result = TableConstraints.validateForeignKeys(
        null, "test", (s, t) -> true);
    assertEquals(null, result);
  }

  @Test void testValidateForeignKeysNoConstraints() {
    Statistic stat = Statistics.UNKNOWN;
    Statistic result = TableConstraints.validateForeignKeys(
        stat, "test", (s, t) -> true);
    assertSame(stat, result);
  }

  @Test void testFromConfigWithEmptyColumnNames() {
    Map<String, Object> constraints = new HashMap<String, Object>();
    constraints.put("primaryKey", Arrays.asList("id"));

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    // Empty column names - should extract from constraints
    List<String> columnNames = Collections.emptyList();

    Statistic result = TableConstraints.fromConfig(tableConfig, columnNames, null);
    assertNotNull(result);
  }

  @Test void testFromConfigWithNullColumnNames() {
    Map<String, Object> constraints = new HashMap<String, Object>();
    constraints.put("primaryKey", Arrays.asList("id"));

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    Statistic result = TableConstraints.fromConfig(tableConfig, null, null);
    assertNotNull(result);
  }

  @Test void testPrimaryKeyColumnNotFound() {
    Map<String, Object> constraints = new HashMap<String, Object>();
    constraints.put("primaryKey", Arrays.asList("nonexistent_col"));

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("id", "name");

    Statistic result = TableConstraints.fromConfig(tableConfig, columnNames, null);
    assertNotNull(result);
    // Key should be empty since column not found
    assertTrue(result.getKeys() == null || result.getKeys().isEmpty());
  }
}
