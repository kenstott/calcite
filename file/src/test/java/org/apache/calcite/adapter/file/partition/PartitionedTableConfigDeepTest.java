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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for PartitionedTableConfig and all its inner classes.
 * Covers fromMap deserialization, all constructors, and edge cases.
 */
@Tag("unit")
public class PartitionedTableConfigDeepTest {

  // ===== Basic constructors =====

  @Test void testMinimalConstructor() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig("hive", null, null, null, null);
    PartitionedTableConfig config = new PartitionedTableConfig("test", "*.parquet", "partitioned", pc);
    assertEquals("test", config.getName());
    assertEquals("*.parquet", config.getPattern());
    assertEquals("partitioned", config.getType());
    assertNotNull(config.getPartitions());
    assertNull(config.getComment());
    assertNull(config.getColumnComments());
    assertNull(config.getColumns());
    assertNull(config.getAlternatePartitions());
    assertNull(config.getMaterialize());
  }

  @Test void testNullTypeDefaultsToPartitioned() {
    PartitionedTableConfig config =
        new PartitionedTableConfig("test", "*.parquet", null, null);
    assertEquals("partitioned", config.getType());
  }

  @Test void testFullConstructor() {
    Map<String, String> comments = new HashMap<>();
    comments.put("col1", "Comment 1");
    List<PartitionedTableConfig.TableColumn> columns = Collections.singletonList(
        new PartitionedTableConfig.TableColumn("col1", "VARCHAR", true, "Comment 1"));
    Map<String, Object> materialize = new HashMap<>();
    materialize.put("format", "parquet");

    PartitionedTableConfig config = new PartitionedTableConfig(
        "test", "*.parquet", "partitioned", null, "table comment",
        comments, columns, null, materialize);

    assertEquals("table comment", config.getComment());
    assertEquals("Comment 1", config.getColumnComments().get("col1"));
    assertEquals(1, config.getColumns().size());
    assertEquals("parquet", config.getMaterialize().get("format"));
  }

  // ===== PartitionConfig =====

  @Test void testPartitionConfigNullStyle() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig(null, null, null, null, null);
    assertEquals("auto", pc.getStyle());
  }

  @Test void testPartitionConfigAllFields() {
    List<String> columns = Arrays.asList("year", "month");
    List<PartitionedTableConfig.ColumnDefinition> defs = Arrays.asList(
        new PartitionedTableConfig.ColumnDefinition("year", "INTEGER"),
        new PartitionedTableConfig.ColumnDefinition("month", "INTEGER"));
    List<PartitionedTableConfig.ColumnMapping> mappings = Collections.singletonList(
        new PartitionedTableConfig.ColumnMapping("year", 1, "INTEGER"));

    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig("regex", columns, defs, ".*-(\\d{4})", mappings);

    assertEquals("regex", pc.getStyle());
    assertEquals(2, pc.getColumns().size());
    assertEquals(2, pc.getColumnDefinitions().size());
    assertEquals(".*-(\\d{4})", pc.getRegex());
    assertEquals(1, pc.getColumnMappings().size());
  }

  // ===== ColumnDefinition =====

  @Test void testColumnDefinitionSimple() {
    PartitionedTableConfig.ColumnDefinition cd =
        new PartitionedTableConfig.ColumnDefinition("year", "INTEGER");
    assertEquals("year", cd.getName());
    assertEquals("INTEGER", cd.getType());
    assertNull(cd.getSourceColumn());
    assertFalse(cd.hasSourceMapping());
  }

  @Test void testColumnDefinitionNullType() {
    PartitionedTableConfig.ColumnDefinition cd =
        new PartitionedTableConfig.ColumnDefinition("col", null);
    assertEquals("VARCHAR", cd.getType());
  }

  @Test void testColumnDefinitionWithSourceColumn() {
    PartitionedTableConfig.ColumnDefinition cd =
        new PartitionedTableConfig.ColumnDefinition("geo_fips", "VARCHAR", "GeoFips");
    assertEquals("geo_fips", cd.getName());
    assertEquals("GeoFips", cd.getSourceColumn());
    assertTrue(cd.hasSourceMapping());
  }

  @Test void testColumnDefinitionSourceColumnSameAsName() {
    PartitionedTableConfig.ColumnDefinition cd =
        new PartitionedTableConfig.ColumnDefinition("year", "INTEGER", "year");
    assertFalse(cd.hasSourceMapping()); // Same name, no mapping needed
  }

  @Test void testColumnDefinitionSourceColumnNull() {
    PartitionedTableConfig.ColumnDefinition cd =
        new PartitionedTableConfig.ColumnDefinition("year", "INTEGER", null);
    assertFalse(cd.hasSourceMapping());
  }

  // ===== ColumnMapping =====

  @Test void testColumnMapping() {
    PartitionedTableConfig.ColumnMapping cm =
        new PartitionedTableConfig.ColumnMapping("year", 1, "INTEGER");
    assertEquals("year", cm.getName());
    assertEquals(1, cm.getGroup());
    assertEquals("INTEGER", cm.getType());
  }

  // ===== TableColumn =====

  @Test void testTableColumnSimple() {
    PartitionedTableConfig.TableColumn tc =
        new PartitionedTableConfig.TableColumn("col1", "VARCHAR", true, "A column");
    assertEquals("col1", tc.getName());
    assertEquals("VARCHAR", tc.getType());
    assertTrue(tc.isNullable());
    assertEquals("A column", tc.getComment());
    assertNull(tc.getExpression());
    assertFalse(tc.hasExpression());
    assertFalse(tc.isComputed());
    assertNull(tc.getCsvColumn());
    assertFalse(tc.isVectorType());
  }

  @Test void testTableColumnWithExpression() {
    PartitionedTableConfig.TableColumn tc =
        new PartitionedTableConfig.TableColumn("year", "INTEGER", false, "Year", "EXTRACT(YEAR FROM date)");
    assertTrue(tc.hasExpression());
    assertTrue(tc.isComputed());
    assertEquals("EXTRACT(YEAR FROM date)", tc.getExpression());
  }

  @Test void testTableColumnWithEmptyExpression() {
    PartitionedTableConfig.TableColumn tc =
        new PartitionedTableConfig.TableColumn("col", "VARCHAR", true, null, "   ");
    assertFalse(tc.hasExpression()); // Trim should make it empty
    assertFalse(tc.isComputed());
  }

  @Test void testTableColumnWithCsvColumn() {
    PartitionedTableConfig.TableColumn tc =
        new PartitionedTableConfig.TableColumn("output_col", "VARCHAR", true, null, null, "csv_col");
    assertEquals("csv_col", tc.getCsvColumn());
  }

  @Test void testTableColumnVectorType() {
    PartitionedTableConfig.TableColumn tc =
        new PartitionedTableConfig.TableColumn("embedding", "array<float>", true, null);
    assertTrue(tc.isVectorType());
  }

  @Test void testTableColumnNonVectorType() {
    PartitionedTableConfig.TableColumn tc =
        new PartitionedTableConfig.TableColumn("name", "VARCHAR", true, null);
    assertFalse(tc.isVectorType());
  }

  @Test void testTableColumnNullType() {
    PartitionedTableConfig.TableColumn tc =
        new PartitionedTableConfig.TableColumn("col", null, true, null);
    assertFalse(tc.isVectorType());
  }

  // ===== AlternatePartitionConfig =====

  @Test void testAlternatePartitionConfigMinimal() {
    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig("alt1", "pattern/*", null, "comment");
    assertEquals("alt1", apc.getName());
    assertEquals("pattern/*", apc.getPattern());
    assertEquals("comment", apc.getComment());
    assertNull(apc.getPartition());
    assertNull(apc.getBatchPartitionColumns());
    assertNull(apc.getColumnMappings());
    assertEquals(0, apc.getThreads());
    assertNull(apc.getIncrementalKeys());
    assertEquals(0, apc.getCurrentYearTtlDays());
    assertTrue(apc.isEnabled());
    assertFalse(apc.supportsIncremental());
    assertEquals(0, apc.getPartitionKeyCount());
  }

  @Test void testAlternatePartitionConfigFull() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig("hive", null,
            Arrays.asList(new PartitionedTableConfig.ColumnDefinition("year", "INTEGER")),
            null, null);
    List<String> batchCols = Arrays.asList("year", "geo_fips");
    Map<String, String> colMappings = new HashMap<>();
    colMappings.put("geo_fips", "GeoFips");
    List<String> incKeys = Collections.singletonList("year");

    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt1", "pattern/*", pc, "comment",
            batchCols, colMappings, 4, incKeys, 30, true);

    assertEquals(4, apc.getThreads());
    assertEquals(2, apc.getBatchPartitionColumns().size());
    assertEquals("GeoFips", apc.getColumnMappings().get("geo_fips"));
    assertTrue(apc.supportsIncremental());
    assertEquals(1, apc.getIncrementalKeys().size());
    assertEquals(30, apc.getCurrentYearTtlDays());
    assertTrue(apc.isEnabled());
    assertEquals(1, apc.getPartitionKeyCount());
  }

  @Test void testAlternatePartitionConfigDisabled() {
    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt1", "p/*", null, null, null, null, 0, null, 0, false);
    assertFalse(apc.isEnabled());
  }

  @Test void testAlternatePartitionConfigEmptyIncrementalKeys() {
    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt1", "p/*", null, null, null, null, 0, Collections.<String>emptyList());
    assertFalse(apc.supportsIncremental()); // Empty list = no incremental
  }

  @Test void testAlternatePartitionConfigNullPartitionDefinitions() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig("hive", null, null, null, null);
    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig("alt1", "p/*", pc, null);
    assertEquals(0, apc.getPartitionKeyCount());
  }

  // ===== fromMap =====

  @Test void testFromMapMinimal() {
    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertEquals("test_table", config.getName());
    assertEquals("data/*.parquet", config.getPattern());
    assertEquals("partitioned", config.getType());
  }

  @Test void testFromMapWithPartitions() {
    Map<String, Object> partitions = new HashMap<>();
    partitions.put("style", "hive");
    List<String> colDefs = Arrays.asList("year", "month");
    partitions.put("columnDefinitions", colDefs);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("partitions", partitions);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNotNull(config.getPartitions());
    assertEquals("hive", config.getPartitions().getStyle());
    assertEquals(2, config.getPartitions().getColumns().size());
    assertNotNull(config.getPartitions().getColumnDefinitions());
    assertEquals(2, config.getPartitions().getColumnDefinitions().size());
    // Simple strings -> default VARCHAR type
    assertEquals("VARCHAR", config.getPartitions().getColumnDefinitions().get(0).getType());
  }

  @Test void testFromMapWithFullColumnDefinitions() {
    List<Map<String, Object>> colDefs = new ArrayList<>();
    Map<String, Object> cd1 = new HashMap<>();
    cd1.put("name", "year");
    cd1.put("type", "INTEGER");
    colDefs.add(cd1);
    Map<String, Object> cd2 = new HashMap<>();
    cd2.put("name", "geo_fips");
    cd2.put("type", "VARCHAR");
    cd2.put("sourceColumn", "GeoFips");
    colDefs.add(cd2);

    Map<String, Object> partitions = new HashMap<>();
    partitions.put("style", "hive");
    partitions.put("columnDefinitions", colDefs);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("partitions", partitions);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNotNull(config.getPartitions());
    assertEquals(2, config.getPartitions().getColumnDefinitions().size());
    assertEquals("INTEGER", config.getPartitions().getColumnDefinitions().get(0).getType());
    assertEquals("GeoFips", config.getPartitions().getColumnDefinitions().get(1).getSourceColumn());
  }

  @Test void testFromMapWithRegexAndColumnMappings() {
    List<Map<String, Object>> mappings = new ArrayList<>();
    Map<String, Object> m1 = new HashMap<>();
    m1.put("name", "year");
    m1.put("group", 1);
    m1.put("type", "INTEGER");
    mappings.add(m1);

    Map<String, Object> partitions = new HashMap<>();
    partitions.put("style", "regex");
    partitions.put("regex", "data-(\\d{4})\\.csv");
    partitions.put("columnMappings", mappings);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data-*.csv");
    map.put("partitions", partitions);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNotNull(config.getPartitions());
    assertEquals("regex", config.getPartitions().getStyle());
    assertEquals("data-(\\d{4})\\.csv", config.getPartitions().getRegex());
    assertEquals(1, config.getPartitions().getColumnMappings().size());
    assertEquals("year", config.getPartitions().getColumnMappings().get(0).getName());
    assertEquals(1, config.getPartitions().getColumnMappings().get(0).getGroup());
  }

  @Test void testFromMapWithColumnComments() {
    List<Map<String, Object>> comments = new ArrayList<>();
    Map<String, Object> c1 = new HashMap<>();
    c1.put("name", "col1");
    c1.put("comment", "First column");
    comments.add(c1);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("column_comments", comments);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNotNull(config.getColumnComments());
    assertEquals("First column", config.getColumnComments().get("col1"));
  }

  @Test void testFromMapWithColumnsExtractsComments() {
    List<Map<String, Object>> columns = new ArrayList<>();
    Map<String, Object> col1 = new HashMap<>();
    col1.put("name", "col1");
    col1.put("type", "VARCHAR");
    col1.put("nullable", true);
    col1.put("comment", "A column");
    columns.add(col1);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("columns", columns);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNotNull(config.getColumns());
    assertEquals(1, config.getColumns().size());
    assertEquals("VARCHAR", config.getColumns().get(0).getType());
    assertTrue(config.getColumns().get(0).isNullable());
    // Comments should be extracted from columns
    assertNotNull(config.getColumnComments());
    assertEquals("A column", config.getColumnComments().get("col1"));
  }

  @Test void testFromMapWithBulkGenerator() {
    List<Map<String, Object>> columns = new ArrayList<>();
    Map<String, Object> col1 = new HashMap<>();
    col1.put("name", "embedding");
    col1.put("type", "array<float>");
    col1.put("bulkGenerator", "quackformers");
    columns.add(col1);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("columns", columns);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNotNull(config.getColumns());
    // bulkGenerator should set expression to __BULK_GENERATED__
    assertTrue(config.getColumns().get(0).hasExpression());
    assertEquals("__BULK_GENERATED__", config.getColumns().get(0).getExpression());
  }

  @Test void testFromMapWithAlternatePartitions() {
    List<Map<String, Object>> alts = new ArrayList<>();
    Map<String, Object> alt1 = new HashMap<>();
    alt1.put("name", "alt_by_year");
    alt1.put("pattern", "alt/year=*/data.parquet");
    alt1.put("comment", "Year partition");
    alt1.put("threads", 4);
    alt1.put("enabled", false);
    alt1.put("current_year_ttl_days", 7);

    List<String> batchCols = Arrays.asList("year", "region");
    alt1.put("batch_partition_columns", batchCols);

    Map<String, String> colMappings = new LinkedHashMap<>();
    colMappings.put("target_col", "source_col");
    alt1.put("column_mappings", colMappings);

    List<String> incKeys = Collections.singletonList("year");
    alt1.put("incremental_keys", incKeys);

    List<Map<String, Object>> colDefs = new ArrayList<>();
    Map<String, Object> cd = new HashMap<>();
    cd.put("name", "year");
    cd.put("type", "INTEGER");
    colDefs.add(cd);
    Map<String, Object> partitionMap = new HashMap<>();
    partitionMap.put("style", "hive");
    partitionMap.put("columnDefinitions", colDefs);
    alt1.put("partition", partitionMap);

    alts.add(alt1);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("alternate_partitions", alts);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNotNull(config.getAlternatePartitions());
    assertEquals(1, config.getAlternatePartitions().size());

    PartitionedTableConfig.AlternatePartitionConfig alt = config.getAlternatePartitions().get(0);
    assertEquals("alt_by_year", alt.getName());
    assertEquals("alt/year=*/data.parquet", alt.getPattern());
    assertEquals("Year partition", alt.getComment());
    assertEquals(4, alt.getThreads());
    assertFalse(alt.isEnabled());
    assertEquals(7, alt.getCurrentYearTtlDays());
    assertEquals(2, alt.getBatchPartitionColumns().size());
    assertNotNull(alt.getColumnMappings());
    assertEquals("source_col", alt.getColumnMappings().get("target_col"));
    assertTrue(alt.supportsIncremental());
    assertNotNull(alt.getPartition());
    assertEquals("hive", alt.getPartition().getStyle());
  }

  @Test void testFromMapAlternatePartitionsNullNameSkipped() {
    List<Map<String, Object>> alts = new ArrayList<>();
    Map<String, Object> alt1 = new HashMap<>();
    alt1.put("pattern", "p/*");
    // Missing name -> should be skipped
    alts.add(alt1);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("alternate_partitions", alts);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNull(config.getAlternatePartitions()); // Empty list -> null
  }

  @Test void testFromMapAlternatePartitionsNotAList() {
    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("alternate_partitions", "not a list");

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNull(config.getAlternatePartitions());
  }

  @Test void testFromMapWithMaterialize() {
    Map<String, Object> materialize = new HashMap<>();
    materialize.put("format", "iceberg");
    materialize.put("enabled", true);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("materialize", materialize);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNotNull(config.getMaterialize());
    assertEquals("iceberg", config.getMaterialize().get("format"));
  }

  @Test void testFromMapEmptyColumnComments() {
    List<Map<String, Object>> comments = new ArrayList<>();
    // Empty list -> null

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("column_comments", comments);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    // No column_comments and no columns -> no comments
    assertNull(config.getColumnComments());
  }

  @Test void testFromMapColumnsWithNullName() {
    List<Map<String, Object>> columns = new ArrayList<>();
    Map<String, Object> col1 = new HashMap<>();
    col1.put("type", "VARCHAR"); // Missing name -> skipped
    columns.add(col1);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("columns", columns);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNull(config.getColumns()); // Empty list after filtering -> null
  }

  @Test void testFromMapAlternateWithSourceColumnMappings() {
    // Test sourceColumn in alternate partition columnDefinitions
    List<Map<String, Object>> colDefs = new ArrayList<>();
    Map<String, Object> cd = new HashMap<>();
    cd.put("name", "geo_fips");
    cd.put("type", "VARCHAR");
    cd.put("sourceColumn", "GeoFips");
    colDefs.add(cd);

    Map<String, Object> partitionMap = new HashMap<>();
    partitionMap.put("style", "hive");
    partitionMap.put("columnDefinitions", colDefs);

    List<Map<String, Object>> alts = new ArrayList<>();
    Map<String, Object> alt1 = new HashMap<>();
    alt1.put("name", "alt1");
    alt1.put("pattern", "alt/*");
    alt1.put("partition", partitionMap);
    alts.add(alt1);

    Map<String, Object> map = new HashMap<>();
    map.put("name", "test_table");
    map.put("pattern", "data/*.parquet");
    map.put("alternate_partitions", alts);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNotNull(config.getAlternatePartitions());
    PartitionedTableConfig.AlternatePartitionConfig alt = config.getAlternatePartitions().get(0);
    // columnMappings should be derived from sourceColumn
    assertNotNull(alt.getColumnMappings());
    assertEquals("GeoFips", alt.getColumnMappings().get("geo_fips"));
  }
}
