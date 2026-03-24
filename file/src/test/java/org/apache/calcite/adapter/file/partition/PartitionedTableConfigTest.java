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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PartitionedTableConfig} and its inner classes.
 */
@Tag("unit")
public class PartitionedTableConfigTest {

  // ===== Basic constructor and getters =====

  @Test void testMinimalConstructor() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig("hive", null, null, null, null);

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "year=*/*.parquet", "partitioned", pc);

    assertEquals("test_table", config.getName());
    assertEquals("year=*/*.parquet", config.getPattern());
    assertEquals("partitioned", config.getType());
    assertNotNull(config.getPartitions());
    assertNull(config.getComment());
    assertNull(config.getColumnComments());
    assertNull(config.getColumns());
    assertNull(config.getAlternatePartitions());
    assertNull(config.getMaterialize());
  }

  @Test void testFullConstructor() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig("hive", null, null, null, null);

    Map<String, String> colComments = new LinkedHashMap<String, String>();
    colComments.put("year", "The year");

    List<PartitionedTableConfig.TableColumn> columns = Collections.singletonList(
        new PartitionedTableConfig.TableColumn("year", "INTEGER", false, "The year"));

    Map<String, Object> materialize = new LinkedHashMap<String, Object>();
    materialize.put("format", "parquet");

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "year=*/*.parquet", null, pc,
            "A test table", colComments, columns, null, materialize);

    assertEquals("test_table", config.getName());
    assertEquals("year=*/*.parquet", config.getPattern());
    assertEquals("partitioned", config.getType()); // defaults to "partitioned" when null
    assertEquals("A test table", config.getComment());
    assertNotNull(config.getColumnComments());
    assertEquals("The year", config.getColumnComments().get("year"));
    assertNotNull(config.getColumns());
    assertEquals(1, config.getColumns().size());
    assertNotNull(config.getMaterialize());
    assertEquals("parquet", config.getMaterialize().get("format"));
  }

  @Test void testNullTypeDefaultsToPartitioned() {
    PartitionedTableConfig config =
        new PartitionedTableConfig("t", "p", null, null);

    assertEquals("partitioned", config.getType());
  }

  // ===== PartitionConfig =====

  @Test void testPartitionConfigGetters() {
    List<String> columns = Arrays.asList("year", "geo");
    List<PartitionedTableConfig.ColumnDefinition> colDefs = Arrays.asList(
        new PartitionedTableConfig.ColumnDefinition("year", "INTEGER"),
        new PartitionedTableConfig.ColumnDefinition("geo", "VARCHAR")
    );
    List<PartitionedTableConfig.ColumnMapping> mappings = Collections.singletonList(
        new PartitionedTableConfig.ColumnMapping("year", 1, "INTEGER")
    );

    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig(
            "hive", columns, colDefs, "(\\d{4})", mappings);

    assertEquals("hive", pc.getStyle());
    assertEquals(columns, pc.getColumns());
    assertEquals(colDefs, pc.getColumnDefinitions());
    assertEquals("(\\d{4})", pc.getRegex());
    assertEquals(mappings, pc.getColumnMappings());
  }

  @Test void testPartitionConfigNullStyleDefaultsToAuto() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig(null, null, null, null, null);

    assertEquals("auto", pc.getStyle());
  }

  // ===== ColumnDefinition =====

  @Test void testColumnDefinitionBasic() {
    PartitionedTableConfig.ColumnDefinition cd =
        new PartitionedTableConfig.ColumnDefinition("year", "INTEGER");

    assertEquals("year", cd.getName());
    assertEquals("INTEGER", cd.getType());
    assertNull(cd.getSourceColumn());
    assertFalse(cd.hasSourceMapping());
  }

  @Test void testColumnDefinitionWithSourceColumn() {
    PartitionedTableConfig.ColumnDefinition cd =
        new PartitionedTableConfig.ColumnDefinition("geo_fips", "VARCHAR", "GeoFips");

    assertEquals("geo_fips", cd.getName());
    assertEquals("VARCHAR", cd.getType());
    assertEquals("GeoFips", cd.getSourceColumn());
    assertTrue(cd.hasSourceMapping());
  }

  @Test void testColumnDefinitionNullTypeDefaultsToVarchar() {
    PartitionedTableConfig.ColumnDefinition cd =
        new PartitionedTableConfig.ColumnDefinition("name", null);

    assertEquals("VARCHAR", cd.getType());
  }

  @Test void testColumnDefinitionSourceColumnSameAsName() {
    // sourceColumn == name should mean no mapping
    PartitionedTableConfig.ColumnDefinition cd =
        new PartitionedTableConfig.ColumnDefinition("geo", "VARCHAR", "geo");

    assertEquals("geo", cd.getSourceColumn());
    assertFalse(cd.hasSourceMapping());
  }

  // ===== ColumnMapping =====

  @Test void testColumnMappingGetters() {
    PartitionedTableConfig.ColumnMapping cm =
        new PartitionedTableConfig.ColumnMapping("year", 1, "INTEGER");

    assertEquals("year", cm.getName());
    assertEquals(1, cm.getGroup());
    assertEquals("INTEGER", cm.getType());
  }

  // ===== AlternatePartitionConfig =====

  @Test void testAlternatePartitionConfigMinimal() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig("hive", null, null, null, null);

    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt_by_geo", "type=foo/geo=*/*.parquet", pc, "By geography");

    assertEquals("alt_by_geo", apc.getName());
    assertEquals("type=foo/geo=*/*.parquet", apc.getPattern());
    assertNotNull(apc.getPartition());
    assertEquals("By geography", apc.getComment());
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
    List<PartitionedTableConfig.ColumnDefinition> colDefs = Arrays.asList(
        new PartitionedTableConfig.ColumnDefinition("geo", "VARCHAR"),
        new PartitionedTableConfig.ColumnDefinition("year", "INTEGER")
    );
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig("hive", null, colDefs, null, null);

    List<String> batchCols = Arrays.asList("year", "geo_fips_set");
    Map<String, String> colMappings = new LinkedHashMap<String, String>();
    colMappings.put("geo", "GeoFips");
    List<String> incKeys = Collections.singletonList("year");

    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt", "pattern", pc, "comment",
            batchCols, colMappings, 4, incKeys, 7, false);

    assertEquals("alt", apc.getName());
    assertEquals(batchCols, apc.getBatchPartitionColumns());
    assertEquals(colMappings, apc.getColumnMappings());
    assertEquals(4, apc.getThreads());
    assertEquals(incKeys, apc.getIncrementalKeys());
    assertEquals(7, apc.getCurrentYearTtlDays());
    assertFalse(apc.isEnabled());
    assertTrue(apc.supportsIncremental());
    assertEquals(2, apc.getPartitionKeyCount());
  }

  @Test void testAlternatePartitionSupportsIncrementalEmpty() {
    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt", "p", null, null,
            null, null, 0, new ArrayList<String>(), 0, true);

    assertFalse(apc.supportsIncremental());
  }

  @Test void testAlternatePartitionKeyCountNullConfig() {
    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt", "p", null, null);

    assertEquals(0, apc.getPartitionKeyCount());
  }

  @Test void testAlternatePartitionKeyCountNullColumnDefs() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig("hive", null, null, null, null);

    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt", "p", pc, null);

    assertEquals(0, apc.getPartitionKeyCount());
  }

  // ===== TableColumn (csvColumn) =====

  @Test void testTableColumnWithCsvColumn() {
    PartitionedTableConfig.TableColumn tc =
        new PartitionedTableConfig.TableColumn(
            "geo_fips", "VARCHAR", false, "FIPS code", null, "GeoFips");

    assertEquals("geo_fips", tc.getName());
    assertEquals("GeoFips", tc.getCsvColumn());
    assertFalse(tc.hasExpression());
  }

  @Test void testTableColumnCsvColumnNull() {
    PartitionedTableConfig.TableColumn tc =
        new PartitionedTableConfig.TableColumn("name", "VARCHAR", true, "comment");

    assertNull(tc.getCsvColumn());
  }

  // ===== fromMap =====

  @Test void testFromMapMinimal() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test_table");
    map.put("pattern", "year=*/*.parquet");

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    assertEquals("test_table", config.getName());
    assertEquals("year=*/*.parquet", config.getPattern());
    assertEquals("partitioned", config.getType());
    assertNull(config.getPartitions());
  }

  @Test void testFromMapWithPartitionsHiveStyle() {
    Map<String, Object> partitionsMap = new LinkedHashMap<String, Object>();
    partitionsMap.put("style", "hive");

    List<String> colDefs = Arrays.asList("year", "geo");
    partitionsMap.put("columnDefinitions", colDefs);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test_table");
    map.put("pattern", "year=*/geo=*/*.parquet");
    map.put("partitions", partitionsMap);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    assertNotNull(config.getPartitions());
    assertEquals("hive", config.getPartitions().getStyle());
    assertNotNull(config.getPartitions().getColumnDefinitions());
    assertEquals(2, config.getPartitions().getColumnDefinitions().size());
    assertEquals("year", config.getPartitions().getColumnDefinitions().get(0).getName());
    assertEquals("VARCHAR", config.getPartitions().getColumnDefinitions().get(0).getType());
  }

  @SuppressWarnings("unchecked")
  @Test void testFromMapWithColumnDefinitionsMaps() {
    List<Map<String, Object>> colDefs = new ArrayList<Map<String, Object>>();
    Map<String, Object> colDef1 = new LinkedHashMap<String, Object>();
    colDef1.put("name", "geo_fips");
    colDef1.put("type", "VARCHAR");
    colDef1.put("sourceColumn", "GeoFips");
    colDefs.add(colDef1);

    Map<String, Object> colDef2 = new LinkedHashMap<String, Object>();
    colDef2.put("name", "year");
    colDef2.put("type", "INTEGER");
    colDefs.add(colDef2);

    Map<String, Object> partitionsMap = new LinkedHashMap<String, Object>();
    partitionsMap.put("style", "directory");
    partitionsMap.put("columnDefinitions", colDefs);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test_table");
    map.put("pattern", "*/*/*.parquet");
    map.put("partitions", partitionsMap);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    assertNotNull(config.getPartitions());
    assertEquals(2, config.getPartitions().getColumnDefinitions().size());
    assertEquals("geo_fips", config.getPartitions().getColumnDefinitions().get(0).getName());
    assertEquals("VARCHAR", config.getPartitions().getColumnDefinitions().get(0).getType());
    assertTrue(config.getPartitions().getColumnDefinitions().get(0).hasSourceMapping());
    assertEquals("GeoFips", config.getPartitions().getColumnDefinitions().get(0).getSourceColumn());
    assertNotNull(config.getPartitions().getColumns());
    assertEquals(Arrays.asList("geo_fips", "year"), config.getPartitions().getColumns());
  }

  @Test void testFromMapWithRegexAndColumnMappings() {
    List<Map<String, Object>> mappings = new ArrayList<Map<String, Object>>();
    Map<String, Object> mapping1 = new LinkedHashMap<String, Object>();
    mapping1.put("name", "year");
    mapping1.put("group", 1);
    mapping1.put("type", "INTEGER");
    mappings.add(mapping1);

    Map<String, Object> partitionsMap = new LinkedHashMap<String, Object>();
    partitionsMap.put("style", "regex");
    partitionsMap.put("regex", "(\\d{4})-.*\\.parquet");
    partitionsMap.put("columnMappings", mappings);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test_table");
    map.put("pattern", "*.parquet");
    map.put("partitions", partitionsMap);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    assertNotNull(config.getPartitions());
    assertEquals("(\\d{4})-.*\\.parquet", config.getPartitions().getRegex());
    assertNotNull(config.getPartitions().getColumnMappings());
    assertEquals(1, config.getPartitions().getColumnMappings().size());
    assertEquals("year", config.getPartitions().getColumnMappings().get(0).getName());
    assertEquals(1, config.getPartitions().getColumnMappings().get(0).getGroup());
  }

  @Test void testFromMapWithColumnComments() {
    List<Map<String, Object>> colComments = new ArrayList<Map<String, Object>>();
    Map<String, Object> cc1 = new LinkedHashMap<String, Object>();
    cc1.put("name", "year");
    cc1.put("comment", "The year");
    colComments.add(cc1);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test_table");
    map.put("pattern", "*.parquet");
    map.put("column_comments", colComments);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    assertNotNull(config.getColumnComments());
    assertEquals("The year", config.getColumnComments().get("year"));
  }

  @Test void testFromMapWithColumns() {
    List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
    Map<String, Object> col1 = new LinkedHashMap<String, Object>();
    col1.put("name", "year");
    col1.put("type", "INTEGER");
    col1.put("nullable", false);
    col1.put("comment", "The year");
    col1.put("expression", "EXTRACT(YEAR FROM date)");
    columns.add(col1);

    Map<String, Object> col2 = new LinkedHashMap<String, Object>();
    col2.put("name", "fips");
    col2.put("type", "VARCHAR");
    col2.put("csvColumn", "GeoFips");
    columns.add(col2);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test_table");
    map.put("pattern", "*.parquet");
    map.put("columns", columns);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    assertNotNull(config.getColumns());
    assertEquals(2, config.getColumns().size());

    PartitionedTableConfig.TableColumn tc1 = config.getColumns().get(0);
    assertEquals("year", tc1.getName());
    assertEquals("INTEGER", tc1.getType());
    assertFalse(tc1.isNullable());
    assertEquals("The year", tc1.getComment());
    assertEquals("EXTRACT(YEAR FROM date)", tc1.getExpression());
    assertTrue(tc1.hasExpression());

    PartitionedTableConfig.TableColumn tc2 = config.getColumns().get(1);
    assertEquals("fips", tc2.getName());
    assertEquals("GeoFips", tc2.getCsvColumn());
  }

  @Test void testFromMapExtractsColumnCommentsFromColumns() {
    List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
    Map<String, Object> col1 = new LinkedHashMap<String, Object>();
    col1.put("name", "year");
    col1.put("type", "INTEGER");
    col1.put("comment", "The year");
    columns.add(col1);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test_table");
    map.put("pattern", "*.parquet");
    map.put("columns", columns);
    // No column_comments provided - should extract from columns

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    assertNotNull(config.getColumnComments());
    assertEquals("The year", config.getColumnComments().get("year"));
  }

  @Test void testFromMapWithAlternatePartitions() {
    List<Map<String, Object>> colDefs = new ArrayList<Map<String, Object>>();
    Map<String, Object> colDef = new LinkedHashMap<String, Object>();
    colDef.put("name", "geo");
    colDef.put("type", "VARCHAR");
    colDef.put("sourceColumn", "GeoFips");
    colDefs.add(colDef);

    Map<String, Object> altPartition = new LinkedHashMap<String, Object>();
    altPartition.put("style", "hive");
    altPartition.put("columnDefinitions", colDefs);

    Map<String, Object> alt = new LinkedHashMap<String, Object>();
    alt.put("name", "by_geo");
    alt.put("pattern", "type=foo/geo=*/*.parquet");
    alt.put("comment", "By geography");
    alt.put("partition", altPartition);
    alt.put("threads", 4);
    alt.put("current_year_ttl_days", 7);
    alt.put("enabled", false);

    List<String> batchCols = Arrays.asList("year", "geo");
    alt.put("batch_partition_columns", batchCols);

    List<String> incKeys = Collections.singletonList("year");
    alt.put("incremental_keys", incKeys);

    Map<String, String> colMappings = new LinkedHashMap<String, String>();
    colMappings.put("region", "Region");
    alt.put("column_mappings", colMappings);

    List<Map<String, Object>> alternates = Collections.singletonList(alt);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test_table");
    map.put("pattern", "year=*/*.parquet");
    map.put("alternate_partitions", alternates);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    assertNotNull(config.getAlternatePartitions());
    assertEquals(1, config.getAlternatePartitions().size());

    PartitionedTableConfig.AlternatePartitionConfig apc =
        config.getAlternatePartitions().get(0);
    assertEquals("by_geo", apc.getName());
    assertEquals("type=foo/geo=*/*.parquet", apc.getPattern());
    assertEquals("By geography", apc.getComment());
    assertEquals(4, apc.getThreads());
    assertEquals(7, apc.getCurrentYearTtlDays());
    assertFalse(apc.isEnabled());
    assertEquals(batchCols, apc.getBatchPartitionColumns());
    assertEquals(incKeys, apc.getIncrementalKeys());
    assertTrue(apc.supportsIncremental());

    // Column mappings should include both explicit and sourceColumn-derived
    assertNotNull(apc.getColumnMappings());
    assertEquals("Region", apc.getColumnMappings().get("region"));
    assertEquals("GeoFips", apc.getColumnMappings().get("geo"));

    assertNotNull(apc.getPartition());
    assertEquals(1, apc.getPartitionKeyCount());
  }

  @Test void testFromMapAlternatePartitionsSkipsMissingNameOrPattern() {
    Map<String, Object> alt1 = new LinkedHashMap<String, Object>();
    alt1.put("name", null);  // missing name
    alt1.put("pattern", "p1");

    Map<String, Object> alt2 = new LinkedHashMap<String, Object>();
    alt2.put("name", "alt2");
    alt2.put("pattern", null);  // missing pattern

    Map<String, Object> alt3 = new LinkedHashMap<String, Object>();
    alt3.put("name", "alt3");
    alt3.put("pattern", "p3");

    List<Map<String, Object>> alternates = Arrays.asList(alt1, alt2, alt3);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");
    map.put("pattern", "*.parquet");
    map.put("alternate_partitions", alternates);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    // Only alt3 should be included
    assertNotNull(config.getAlternatePartitions());
    assertEquals(1, config.getAlternatePartitions().size());
    assertEquals("alt3", config.getAlternatePartitions().get(0).getName());
  }

  @Test void testFromMapAlternatePartitionsNonList() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");
    map.put("pattern", "*.parquet");
    map.put("alternate_partitions", "not a list");

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);
    assertNull(config.getAlternatePartitions());
  }

  @Test void testFromMapWithBulkGenerator() {
    List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
    Map<String, Object> col = new LinkedHashMap<String, Object>();
    col.put("name", "embedding");
    col.put("type", "array<float>");
    col.put("bulkGenerator", "gpu_embed");
    columns.add(col);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");
    map.put("pattern", "*.parquet");
    map.put("columns", columns);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    assertNotNull(config.getColumns());
    assertEquals(1, config.getColumns().size());
    // bulkGenerator sets expression to placeholder
    assertEquals("__BULK_GENERATED__", config.getColumns().get(0).getExpression());
    assertTrue(config.getColumns().get(0).isComputed());
  }

  @Test void testFromMapWithMaterialize() {
    Map<String, Object> materialize = new LinkedHashMap<String, Object>();
    materialize.put("format", "parquet");
    materialize.put("rowGroupSize", 1000000);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");
    map.put("pattern", "*.parquet");
    map.put("materialize", materialize);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    assertNotNull(config.getMaterialize());
    assertEquals("parquet", config.getMaterialize().get("format"));
  }

  @Test void testFromMapColumnsWithNullName() {
    List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
    Map<String, Object> col = new LinkedHashMap<String, Object>();
    col.put("name", null);
    col.put("type", "VARCHAR");
    columns.add(col);

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");
    map.put("pattern", "*.parquet");
    map.put("columns", columns);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    // Column with null name should be skipped
    assertNull(config.getColumns());
  }

  @Test void testFromMapColumnCommentsEmptyList() {
    List<Map<String, Object>> colComments = new ArrayList<Map<String, Object>>();

    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");
    map.put("pattern", "*.parquet");
    map.put("column_comments", colComments);

    PartitionedTableConfig config = PartitionedTableConfig.fromMap(map);

    // Empty list returns null
    assertNull(config.getColumnComments());
  }
}
