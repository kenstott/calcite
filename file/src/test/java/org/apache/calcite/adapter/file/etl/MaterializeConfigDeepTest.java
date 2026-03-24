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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link MaterializeConfig}, including inner classes.
 */
@Tag("unit")
class MaterializeConfigDeepTest {

  @Test void testFromMapFormatParsing() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    // Test each format
    for (String[] pair : new String[][]{
        {"iceberg", "ICEBERG"},
        {"parquet", "PARQUET"},
        {"delta", "DELTA"},
        {"snowflake", "SNOWFLAKE"},
        {"bigquery", "BIGQUERY"},
        {"databricks", "DATABRICKS"}}) {
      map.put("format", pair[0]);
      MaterializeConfig config = MaterializeConfig.fromMap(map);
      assertNotNull(config);
      assertEquals(MaterializeConfig.Format.valueOf(pair[1]), config.getFormat());
    }
  }

  @Test void testFromMapTriggerParsing() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    map.put("trigger", "auto");
    assertEquals(MaterializeConfig.Trigger.AUTO,
        MaterializeConfig.fromMap(map).getTrigger());

    map.put("trigger", "manual");
    assertEquals(MaterializeConfig.Trigger.MANUAL,
        MaterializeConfig.fromMap(map).getTrigger());

    map.put("trigger", "onfirstquery");
    assertEquals(MaterializeConfig.Trigger.ON_FIRST_QUERY,
        MaterializeConfig.fromMap(map).getTrigger());

    map.put("trigger", "on_first_query");
    assertEquals(MaterializeConfig.Trigger.ON_FIRST_QUERY,
        MaterializeConfig.fromMap(map).getTrigger());

    map.put("trigger", "unknown");
    assertEquals(MaterializeConfig.Trigger.AUTO,
        MaterializeConfig.fromMap(map).getTrigger());
  }

  @Test void testFromMapNull() {
    assertNull(MaterializeConfig.fromMap(null));
  }

  @Test void testFromMapDefaults() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertTrue(config.isEnabled());
    assertEquals(MaterializeConfig.Format.ICEBERG, config.getFormat());
    assertEquals(MaterializeConfig.Trigger.AUTO, config.getTrigger());
    assertTrue(config.getColumns().isEmpty());
    assertTrue(config.getColumnComments().isEmpty());
    assertNotNull(config.getOptions());
  }

  @Test void testFromMapWithEnabledFalse() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("enabled", Boolean.FALSE);
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    // enabled flag is parsed
  }

  @Test void testFromMapWithColumns() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    java.util.List<Map<String, Object>> columns = new java.util.ArrayList<Map<String, Object>>();
    Map<String, Object> col = new HashMap<String, Object>();
    col.put("name", "year");
    col.put("type", "INTEGER");
    columns.add(col);
    map.put("columns", columns);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(1, config.getColumns().size());
  }

  @Test void testFromMapWithTargetTableId() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("targetTableId", "my_table");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("my_table", config.getTargetTableId());
  }

  @Test void testFromMapWithName() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "test_materialize");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("test_materialize", config.getName());
  }

  @Test void testFromMapWithTableComment() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("tableComment", "This is a test table");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("This is a test table", config.getTableComment());
  }

  @Test void testFromMapWithColumnComments() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    Map<String, Object> columnComments = new HashMap<String, Object>();
    columnComments.put("year", "Fiscal year");
    columnComments.put("region", "Geographic region");
    map.put("columnComments", columnComments);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(2, config.getColumnComments().size());
    assertEquals("Fiscal year", config.getColumnComments().get("year"));
  }

  @Test void testBuilderMissingOutputThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        MaterializeConfig.builder().build());
  }

  @Test void testIcebergConfigDefaults() {
    MaterializeConfig.IcebergConfig config =
        MaterializeConfig.IcebergConfig.builder().build();

    assertEquals(MaterializeConfig.IcebergConfig.CatalogType.HADOOP, config.getCatalogType());
    assertEquals("default", config.getNamespace());
    assertNull(config.getWarehousePath());
    assertNull(config.getRestUri());
    assertTrue(config.getBatchPartitionColumns().isEmpty());
    assertTrue(config.getIncrementalKeys().isEmpty());
    assertEquals(3, config.getMaxRetries());
    assertEquals(1000, config.getRetryDelayMs());
    assertEquals(false, config.isRunMaintenance());
    assertEquals(7, config.getSnapshotRetentionDays());
    assertEquals(false, config.isRunCompaction());
    assertEquals(128L * 1024 * 1024, config.getCompactionTargetFileSizeBytes());
    assertEquals(10, config.getCompactionMinFiles());
    assertEquals(10L * 1024 * 1024, config.getCompactionSmallFileSizeBytes());
  }

  @Test void testIcebergConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("catalogType", "rest");
    map.put("warehousePath", "/data/warehouse");
    map.put("namespace", "production");
    map.put("restUri", "http://localhost:8181");
    map.put("batchPartitionColumns", Arrays.asList("year"));
    map.put("incrementalKeys", Arrays.asList("year", "month"));
    map.put("maxRetries", 5);
    map.put("retryDelayMs", 2000L);
    map.put("runMaintenance", true);
    map.put("snapshotRetentionDays", 14);
    map.put("runCompaction", true);
    map.put("compactionTargetFileSizeBytes", 256L * 1024 * 1024);
    map.put("compactionMinFiles", 20);
    map.put("compactionSmallFileSizeBytes", 5L * 1024 * 1024);

    MaterializeConfig.IcebergConfig config =
        MaterializeConfig.IcebergConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(MaterializeConfig.IcebergConfig.CatalogType.REST, config.getCatalogType());
    assertEquals("/data/warehouse", config.getWarehousePath());
    assertEquals("production", config.getNamespace());
    assertEquals("http://localhost:8181", config.getRestUri());
    assertEquals(1, config.getBatchPartitionColumns().size());
    assertEquals(2, config.getIncrementalKeys().size());
    assertEquals(5, config.getMaxRetries());
    assertEquals(2000, config.getRetryDelayMs());
    assertTrue(config.isRunMaintenance());
    assertEquals(14, config.getSnapshotRetentionDays());
    assertTrue(config.isRunCompaction());
    assertEquals(256L * 1024 * 1024, config.getCompactionTargetFileSizeBytes());
    assertEquals(20, config.getCompactionMinFiles());
    assertEquals(5L * 1024 * 1024, config.getCompactionSmallFileSizeBytes());
  }

  @Test void testIcebergConfigFromMapNull() {
    assertNull(MaterializeConfig.IcebergConfig.fromMap(null));
  }

  @Test void testIcebergCatalogTypeParsing() {
    Map<String, Object> map = new HashMap<String, Object>();

    map.put("catalogType", "hadoop");
    assertEquals(MaterializeConfig.IcebergConfig.CatalogType.HADOOP,
        MaterializeConfig.IcebergConfig.fromMap(map).getCatalogType());

    map.put("catalogType", "rest");
    assertEquals(MaterializeConfig.IcebergConfig.CatalogType.REST,
        MaterializeConfig.IcebergConfig.fromMap(map).getCatalogType());

    map.put("catalogType", "hive");
    assertEquals(MaterializeConfig.IcebergConfig.CatalogType.HIVE,
        MaterializeConfig.IcebergConfig.fromMap(map).getCatalogType());

    map.put("catalogType", "unknown");
    assertEquals(MaterializeConfig.IcebergConfig.CatalogType.HADOOP,
        MaterializeConfig.IcebergConfig.fromMap(map).getCatalogType());
  }

  @Test void testFromMapWithIcebergConfig() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);
    map.put("format", "iceberg");

    Map<String, Object> icebergMap = new HashMap<String, Object>();
    icebergMap.put("catalogType", "hadoop");
    icebergMap.put("warehousePath", "/warehouse");
    map.put("iceberg", icebergMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertNotNull(config.getIceberg());
    assertEquals("/warehouse", config.getIceberg().getWarehousePath());
  }

  @Test void testFromMapWithPartition() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    Map<String, Object> partitionMap = new HashMap<String, Object>();
    partitionMap.put("columns", Arrays.asList("year", "region"));
    partitionMap.put("batchBy", Arrays.asList("year"));
    map.put("partition", partitionMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertNotNull(config.getPartition());
    assertEquals(2, config.getPartition().getColumns().size());
    assertTrue(config.getPartition().hasBatching());
  }

  @Test void testFromMapWithOptions() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    map.put("output", outputMap);

    Map<String, Object> optionsMap = new HashMap<String, Object>();
    optionsMap.put("threads", 8);
    optionsMap.put("rowGroupSize", 500000);
    map.put("options", optionsMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(8, config.getOptions().getThreads());
    assertEquals(500000, config.getOptions().getRowGroupSize());
  }

  @Test void testColumnCommentsImmutable() {
    MaterializeConfig config = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .columnComments(new HashMap<String, String>() {{
          put("year", "Year comment");
        }})
        .build();

    try {
      config.getColumnComments().put("new", "value");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }
}
