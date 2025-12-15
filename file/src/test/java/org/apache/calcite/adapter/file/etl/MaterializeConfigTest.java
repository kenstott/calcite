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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for MaterializeConfig and related configuration classes.
 */
@Tag("unit")
public class MaterializeConfigTest {

  @Test void testMaterializeOutputConfigBuilder() {
    MaterializeOutputConfig config = MaterializeOutputConfig.builder()
        .location("s3://bucket/data/")
        .pattern("type=sales/year=*/region=*/")
        .format("parquet")
        .compression("snappy")
        .build();

    assertEquals("s3://bucket/data/", config.getLocation());
    assertEquals("type=sales/year=*/region=*/", config.getPattern());
    assertEquals("parquet", config.getFormat());
    assertEquals("snappy", config.getCompression());
  }

  @Test void testMaterializeOutputConfigDefaults() {
    MaterializeOutputConfig config = MaterializeOutputConfig.builder()
        .location("/data/output")
        .build();

    assertEquals("/data/output", config.getLocation());
    assertNull(config.getPattern());
    assertEquals("parquet", config.getFormat());  // default
    assertEquals("snappy", config.getCompression());  // default
  }

  @Test void testMaterializeOutputConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("location", "s3://bucket/data/");
    map.put("pattern", "year=*/");
    map.put("format", "parquet");
    map.put("compression", "zstd");

    MaterializeOutputConfig config = MaterializeOutputConfig.fromMap(map);

    assertEquals("s3://bucket/data/", config.getLocation());
    assertEquals("year=*/", config.getPattern());
    assertEquals("parquet", config.getFormat());
    assertEquals("zstd", config.getCompression());
  }

  @Test void testMaterializeOutputConfigRequiresLocation() {
    assertThrows(IllegalArgumentException.class, () -> {
      MaterializeOutputConfig.builder().build();
    });
  }

  @Test void testMaterializePartitionConfigBuilder() {
    MaterializePartitionConfig config = MaterializePartitionConfig.builder()
        .columns(Arrays.asList("year", "region"))
        .batchBy(Arrays.asList("year"))
        .build();

    assertEquals(Arrays.asList("year", "region"), config.getColumns());
    assertEquals(Arrays.asList("year"), config.getBatchBy());
    assertTrue(config.hasBatching());
  }

  @Test void testMaterializePartitionConfigDefaults() {
    MaterializePartitionConfig config = MaterializePartitionConfig.builder().build();

    assertTrue(config.getColumns().isEmpty());
    assertTrue(config.getBatchBy().isEmpty());
    assertFalse(config.hasBatching());
  }

  @Test void testMaterializePartitionConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    List<String> columns = new ArrayList<String>();
    columns.add("year");
    columns.add("region");
    map.put("columns", columns);

    List<String> batchBy = new ArrayList<String>();
    batchBy.add("year");
    map.put("batchBy", batchBy);

    MaterializePartitionConfig config = MaterializePartitionConfig.fromMap(map);

    assertEquals(2, config.getColumns().size());
    assertEquals("year", config.getColumns().get(0));
    assertEquals("region", config.getColumns().get(1));
    assertEquals(1, config.getBatchBy().size());
    assertEquals("year", config.getBatchBy().get(0));
  }

  @Test void testMaterializeOptionsConfigBuilder() {
    MaterializeOptionsConfig config = MaterializeOptionsConfig.builder()
        .threads(8)
        .rowGroupSize(200000)
        .preserveInsertionOrder(true)
        .build();

    assertEquals(8, config.getThreads());
    assertEquals(200000, config.getRowGroupSize());
    assertTrue(config.isPreserveInsertionOrder());
  }

  @Test void testMaterializeOptionsConfigDefaults() {
    MaterializeOptionsConfig config = MaterializeOptionsConfig.defaults();

    assertEquals(2, config.getThreads());  // default
    assertEquals(100000, config.getRowGroupSize());  // default
    assertFalse(config.isPreserveInsertionOrder());  // default
  }

  @Test void testMaterializeOptionsConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("threads", 4);
    map.put("rowGroupSize", 50000);
    map.put("preserveInsertionOrder", true);

    MaterializeOptionsConfig config = MaterializeOptionsConfig.fromMap(map);

    assertEquals(4, config.getThreads());
    assertEquals(50000, config.getRowGroupSize());
    assertTrue(config.isPreserveInsertionOrder());
  }

  @Test void testColumnConfigBuilder() {
    ColumnConfig config = ColumnConfig.builder()
        .name("region_code")
        .type("VARCHAR")
        .source("regionCode")
        .required(true)
        .build();

    assertEquals("region_code", config.getName());
    assertEquals("VARCHAR", config.getType());
    assertEquals("regionCode", config.getSource());
    assertNull(config.getExpression());
    assertTrue(config.isRequired());
    assertFalse(config.isComputed());
    assertEquals("regionCode", config.getEffectiveSource());
  }

  @Test void testColumnConfigWithExpression() {
    ColumnConfig config = ColumnConfig.builder()
        .name("quarter")
        .type("VARCHAR")
        .expression("SUBSTR(period, 1, 2)")
        .build();

    assertEquals("quarter", config.getName());
    assertEquals("VARCHAR", config.getType());
    assertNull(config.getSource());
    assertEquals("SUBSTR(period, 1, 2)", config.getExpression());
    assertTrue(config.isComputed());
    assertEquals("quarter", config.getEffectiveSource());  // falls back to name
  }

  @Test void testColumnConfigBuildSelectExpression() {
    // Direct column
    ColumnConfig directCol = ColumnConfig.builder()
        .name("id")
        .type("INTEGER")
        .build();
    assertEquals("id", directCol.buildSelectExpression());

    // Renamed column
    ColumnConfig renamedCol = ColumnConfig.builder()
        .name("region_code")
        .type("VARCHAR")
        .source("regionCode")
        .build();
    assertEquals("\"regionCode\" AS region_code", renamedCol.buildSelectExpression());

    // Computed column
    ColumnConfig computedCol = ColumnConfig.builder()
        .name("quarter")
        .type("VARCHAR")
        .expression("SUBSTR(period, 1, 2)")
        .build();
    assertEquals("SUBSTR(period, 1, 2) AS quarter", computedCol.buildSelectExpression());
  }

  @Test void testColumnConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "year");
    map.put("type", "INTEGER");
    map.put("source", "fiscalYear");
    map.put("required", true);

    ColumnConfig config = ColumnConfig.fromMap(map);

    assertEquals("year", config.getName());
    assertEquals("INTEGER", config.getType());
    assertEquals("fiscalYear", config.getSource());
    assertTrue(config.isRequired());
  }

  @Test void testColumnConfigFromList() {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

    Map<String, Object> col1 = new HashMap<String, Object>();
    col1.put("name", "id");
    col1.put("type", "INTEGER");
    list.add(col1);

    Map<String, Object> col2 = new HashMap<String, Object>();
    col2.put("name", "name");
    col2.put("type", "VARCHAR");
    list.add(col2);

    List<ColumnConfig> configs = ColumnConfig.fromList(list);

    assertEquals(2, configs.size());
    assertEquals("id", configs.get(0).getName());
    assertEquals("name", configs.get(1).getName());
  }

  @Test void testColumnConfigRequiresName() {
    assertThrows(IllegalArgumentException.class, () -> {
      ColumnConfig.builder()
          .type("VARCHAR")
          .build();
    });
  }

  @Test void testMaterializeConfigBuilder() {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .trigger(MaterializeConfig.Trigger.AUTO)
        .name("sales_data")
        .output(MaterializeOutputConfig.builder()
            .location("s3://bucket/data/")
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year", "region"))
            .batchBy(Arrays.asList("year"))
            .build())
        .options(MaterializeOptionsConfig.builder()
            .threads(4)
            .build())
        .build();

    assertTrue(config.isEnabled());
    assertEquals(MaterializeConfig.Trigger.AUTO, config.getTrigger());
    assertEquals("sales_data", config.getName());
    assertNotNull(config.getOutput());
    assertNotNull(config.getPartition());
    assertNotNull(config.getOptions());
    assertEquals("s3://bucket/data/", config.getOutput().getLocation());
    assertEquals(2, config.getPartition().getColumns().size());
    assertEquals(4, config.getOptions().getThreads());
  }

  @Test void testMaterializeConfigDefaults() {
    MaterializeConfig config = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder()
            .location("/data/output")
            .build())
        .build();

    assertTrue(config.isEnabled());  // default true
    assertEquals(MaterializeConfig.Trigger.AUTO, config.getTrigger());  // default AUTO
    assertNull(config.getName());
    assertNotNull(config.getOptions());  // defaults applied
    assertEquals(2, config.getOptions().getThreads());  // default
  }

  @Test void testMaterializeConfigTriggerTypes() {
    // AUTO trigger
    MaterializeConfig autoConfig = MaterializeConfig.builder()
        .trigger(MaterializeConfig.Trigger.AUTO)
        .output(MaterializeOutputConfig.builder().location("/data").build())
        .build();
    assertEquals(MaterializeConfig.Trigger.AUTO, autoConfig.getTrigger());

    // MANUAL trigger
    MaterializeConfig manualConfig = MaterializeConfig.builder()
        .trigger(MaterializeConfig.Trigger.MANUAL)
        .output(MaterializeOutputConfig.builder().location("/data").build())
        .build();
    assertEquals(MaterializeConfig.Trigger.MANUAL, manualConfig.getTrigger());

    // ON_FIRST_QUERY trigger
    MaterializeConfig onFirstQueryConfig = MaterializeConfig.builder()
        .trigger(MaterializeConfig.Trigger.ON_FIRST_QUERY)
        .output(MaterializeOutputConfig.builder().location("/data").build())
        .build();
    assertEquals(MaterializeConfig.Trigger.ON_FIRST_QUERY, onFirstQueryConfig.getTrigger());
  }

  @Test void testMaterializeConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("enabled", true);
    map.put("trigger", "auto");
    map.put("name", "test_materialize");

    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "s3://bucket/output/");
    outputMap.put("compression", "zstd");
    map.put("output", outputMap);

    Map<String, Object> partitionMap = new HashMap<String, Object>();
    List<String> columns = new ArrayList<String>();
    columns.add("year");
    partitionMap.put("columns", columns);
    map.put("partition", partitionMap);

    Map<String, Object> optionsMap = new HashMap<String, Object>();
    optionsMap.put("threads", 8);
    map.put("options", optionsMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);

    assertTrue(config.isEnabled());
    assertEquals(MaterializeConfig.Trigger.AUTO, config.getTrigger());
    assertEquals("test_materialize", config.getName());
    assertEquals("s3://bucket/output/", config.getOutput().getLocation());
    assertEquals("zstd", config.getOutput().getCompression());
    assertEquals(1, config.getPartition().getColumns().size());
    assertEquals(8, config.getOptions().getThreads());
  }

  @Test void testMaterializeConfigFromMapWithColumns() {
    Map<String, Object> map = new HashMap<String, Object>();

    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data/output/");
    map.put("output", outputMap);

    List<Map<String, Object>> columnsList = new ArrayList<Map<String, Object>>();
    Map<String, Object> col1 = new HashMap<String, Object>();
    col1.put("name", "region_code");
    col1.put("type", "VARCHAR");
    col1.put("source", "regionCode");
    columnsList.add(col1);

    Map<String, Object> col2 = new HashMap<String, Object>();
    col2.put("name", "quarter");
    col2.put("type", "VARCHAR");
    col2.put("expression", "SUBSTR(period, 1, 2)");
    columnsList.add(col2);

    map.put("columns", columnsList);

    MaterializeConfig config = MaterializeConfig.fromMap(map);

    assertEquals(2, config.getColumns().size());
    assertEquals("region_code", config.getColumns().get(0).getName());
    assertEquals("regionCode", config.getColumns().get(0).getSource());
    assertEquals("quarter", config.getColumns().get(1).getName());
    assertEquals("SUBSTR(period, 1, 2)", config.getColumns().get(1).getExpression());
  }

  @Test void testMaterializeConfigRequiresOutput() {
    assertThrows(IllegalArgumentException.class, () -> {
      MaterializeConfig.builder().build();
    });
  }

  @Test void testMaterializeConfigFromMapNullReturnsNull() {
    assertNull(MaterializeConfig.fromMap(null));
    assertNull(MaterializeOutputConfig.fromMap(null));
    assertNull(MaterializePartitionConfig.fromMap(null));
    assertNull(ColumnConfig.fromMap(null));
  }
}
