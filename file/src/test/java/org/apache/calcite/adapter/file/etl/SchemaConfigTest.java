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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SchemaConfig}.
 */
@Tag("unit")
class SchemaConfigTest {

  private static MaterializeConfig defaultMaterialize() {
    return MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
  }

  private static EtlPipelineConfig createTableConfig(String name, String url) {
    return EtlPipelineConfig.builder()
        .name(name)
        .source(HttpSourceConfig.builder().url(url).build())
        .materialize(defaultMaterialize())
        .build();
  }

  @Test void testBuilderBasic() {
    SchemaConfig config = SchemaConfig.builder()
        .name("test_schema")
        .build();
    assertEquals("test_schema", config.getName());
    assertNull(config.getSourceDirectory());
    assertNull(config.getMaterializeDirectory());
    assertTrue(config.getTables().isEmpty());
    assertTrue(config.getBulkDownloads().isEmpty());
    assertTrue(config.getPostProcess().isEmpty());
    assertTrue(config.getMetadata().isEmpty());
    assertEquals(0, config.getTableCount());
  }

  @Test void testBuilderMissingNameThrows() {
    try {
      SchemaConfig.builder().build();
      assertTrue(false, "Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("name"));
    }
  }

  @Test void testBuilderEmptyNameThrows() {
    try {
      SchemaConfig.builder().name("").build();
      assertTrue(false, "Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("name"));
    }
  }

  @Test void testBuilderWithDirectories() {
    SchemaConfig config = SchemaConfig.builder()
        .name("econ")
        .sourceDirectory("s3://bucket/raw/")
        .materializeDirectory("s3://bucket/parquet/")
        .build();
    assertEquals("s3://bucket/raw/", config.getSourceDirectory());
    assertEquals("s3://bucket/parquet/", config.getMaterializeDirectory());
  }

  @Test void testBuilderWithTables() {
    EtlPipelineConfig table1 = createTableConfig("table1", "http://example.com");
    EtlPipelineConfig table2 = createTableConfig("table2", "http://example.com/2");

    List<EtlPipelineConfig> tables = new ArrayList<EtlPipelineConfig>();
    tables.add(table1);
    tables.add(table2);

    SchemaConfig config = SchemaConfig.builder()
        .name("test")
        .tables(tables)
        .build();
    assertEquals(2, config.getTableCount());
    assertEquals(2, config.getTables().size());
  }

  @Test void testAddTable() {
    EtlPipelineConfig table = createTableConfig("t1", "http://example.com");

    SchemaConfig config = SchemaConfig.builder()
        .name("test")
        .addTable(table)
        .build();
    assertEquals(1, config.getTableCount());
  }

  @Test void testGetTableByName() {
    EtlPipelineConfig table = createTableConfig("gdp", "http://example.com");

    SchemaConfig config = SchemaConfig.builder()
        .name("econ")
        .addTable(table)
        .build();
    assertNotNull(config.getTable("gdp"));
    assertNull(config.getTable("nonexistent"));
  }

  @Test void testTablesAreImmutable() {
    SchemaConfig config = SchemaConfig.builder()
        .name("test")
        .build();
    try {
      config.getTables().add(null);
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testBulkDownloadsAreImmutable() {
    SchemaConfig config = SchemaConfig.builder()
        .name("test")
        .build();
    try {
      config.getBulkDownloads().put("key", null);
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testHooksDefault() {
    SchemaConfig config = SchemaConfig.builder()
        .name("test")
        .build();
    assertNotNull(config.getHooks());
    assertNull(config.getHooks().getSchemaLifecycleListenerClass());
    assertNull(config.getHooks().getTableLifecycleListenerClass());
  }

  @Test void testFromMapNull() {
    assertNull(SchemaConfig.fromMap(null));
  }

  @Test void testFromMapBasic() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "my_schema");
    map.put("sourceDirectory", "/raw");
    map.put("materializeDirectory", "/parquet");

    SchemaConfig config = SchemaConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("my_schema", config.getName());
    assertEquals("/raw", config.getSourceDirectory());
    assertEquals("/parquet", config.getMaterializeDirectory());
  }

  @Test void testFromMapSchemaNameAlias() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("schemaName", "alt_name");

    SchemaConfig config = SchemaConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("alt_name", config.getName());
  }

  @Test void testFromMapWithHooks() {
    Map<String, Object> hooksMap = new HashMap<String, Object>();
    hooksMap.put("schemaLifecycleListener", "org.example.Listener");
    hooksMap.put("tableLifecycleListener", "org.example.TableListener");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "test");
    map.put("hooks", hooksMap);

    SchemaConfig config = SchemaConfig.fromMap(map);
    assertNotNull(config.getHooks());
    assertEquals("org.example.Listener",
        config.getHooks().getSchemaLifecycleListenerClass());
    assertEquals("org.example.TableListener",
        config.getHooks().getTableLifecycleListenerClass());
  }

  @Test void testFromMapWithTables() {
    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "http://example.com");

    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/output");
    Map<String, Object> matMap = new HashMap<String, Object>();
    matMap.put("output", outputMap);

    Map<String, Object> tableMap = new HashMap<String, Object>();
    tableMap.put("name", "t1");
    tableMap.put("source", sourceMap);
    tableMap.put("materialize", matMap);

    List<Object> tableList = new ArrayList<Object>();
    tableList.add(tableMap);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "test");
    map.put("tables", tableList);

    SchemaConfig config = SchemaConfig.fromMap(map);
    assertEquals(1, config.getTableCount());
  }

  @Test void testFromMapSkipsViewType() {
    Map<String, Object> viewMap = new HashMap<String, Object>();
    viewMap.put("name", "my_view");
    viewMap.put("type", "view");

    List<Object> tableList = new ArrayList<Object>();
    tableList.add(viewMap);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "test");
    map.put("tables", tableList);

    SchemaConfig config = SchemaConfig.fromMap(map);
    assertEquals(0, config.getTableCount());
  }

  @Test void testFromMapPartitionedTablesKey() {
    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "http://example.com");

    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/output");
    Map<String, Object> matMap = new HashMap<String, Object>();
    matMap.put("output", outputMap);

    Map<String, Object> tableMap = new HashMap<String, Object>();
    tableMap.put("name", "t1");
    tableMap.put("source", sourceMap);
    tableMap.put("materialize", matMap);

    List<Object> tableList = new ArrayList<Object>();
    tableList.add(tableMap);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "test");
    map.put("partitionedTables", tableList);

    SchemaConfig config = SchemaConfig.fromMap(map);
    assertEquals(1, config.getTableCount());
  }

  @Test void testSchemaHooksConfigEmpty() {
    SchemaConfig.SchemaHooksConfig hooks = SchemaConfig.SchemaHooksConfig.empty();
    assertNull(hooks.getSchemaLifecycleListenerClass());
    assertNull(hooks.getTableLifecycleListenerClass());
  }

  @Test void testSchemaHooksConfigFromMapNull() {
    SchemaConfig.SchemaHooksConfig hooks =
        SchemaConfig.SchemaHooksConfig.fromMap(null);
    assertNull(hooks.getSchemaLifecycleListenerClass());
    assertNull(hooks.getTableLifecycleListenerClass());
  }

  @Test void testAddPostProcess() {
    PostProcessConfig pp = PostProcessConfig.builder()
        .name("script1")
        .script("/bin/script.sh")
        .build();

    SchemaConfig config = SchemaConfig.builder()
        .name("test")
        .addPostProcess(pp)
        .build();
    assertEquals(1, config.getPostProcess().size());
  }

  @Test void testBuilderWithBulkDownload() {
    BulkDownloadConfig bdConfig = BulkDownloadConfig.builder()
        .name("big_file")
        .url("http://example.com/big.zip")
        .cachePattern("{name}.zip")
        .build();

    SchemaConfig config = SchemaConfig.builder()
        .name("test")
        .bulkDownload("big_file", bdConfig)
        .build();
    assertEquals(1, config.getBulkDownloads().size());
    assertNotNull(config.getBulkDownload("big_file"));
    assertNull(config.getBulkDownload("nonexistent"));
  }
}
