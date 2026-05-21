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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link TableContext}.
 */
@Tag("unit")
class TableContextTest {

  private SchemaContext schemaContext;
  private EtlPipelineConfig tableConfig;

  private static MaterializeConfig defaultMaterialize() {
    return MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
  }

  private static EtlPipelineConfig createConfig(String name, String url) {
    return EtlPipelineConfig.builder()
        .name(name)
        .source(HttpSourceConfig.builder().url(url).build())
        .materialize(defaultMaterialize())
        .build();
  }

  @BeforeEach
  void setUp() {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test_schema")
        .build();

    schemaContext = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(new LocalFileStorageProvider())
        .sourceDirectory("/raw")
        .materializeDirectory("/parquet")
        .build();

    tableConfig = createConfig("gdp", "http://bea.gov/api/data");
  }

  @Test void testBasicCreation() {
    TableContext ctx = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .tableIndex(0)
        .totalTables(5)
        .build();

    assertEquals("gdp", ctx.getTableName());
    assertEquals(0, ctx.getTableIndex());
    assertEquals(5, ctx.getTotalTables());
    assertNotNull(ctx.getTableConfig());
    assertNotNull(ctx.getSchemaContext());
  }

  @Test void testMissingTableConfigThrows() {
    try {
      TableContext.builder()
          .schemaContext(schemaContext)
          .build();
      assertTrue(false, "Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("config"));
    }
  }

  @Test void testMissingSchemaContextThrows() {
    try {
      TableContext.builder()
          .tableConfig(tableConfig)
          .build();
      assertTrue(false, "Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("context"));
    }
  }

  @Test void testAttributes() {
    TableContext ctx = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .build();

    assertNotNull(ctx.getAttributes());
    assertTrue(ctx.getAttributes().isEmpty());

    ctx.setAttribute("key1", "value1");
    assertEquals("value1", ctx.getAttribute("key1"));
    assertNull(ctx.getAttribute("nonexistent"));
  }

  @Test void testDetectSourceFromUrl() {
    TableContext blsCtx = TableContext.builder()
        .tableConfig(createConfig("employment", "http://api.bls.gov/data"))
        .schemaContext(schemaContext)
        .build();
    assertEquals("bls", blsCtx.detectSource());
    assertEquals("BLS_API_KEY", blsCtx.getRequiredApiKeyName());
  }

  @Test void testDetectSourceFred() {
    TableContext fredCtx = TableContext.builder()
        .tableConfig(createConfig("indicators", "http://api.stlouisfed.org/data"))
        .schemaContext(schemaContext)
        .build();
    assertEquals("fred", fredCtx.detectSource());
    assertEquals("FRED_API_KEY", fredCtx.getRequiredApiKeyName());
  }

  @Test void testDetectSourceBea() {
    TableContext beaCtx = TableContext.builder()
        .tableConfig(createConfig("gdp_data", "http://apps.bea.gov/api"))
        .schemaContext(schemaContext)
        .build();
    assertEquals("bea", beaCtx.detectSource());
    assertEquals("BEA_API_KEY", beaCtx.getRequiredApiKeyName());
  }

  @Test void testDetectSourceFromTableName() {
    TableContext ctx = TableContext.builder()
        .tableConfig(createConfig("bls_employment", "http://generic.com/api"))
        .schemaContext(schemaContext)
        .build();
    assertEquals("bls", ctx.detectSource());
  }

  @Test void testDetectSourceTreasury() {
    TableContext ctx = TableContext.builder()
        .tableConfig(createConfig("rates", "http://api.treasury.gov/data"))
        .schemaContext(schemaContext)
        .build();
    assertEquals("treasury", ctx.detectSource());
    assertNull(ctx.getRequiredApiKeyName());
  }

  @Test void testDetectSourceWorldBank() {
    TableContext ctx = TableContext.builder()
        .tableConfig(createConfig("indicators", "http://api.worldbank.org/data"))
        .schemaContext(schemaContext)
        .build();
    assertEquals("worldbank", ctx.detectSource());
    assertNull(ctx.getRequiredApiKeyName());
  }

  @Test void testDetectSourceUnknown() {
    TableContext ctx = TableContext.builder()
        .tableConfig(createConfig("custom_table", "http://custom.com/api"))
        .schemaContext(schemaContext)
        .build();
    assertEquals("unknown", ctx.detectSource());
    assertNull(ctx.getRequiredApiKeyName());
  }

  @Test void testDetectSourceFromTableNamePatterns() {
    TableContext fredCtx = TableContext.builder()
        .tableConfig(createConfig("fred_rates", "http://generic.com"))
        .schemaContext(schemaContext)
        .build();
    assertEquals("fred", fredCtx.detectSource());

    TableContext econCtx = TableContext.builder()
        .tableConfig(createConfig("economic_indicators", "http://generic.com"))
        .schemaContext(schemaContext)
        .build();
    assertEquals("fred", econCtx.detectSource());
  }

  @Test void testGetSchemaName() {
    TableContext ctx = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .build();
    assertEquals("test_schema", ctx.getSchemaName());
  }

  @Test void testGetSourceDirectory() {
    TableContext ctx = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .build();
    assertEquals("/raw", ctx.getSourceDirectory());
  }

  @Test void testGetMaterializeDirectory() {
    TableContext ctx = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .build();
    assertEquals("/parquet", ctx.getMaterializeDirectory());
  }

  @Test void testGetSourceUrl() {
    TableContext ctx = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .build();
    assertEquals("http://bea.gov/api/data", ctx.getSourceUrl());
  }

  @Test void testGetSourceUrlNullSource() {
    // Hooks-only table has no source config
    EtlPipelineConfig noSourceConfig = EtlPipelineConfig.builder()
        .name("nosource")
        .hooks(HooksConfig.builder().enabled(true).build())
        .build();
    TableContext ctx = TableContext.builder()
        .tableConfig(noSourceConfig)
        .schemaContext(schemaContext)
        .build();
    assertNull(ctx.getSourceUrl());
  }

  @Test void testNotBulkDownloadSource() {
    TableContext ctx = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .build();
    assertFalse(ctx.isBulkDownloadSource());
    assertNull(ctx.getBulkDownloadName());
  }

  @Test void testGetStorageProvider() {
    TableContext ctx = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .build();
    assertNotNull(ctx.getStorageProvider());
  }
}
