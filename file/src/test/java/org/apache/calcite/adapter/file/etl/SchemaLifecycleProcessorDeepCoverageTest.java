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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link SchemaLifecycleProcessor} targeting uncovered code paths:
 * builder pattern, process() lifecycle phases, table enabled/disabled logic,
 * error handling, post-processing hooks, archive raw cache, dimension resolver,
 * DataProvider creation, and table with no source.
 */
@Tag("unit")
public class SchemaLifecycleProcessorDeepCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider mockStorage;
  private IncrementalTracker mockTracker;
  private MockedStatic<MaterializationWriterFactory> factoryMock;

  @BeforeEach
  void setUp() {
    mockStorage = mock(StorageProvider.class);
    mockTracker = mock(IncrementalTracker.class);

    MaterializationWriter mockWriter = mock(MaterializationWriter.class);
    factoryMock = mockStatic(MaterializationWriterFactory.class);
    factoryMock.when(() -> MaterializationWriterFactory.createFromConfig(
        any(), any(), anyString(), any()))
        .thenReturn(mockWriter);
  }

  @AfterEach
  void tearDown() {
    if (factoryMock != null) {
      factoryMock.close();
    }
  }

  // --- Builder tests ---

  @Test
  void testBuilderMinimal() {
    SchemaConfig config = buildMinimalSchemaConfig("test_schema");

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(mockStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .incrementalTracker(mockTracker)
        .build();

    assertNotNull(processor);
  }

  @Test
  void testBuilderWithSourceStorageProvider() {
    SchemaConfig config = buildMinimalSchemaConfig("test_schema");
    StorageProvider sourceStorage = mock(StorageProvider.class);

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(mockStorage)
        .sourceStorageProvider(sourceStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .incrementalTracker(mockTracker)
        .build();

    assertNotNull(processor);
  }

  @Test
  void testBuilderWithOperatingDirectory() {
    SchemaConfig config = buildMinimalSchemaConfig("test_schema");

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(mockStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .operatingDirectory(tempDir.toString())
        .incrementalTracker(mockTracker)
        .build();

    assertNotNull(processor);
  }

  @Test
  void testBuilderWithListeners() {
    SchemaConfig config = buildMinimalSchemaConfig("test_schema");
    SchemaLifecycleListener schemaListener = mock(SchemaLifecycleListener.class);
    TableLifecycleListener tableListener = mock(TableLifecycleListener.class);

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(mockStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .incrementalTracker(mockTracker)
        .schemaListener(schemaListener)
        .defaultTableListener(tableListener)
        .build();

    assertNotNull(processor);
  }

  @Test
  void testBuilderNullTracker() {
    SchemaConfig config = buildMinimalSchemaConfig("test_schema");

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(mockStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .build();

    assertNotNull(processor);
  }

  // --- Process with empty tables list ---

  @Test
  void testProcessEmptyTables() throws Exception {
    SchemaConfig config = buildMinimalSchemaConfig("empty_schema");
    SchemaLifecycleListener schemaListener = mock(SchemaLifecycleListener.class);
    TableLifecycleListener tableListener = mock(TableLifecycleListener.class);

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(mockStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .incrementalTracker(mockTracker)
        .schemaListener(schemaListener)
        .defaultTableListener(tableListener)
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
    assertEquals("empty_schema", result.getSchemaName());

    verify(schemaListener).beforeSchema(any(SchemaContext.class));
    verify(schemaListener).afterSchema(any(SchemaContext.class), any(SchemaResult.class));
  }

  // --- Process with disabled table ---

  @Test
  void testProcessWithDisabledTable() throws Exception {
    EtlPipelineConfig tableConfig = EtlPipelineConfig.builder()
        .name("disabled_table")
        .enabled(false)
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .build();

    SchemaConfig config = buildSchemaConfigWithTables("disabled_schema",
        Collections.singletonList(tableConfig));

    SchemaLifecycleListener schemaListener = mock(SchemaLifecycleListener.class);
    TableLifecycleListener tableListener = mock(TableLifecycleListener.class);
    when(tableListener.isTableEnabled(any())).thenReturn(true);
    when(tableListener.resolveDimensions(any(), any())).thenReturn(null);

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(mockStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .incrementalTracker(mockTracker)
        .schemaListener(schemaListener)
        .defaultTableListener(tableListener)
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
  }

  // --- Process with hook-disabled table ---

  @Test
  void testProcessWithHookDisabledTable() throws Exception {
    EtlPipelineConfig tableConfig = EtlPipelineConfig.builder()
        .name("hook_disabled_table")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .output(MaterializeOutputConfig.builder().pattern("test/").build())
            .build())
        .build();

    SchemaConfig config = buildSchemaConfigWithTables("hook_disabled_schema",
        Collections.singletonList(tableConfig));

    SchemaLifecycleListener schemaListener = mock(SchemaLifecycleListener.class);
    TableLifecycleListener tableListener = mock(TableLifecycleListener.class);
    when(tableListener.isTableEnabled(any())).thenReturn(false);

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(mockStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .incrementalTracker(mockTracker)
        .schemaListener(schemaListener)
        .defaultTableListener(tableListener)
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
  }

  // --- Process with table that has no source (hooks only) ---

  @Test
  void testProcessTableNoSource() throws Exception {
    EtlPipelineConfig tableConfig = EtlPipelineConfig.builder()
        .name("no_source_table")
        // No source config - hooks-only table
        .hooks(HooksConfig.builder().enabled(true).build())
        .build();

    SchemaConfig config = buildSchemaConfigWithTables("no_source_schema",
        Collections.singletonList(tableConfig));

    SchemaLifecycleListener schemaListener = mock(SchemaLifecycleListener.class);
    TableLifecycleListener tableListener = mock(TableLifecycleListener.class);
    when(tableListener.isTableEnabled(any())).thenReturn(true);
    when(tableListener.resolveDimensions(any(), any())).thenReturn(null);

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(mockStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .incrementalTracker(mockTracker)
        .schemaListener(schemaListener)
        .defaultTableListener(tableListener)
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);

    verify(tableListener).beforeTable(any(TableContext.class));
    verify(tableListener).afterTable(any(TableContext.class), any(EtlResult.class));
  }

  // --- SchemaResult builder ---

  @Test
  void testSchemaResultBuilder() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("test")
        .elapsedMs(5000)
        .addTableResult("table1", EtlResult.skipped("table1", 100))
        .addError("test error")
        .build();

    assertEquals("test", result.getSchemaName());
    assertEquals(5000, result.getElapsedMs());
  }

  // --- SchemaContext builder ---

  @Test
  void testSchemaContextBuilder() {
    SchemaConfig config = buildMinimalSchemaConfig("test_schema");

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(mockStorage)
        .sourceStorageProvider(mockStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .operatingDirectory(tempDir.toString())
        .incrementalTracker(mockTracker)
        .build();

    assertNotNull(ctx);
    assertEquals(config, ctx.getConfig());
    assertEquals(mockStorage, ctx.getStorageProvider());
    assertEquals("/source", ctx.getSourceDirectory());
    assertEquals("/output", ctx.getMaterializeDirectory());
    assertEquals(tempDir.toString(), ctx.getOperatingDirectory());
  }

  // --- TableContext builder ---

  @Test
  void testTableContextBuilder() {
    SchemaConfig schemaConfig = buildMinimalSchemaConfig("test_schema");
    EtlPipelineConfig tableConfig = EtlPipelineConfig.builder()
        .name("test_table")
        .source(HttpSourceConfig.builder().url("https://example.com").build())
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .output(MaterializeOutputConfig.builder().pattern("test/").build())
            .build())
        .build();

    SchemaContext schemaCtx = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(mockStorage)
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .incrementalTracker(mockTracker)
        .build();

    TableContext tableCtx = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaCtx)
        .tableIndex(0)
        .totalTables(5)
        .build();

    assertNotNull(tableCtx);
    assertEquals(tableConfig, tableCtx.getTableConfig());
    assertEquals(schemaCtx, tableCtx.getSchemaContext());
    assertEquals(0, tableCtx.getTableIndex());
    assertEquals(5, tableCtx.getTotalTables());
  }

  // --- Helper methods ---

  private SchemaConfig buildMinimalSchemaConfig(String name) {
    return SchemaConfig.builder()
        .name(name)
        .tables(new ArrayList<EtlPipelineConfig>())
        .build();
  }

  private SchemaConfig buildSchemaConfigWithTables(String name,
      List<EtlPipelineConfig> tables) {
    return SchemaConfig.builder()
        .name(name)
        .tables(tables)
        .build();
  }
}
