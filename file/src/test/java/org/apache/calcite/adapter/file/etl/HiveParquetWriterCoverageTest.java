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
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link HiveParquetWriter}.
 */
@Tag("unit")
class HiveParquetWriterCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;
  private HiveParquetWriter writer;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    writer = new HiveParquetWriter(storageProvider, tempDir.toString());
  }

  // ========== Constructor and factory ==========

  @Test void testConstructor() {
    HiveParquetWriter w = new HiveParquetWriter(storageProvider, "/base/path");
    assertNotNull(w);
  }

  @Test void testCreateFactory() {
    HiveParquetWriter w = HiveParquetWriter.create(storageProvider, "/base/path");
    assertNotNull(w);
  }

  // ========== materialize() disabled path ==========

  @Test void testMaterializeDisabled() throws IOException {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .build())
        .build();

    DataSource source = new EmptyDataSource();
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSkipped());
    assertEquals("Materialization disabled", result.getMessage());
  }

  // ========== materialize() with empty source ==========

  @Test void testMaterializeEmptySource() throws IOException {
    Path outputDir = tempDir.resolve("output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("empty_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    DataSource source = new EmptyDataSource();
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  // ========== materialize() with batching ==========

  @Test void testMaterializeWithBatchByColumns() throws IOException {
    Path outputDir = tempDir.resolve("batch_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("batch_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year"))
            .batchBy(Arrays.asList("year"))
            .build())
        .build();

    DataSource source = new EmptyDataSource();
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  // ========== materialize() without name ==========

  @Test void testMaterializeWithoutName() throws IOException {
    Path outputDir = tempDir.resolve("noname_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    DataSource source = new EmptyDataSource();
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  // ========== resolveOutputPath coverage ==========

  @Test void testResolveOutputPathWithBaseDirectory() throws IOException {
    Path outputDir = tempDir.resolve("base_dir_output");
    Files.createDirectories(outputDir);

    // Config with null output location - should use baseDirectory
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    DataSource source = new EmptyDataSource();
    MaterializeResult result = writer.materialize(config, source);
    assertNotNull(result);
  }

  @Test void testResolveOutputPathWithBaseDirectoryPlaceholder() throws IOException {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder()
            .location("{baseDirectory}")
            .build())
        .build();

    DataSource source = new EmptyDataSource();
    MaterializeResult result = writer.materialize(config, source);
    assertNotNull(result);
  }

  // ========== MaterializeResult coverage ==========

  @Test void testMaterializeResultSuccess() {
    MaterializeResult result = MaterializeResult.success(1000, 5, 500);
    assertTrue(result.isSuccess());
    assertEquals(MaterializeResult.Status.SUCCESS, result.getStatus());
    assertEquals(1000, result.getRowCount());
    assertEquals(5, result.getFileCount());
    assertEquals(500, result.getElapsedMillis());
  }

  @Test void testMaterializeResultSkipped() {
    MaterializeResult result = MaterializeResult.skipped("Already done");
    assertTrue(result.isSkipped());
    assertEquals("Already done", result.getMessage());
  }

  @Test void testMaterializeResultError() {
    MaterializeResult result = MaterializeResult.error("Failed", 200);
    assertTrue(result.isError());
    assertEquals("Failed", result.getMessage());
    assertEquals(200, result.getElapsedMillis());
  }

  @Test void testMaterializeResultToString() {
    MaterializeResult result = MaterializeResult.success(100, 3, 250);
    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("SUCCESS"));
  }

  // ========== MaterializeConfig builder coverage ==========

  @Test void testMaterializeConfigWithColumns() {
    MaterializeConfig config = MaterializeConfig.builder()
        .name("col_test")
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .compression("zstd")
            .build())
        .columns(Arrays.asList(
            ColumnConfig.builder().name("id").type("INTEGER").build(),
            ColumnConfig.builder().name("name").type("VARCHAR").source("fullName").build(),
            ColumnConfig.builder().name("computed").type("INTEGER").expression("1 + 1").build()
        ))
        .options(MaterializeOptionsConfig.builder()
            .threads(4)
            .rowGroupSize(100000)
            .preserveInsertionOrder(true)
            .build())
        .build();

    assertNotNull(config);
    assertEquals("col_test", config.getName());
    assertNotNull(config.getColumns());
    assertEquals(3, config.getColumns().size());
  }

  @Test void testMaterializeConfigWithPartition() {
    MaterializeConfig config = MaterializeConfig.builder()
        .name("part_test")
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year", "region"))
            .batchBy(Arrays.asList("year"))
            .build())
        .build();

    assertNotNull(config);
    MaterializePartitionConfig partConfig = config.getPartition();
    assertNotNull(partConfig);
    assertEquals(2, partConfig.getColumns().size());
    assertEquals(1, partConfig.getBatchBy().size());
  }

  // ========== ColumnConfig builder coverage ==========

  @Test void testColumnConfigWithSource() {
    ColumnConfig col = ColumnConfig.builder()
        .name("output_name")
        .type("VARCHAR")
        .source("input_name")
        .build();

    assertNotNull(col);
    assertEquals("output_name", col.getName());
    String selectExpr = col.buildSelectExpression();
    assertNotNull(selectExpr);
  }

  @Test void testColumnConfigWithExpression() {
    ColumnConfig col = ColumnConfig.builder()
        .name("computed")
        .type("INTEGER")
        .expression("CAST(raw_value AS INTEGER)")
        .build();

    assertNotNull(col);
    String selectExpr = col.buildSelectExpression();
    assertNotNull(selectExpr);
  }

  @Test void testColumnConfigSimple() {
    ColumnConfig col = ColumnConfig.builder()
        .name("id")
        .type("INTEGER")
        .build();

    assertNotNull(col);
    String selectExpr = col.buildSelectExpression();
    assertNotNull(selectExpr);
  }

  // ========== DataSource implementation for testing ==========

  private static class EmptyDataSource implements DataSource {
    @Override
    public Iterator<Map<String, Object>> fetch(Map<String, String> variables) {
      return Collections.<Map<String, Object>>emptyIterator();
    }

    @Override
    public String getType() {
      return "test";
    }
  }
}
