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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HiveParquetWriter.
 */
@Tag("integration")
public class HiveParquetWriterTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;
  private HiveParquetWriter writer;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    writer = new HiveParquetWriter(storageProvider, tempDir.toString());
  }

  @Test void testMaterializeFromJsonSimple() throws IOException {
    // Create test JSON file
    String jsonContent = "[{\"id\": 1, \"name\": \"Alice\", \"year\": 2020},"
        + "{\"id\": 2, \"name\": \"Bob\", \"year\": 2020},"
        + "{\"id\": 3, \"name\": \"Charlie\", \"year\": 2021}]";

    Path jsonFile = tempDir.resolve("test_data.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path outputDir = tempDir.resolve("output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("test_materialize")
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());

    assertNotNull(result);
    assertTrue(result.isSuccess());
    assertTrue(result.getElapsedMillis() >= 0);
  }

  @Test void testMaterializeFromJsonWithPartitioning() throws IOException {
    // Create test JSON file with partition-suitable data
    String jsonContent = "[{\"id\": 1, \"name\": \"Alice\", \"year\": 2020, \"region\": \"EAST\"},"
        + "{\"id\": 2, \"name\": \"Bob\", \"year\": 2020, \"region\": \"WEST\"},"
        + "{\"id\": 3, \"name\": \"Charlie\", \"year\": 2021, \"region\": \"EAST\"}]";

    Path jsonFile = tempDir.resolve("partitioned_data.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path outputDir = tempDir.resolve("partitioned_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("partitioned_materialize")
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year", "region"))
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());

    assertNotNull(result);
    assertTrue(result.isSuccess());

    // Verify partition directories exist
    assertTrue(outputDir.toFile().exists());
  }

  @Test void testMaterializeFromJsonWithColumnTransform() throws IOException {
    // Create test JSON file
    String jsonContent = "[{\"fullName\": \"Alice Smith\", \"fiscalYear\": 2020, \"period\": \"Q1\"},"
        + "{\"fullName\": \"Bob Jones\", \"fiscalYear\": 2021, \"period\": \"Q2\"}]";

    Path jsonFile = tempDir.resolve("transform_data.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path outputDir = tempDir.resolve("transform_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("transform_materialize")
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .columns(
            Arrays.asList(
            ColumnConfig.builder()
                .name("name")
                .type("VARCHAR")
                .source("fullName")
                .build(),
            ColumnConfig.builder()
                .name("year")
                .type("INTEGER")
                .source("fiscalYear")
                .build(),
            ColumnConfig.builder()
                .name("quarter_num")
                .type("INTEGER")
                .expression("CAST(SUBSTR(period, 2, 1) AS INTEGER)")
                .build()))
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());

    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  @Test void testMaterializeFromCsvSimple() throws IOException {
    // Create test CSV file
    String csvContent = "id,name,year\n1,Alice,2020\n2,Bob,2020\n3,Charlie,2021\n";

    Path csvFile = tempDir.resolve("test_data.csv");
    Files.write(csvFile, csvContent.getBytes());

    Path outputDir = tempDir.resolve("csv_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("csv_materialize")
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    MaterializeResult result = writer.materializeFromCsv(config, csvFile.toString());

    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  @Test void testMaterializeDisabled() throws IOException {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .build())
        .build();

    // Create a simple data source
    DataSource source = new SimpleDataSource();

    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSkipped());
    assertEquals("Materialization disabled", result.getMessage());
  }

  @Test void testMaterializeResultSuccess() {
    MaterializeResult result = MaterializeResult.success(1000, 5, 500);

    assertTrue(result.isSuccess());
    assertEquals(MaterializeResult.Status.SUCCESS, result.getStatus());
    assertEquals(1000, result.getRowCount());
    assertEquals(5, result.getFileCount());
    assertEquals(500, result.getElapsedMillis());
  }

  @Test void testMaterializeResultSkipped() {
    MaterializeResult result = MaterializeResult.skipped("Already up to date");

    assertTrue(result.isSkipped());
    assertEquals(MaterializeResult.Status.SKIPPED, result.getStatus());
    assertEquals("Already up to date", result.getMessage());
  }

  @Test void testMaterializeResultError() {
    MaterializeResult result = MaterializeResult.error("Connection failed", 100);

    assertTrue(result.isError());
    assertEquals(MaterializeResult.Status.ERROR, result.getStatus());
    assertEquals("Connection failed", result.getMessage());
    assertEquals(100, result.getElapsedMillis());
  }

  @Test void testMaterializeResultToString() {
    MaterializeResult success = MaterializeResult.success(1000, 5, 500);
    String str = success.toString();
    assertTrue(str.contains("SUCCESS"));
    assertTrue(str.contains("1000"));
    assertTrue(str.contains("5"));
    assertTrue(str.contains("500ms"));
  }

  @Test void testWriterCreate() {
    HiveParquetWriter created = HiveParquetWriter.create(storageProvider, "/base/path");
    assertNotNull(created);
  }

  @Test void testMaterializeOptionsApplied() throws IOException {
    // Create test JSON file
    String jsonContent = "[{\"id\": 1, \"name\": \"Test\"}]";

    Path jsonFile = tempDir.resolve("options_data.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path outputDir = tempDir.resolve("options_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("options_test")
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("zstd")  // Different compression
            .build())
        .options(MaterializeOptionsConfig.builder()
            .threads(4)
            .rowGroupSize(50000)
            .preserveInsertionOrder(true)
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());

    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  /**
   * Simple test data source implementation.
   */
  private static class SimpleDataSource implements DataSource {
    @Override public java.util.Iterator<java.util.Map<String, Object>> fetch(
        java.util.Map<String, String> variables) {
      return java.util.Collections.emptyIterator();
    }

    @Override public String getType() {
      return "test";
    }
  }
}
