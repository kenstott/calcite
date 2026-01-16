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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for metadata propagation to materialized Parquet files.
 *
 * <p>Verifies that table and column comments configured in MaterializeConfig
 * are properly embedded in Parquet file footer metadata.
 */
@Tag("integration")
public class MetadataPropagationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  java.nio.file.Path tempDir;

  private StorageProvider storageProvider;
  private HiveParquetWriter writer;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    writer = new HiveParquetWriter(storageProvider, tempDir.toString());
  }

  @Test void testTableCommentPropagatedToParquet() throws IOException {
    // Create test JSON file with partition column for hive-style output
    String jsonContent = "[{\"id\": 1, \"name\": \"Alice\", \"year\": 2020},"
        + "{\"id\": 2, \"name\": \"Bob\", \"year\": 2020}]";

    java.nio.file.Path jsonFile = tempDir.resolve("test_data.json");
    Files.write(jsonFile, jsonContent.getBytes());

    java.nio.file.Path outputDir = tempDir.resolve("output_table_comment");
    Files.createDirectories(outputDir);

    String expectedTableComment = "Test table with sample data";

    MaterializeConfig config = MaterializeConfig.builder()
        .name("table_comment_test")
        .tableComment(expectedTableComment)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(java.util.Arrays.asList("year"))
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());

    assertNotNull(result);
    assertTrue(result.isSuccess());

    // Find parquet files recursively in partition directories
    java.util.List<File> parquetFiles = findParquetFiles(outputDir.toFile());
    assertTrue(!parquetFiles.isEmpty(), "Should have at least one parquet file");

    Map<String, String> metadata = readParquetMetadata(parquetFiles.get(0).getAbsolutePath());
    assertEquals(expectedTableComment, metadata.get("table_comment"),
        "Table comment should be in Parquet metadata");
  }

  @Test void testColumnCommentsPropagatedToParquet() throws IOException {
    // Create test JSON file with partition column for hive-style output
    String jsonContent = "[{\"user_id\": 1, \"user_name\": \"Alice\", \"age\": 30, \"year\": 2020},"
        + "{\"user_id\": 2, \"user_name\": \"Bob\", \"age\": 25, \"year\": 2020}]";

    java.nio.file.Path jsonFile = tempDir.resolve("test_data_cols.json");
    Files.write(jsonFile, jsonContent.getBytes());

    java.nio.file.Path outputDir = tempDir.resolve("output_column_comments");
    Files.createDirectories(outputDir);

    Map<String, String> expectedColumnComments = new HashMap<>();
    expectedColumnComments.put("user_id", "Unique identifier for the user");
    expectedColumnComments.put("user_name", "Full name of the user");
    expectedColumnComments.put("age", "Age in years");

    MaterializeConfig config = MaterializeConfig.builder()
        .name("column_comments_test")
        .columnComments(expectedColumnComments)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(java.util.Arrays.asList("year"))
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());

    assertNotNull(result);
    assertTrue(result.isSuccess());

    // Find parquet files recursively in partition directories
    java.util.List<File> parquetFiles = findParquetFiles(outputDir.toFile());
    assertTrue(!parquetFiles.isEmpty(), "Should have at least one parquet file");

    Map<String, String> metadata = readParquetMetadata(parquetFiles.get(0).getAbsolutePath());
    assertNotNull(metadata.get("column_comments"), "Should have column_comments in metadata");

    // Parse the JSON column comments
    Map<String, String> actualColumnComments = MAPPER.readValue(
        metadata.get("column_comments"),
        new TypeReference<Map<String, String>>() { });

    assertEquals(expectedColumnComments.get("user_id"), actualColumnComments.get("user_id"));
    assertEquals(expectedColumnComments.get("user_name"), actualColumnComments.get("user_name"));
    assertEquals(expectedColumnComments.get("age"), actualColumnComments.get("age"));
  }

  @Test void testBothTableAndColumnCommentsPropagated() throws IOException {
    // Create test JSON file with partition column for hive-style output
    String jsonContent = "[{\"region\": \"NORTH\", \"sales\": 1000, \"year\": 2020},"
        + "{\"region\": \"SOUTH\", \"sales\": 2000, \"year\": 2020}]";

    java.nio.file.Path jsonFile = tempDir.resolve("test_data_both.json");
    Files.write(jsonFile, jsonContent.getBytes());

    java.nio.file.Path outputDir = tempDir.resolve("output_both_comments");
    Files.createDirectories(outputDir);

    String expectedTableComment = "Regional sales data";
    Map<String, String> expectedColumnComments = new HashMap<>();
    expectedColumnComments.put("region", "Geographic region code");
    expectedColumnComments.put("sales", "Total sales in USD");

    MaterializeConfig config = MaterializeConfig.builder()
        .name("both_comments_test")
        .tableComment(expectedTableComment)
        .columnComments(expectedColumnComments)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(java.util.Arrays.asList("year"))
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());

    assertNotNull(result);
    assertTrue(result.isSuccess());

    // Find parquet files recursively in partition directories
    java.util.List<File> parquetFiles = findParquetFiles(outputDir.toFile());
    assertTrue(!parquetFiles.isEmpty(), "Should have at least one parquet file");

    Map<String, String> metadata = readParquetMetadata(parquetFiles.get(0).getAbsolutePath());

    // Verify table comment
    assertEquals(expectedTableComment, metadata.get("table_comment"),
        "Table comment should be in Parquet metadata");

    // Verify column comments
    assertNotNull(metadata.get("column_comments"), "Should have column_comments in metadata");
    Map<String, String> actualColumnComments = MAPPER.readValue(
        metadata.get("column_comments"),
        new TypeReference<Map<String, String>>() { });

    assertEquals(expectedColumnComments.get("region"), actualColumnComments.get("region"));
    assertEquals(expectedColumnComments.get("sales"), actualColumnComments.get("sales"));
  }

  @Test void testNoMetadataWhenNotConfigured() throws IOException {
    // Create test JSON file with partition column for hive-style output
    String jsonContent = "[{\"id\": 1, \"value\": 100, \"year\": 2020}]";

    java.nio.file.Path jsonFile = tempDir.resolve("test_data_no_comments.json");
    Files.write(jsonFile, jsonContent.getBytes());

    java.nio.file.Path outputDir = tempDir.resolve("output_no_comments");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("no_comments_test")
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(java.util.Arrays.asList("year"))
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());

    assertNotNull(result);
    assertTrue(result.isSuccess());

    // Find parquet files recursively in partition directories
    java.util.List<File> parquetFiles = findParquetFiles(outputDir.toFile());
    assertTrue(!parquetFiles.isEmpty(), "Should have at least one parquet file");

    Map<String, String> metadata = readParquetMetadata(parquetFiles.get(0).getAbsolutePath());

    // Should not have our custom metadata keys (but may have DuckDB default metadata)
    assertTrue(metadata.get("table_comment") == null,
        "Should not have table_comment when not configured");
    assertTrue(metadata.get("column_comments") == null,
        "Should not have column_comments when not configured");
  }

  /**
   * Finds all Parquet files recursively under a directory.
   */
  private java.util.List<File> findParquetFiles(File dir) {
    java.util.List<File> result = new java.util.ArrayList<>();
    findParquetFilesRecursive(dir, result);
    return result;
  }

  private void findParquetFilesRecursive(File dir, java.util.List<File> result) {
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      if (file.isDirectory()) {
        findParquetFilesRecursive(file, result);
      } else if (file.getName().endsWith(".parquet")) {
        result.add(file);
      }
    }
  }

  /**
   * Reads key-value metadata from a Parquet file footer.
   */
  private Map<String, String> readParquetMetadata(String filePath) throws IOException {
    Configuration conf = new Configuration();
    Path hadoopPath = new Path(filePath);
    InputFile inputFile = HadoopInputFile.fromPath(hadoopPath, conf);

    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      ParquetMetadata footer = reader.getFooter();
      return footer.getFileMetaData().getKeyValueMetaData();
    }
  }
}
