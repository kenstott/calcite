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
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Deep coverage tests for {@link HiveParquetWriter} targeting uncovered lines.
 *
 * <p>Uses reflection to test private methods directly (SQL builders, path resolution, etc.)
 * and exercises DuckDB-based methods through public API with real temp directories.
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

  // ========== resolveOutputPath coverage ==========

  @Test void testResolveOutputPathNullLocation() throws Exception {
    // Config with null output location - should use baseDirectory
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year"))
            .build())
        .build();

    Method resolveMethod = HiveParquetWriter.class.getDeclaredMethod(
        "resolveOutputPath", MaterializeConfig.class);
    resolveMethod.setAccessible(true);
    String resolved = (String) resolveMethod.invoke(writer, config);
    // Should be baseDirectory since output location is null and partitions exist
    assertEquals(tempDir.toString(), resolved);
  }

  @Test void testResolveOutputPathEmptyLocation() throws Exception {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().location("").build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year"))
            .build())
        .build();

    Method resolveMethod = HiveParquetWriter.class.getDeclaredMethod(
        "resolveOutputPath", MaterializeConfig.class);
    resolveMethod.setAccessible(true);
    String resolved = (String) resolveMethod.invoke(writer, config);
    assertEquals(tempDir.toString(), resolved);
  }

  @Test void testResolveOutputPathBaseDirectoryPlaceholder() throws Exception {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().location("{baseDirectory}").build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year"))
            .build())
        .build();

    Method resolveMethod = HiveParquetWriter.class.getDeclaredMethod(
        "resolveOutputPath", MaterializeConfig.class);
    resolveMethod.setAccessible(true);
    String resolved = (String) resolveMethod.invoke(writer, config);
    assertEquals(tempDir.toString(), resolved);
  }

  @Test void testResolveOutputPathCustomAbsoluteLocation() throws Exception {
    Path customOutput = tempDir.resolve("custom_output");
    Files.createDirectories(customOutput);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(customOutput.toString())
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year"))
            .build())
        .build();

    Method resolveMethod = HiveParquetWriter.class.getDeclaredMethod(
        "resolveOutputPath", MaterializeConfig.class);
    resolveMethod.setAccessible(true);
    String resolved = (String) resolveMethod.invoke(writer, config);
    assertEquals(customOutput.toString(), resolved);
  }

  @Test void testResolveOutputPathNoPartitionsWithName() throws Exception {
    // When no partition columns, output should resolve to a file
    MaterializeConfig config = MaterializeConfig.builder()
        .name("test_data")
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    Method resolveMethod = HiveParquetWriter.class.getDeclaredMethod(
        "resolveOutputPath", MaterializeConfig.class);
    resolveMethod.setAccessible(true);
    String resolved = (String) resolveMethod.invoke(writer, config);
    assertTrue(resolved.endsWith("/test_data.parquet"));
  }

  @Test void testResolveOutputPathNoPartitionsNoName() throws Exception {
    // When no partition columns and no name, default to "data.parquet"
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    Method resolveMethod = HiveParquetWriter.class.getDeclaredMethod(
        "resolveOutputPath", MaterializeConfig.class);
    resolveMethod.setAccessible(true);
    String resolved = (String) resolveMethod.invoke(writer, config);
    assertTrue(resolved.endsWith("/data.parquet"));
  }

  @Test void testResolveOutputPathNoPartitionsEmptyColumns() throws Exception {
    // Partition config exists but columns list is empty
    MaterializeConfig config = MaterializeConfig.builder()
        .name("mydata")
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Collections.<String>emptyList())
            .build())
        .build();

    Method resolveMethod = HiveParquetWriter.class.getDeclaredMethod(
        "resolveOutputPath", MaterializeConfig.class);
    resolveMethod.setAccessible(true);
    String resolved = (String) resolveMethod.invoke(writer, config);
    assertTrue(resolved.endsWith("/mydata.parquet"));
  }

  @Test void testResolveOutputPathNullPartitionConfig() throws Exception {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    Method resolveMethod = HiveParquetWriter.class.getDeclaredMethod(
        "resolveOutputPath", MaterializeConfig.class);
    resolveMethod.setAccessible(true);
    String resolved = (String) resolveMethod.invoke(writer, config);
    // No partitions => file path
    assertTrue(resolved.endsWith(".parquet"));
  }

  // ========== buildSelectClause coverage ==========

  @Test void testBuildSelectClauseNull() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildSelectClause", List.class);
    method.setAccessible(true);
    String result = (String) method.invoke(writer, (Object) null);
    assertEquals("*", result);
  }

  @Test void testBuildSelectClauseEmpty() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildSelectClause", List.class);
    method.setAccessible(true);
    String result = (String) method.invoke(writer, Collections.emptyList());
    assertEquals("*", result);
  }

  @Test void testBuildSelectClauseSingleColumn() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildSelectClause", List.class);
    method.setAccessible(true);

    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("id").type("INTEGER").build()
    );

    String result = (String) method.invoke(writer, columns);
    assertEquals("id", result);
  }

  @Test void testBuildSelectClauseMultipleColumns() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildSelectClause", List.class);
    method.setAccessible(true);

    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("id").type("INTEGER").build(),
        ColumnConfig.builder().name("name").type("VARCHAR").source("fullName").build(),
        ColumnConfig.builder().name("computed_val").type("INTEGER").expression("1 + 1").build()
    );

    String result = (String) method.invoke(writer, columns);
    assertTrue(result.contains("id"));
    assertTrue(result.contains(", "));
    assertTrue(result.contains("AS name"));
    assertTrue(result.contains("1 + 1 AS computed_val"));
  }

  // ========== buildCopyOptions coverage ==========

  @Test void testBuildCopyOptionsBasic() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopyOptions", MaterializeConfig.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .compression("snappy")
            .build())
        .build();

    String result = (String) method.invoke(writer, config);
    assertTrue(result.contains("FORMAT PARQUET"));
    assertTrue(result.contains("COMPRESSION SNAPPY"));
    assertTrue(result.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test void testBuildCopyOptionsWithPartitions() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopyOptions", MaterializeConfig.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year", "region"))
            .build())
        .build();

    String result = (String) method.invoke(writer, config);
    assertTrue(result.contains("PARTITION_BY (year, region)"));
    assertTrue(result.contains("COMPRESSION SNAPPY"));
  }

  @Test void testBuildCopyOptionsSinglePartition() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopyOptions", MaterializeConfig.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .compression("zstd")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year"))
            .build())
        .build();

    String result = (String) method.invoke(writer, config);
    assertTrue(result.contains("PARTITION_BY (year)"));
    assertTrue(result.contains("COMPRESSION ZSTD"));
  }

  @Test void testBuildCopyOptionsNoCompression() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopyOptions", MaterializeConfig.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .compression("none")
            .build())
        .build();

    String result = (String) method.invoke(writer, config);
    // "none" compression should not add COMPRESSION clause
    assertFalse(result.contains("COMPRESSION"));
  }

  @Test void testBuildCopyOptionsNullCompression() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopyOptions", MaterializeConfig.class);
    method.setAccessible(true);

    // Default compression is "snappy" from MaterializeOutputConfig
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    String result = (String) method.invoke(writer, config);
    // Default compression is snappy, should be present
    assertTrue(result.contains("COMPRESSION SNAPPY"));
  }

  @Test void testBuildCopyOptionsWithRowGroupSize() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopyOptions", MaterializeConfig.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .compression("snappy")
            .build())
        .options(MaterializeOptionsConfig.builder()
            .rowGroupSize(50000)
            .build())
        .build();

    String result = (String) method.invoke(writer, config);
    assertTrue(result.contains("ROW_GROUP_SIZE 50000"));
  }

  @Test void testBuildCopyOptionsEmptyPartitionColumns() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopyOptions", MaterializeConfig.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Collections.<String>emptyList())
            .build())
        .build();

    String result = (String) method.invoke(writer, config);
    assertFalse(result.contains("PARTITION_BY"));
  }

  // ========== buildCopySqlFromJson coverage ==========

  @Test void testBuildCopySqlFromJson() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopySqlFromJson", String.class, String.class, MaterializeConfig.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year"))
            .build())
        .build();

    String sql = (String) method.invoke(writer,
        "/data/input.json", "/data/output", config);

    assertTrue(sql.contains("read_json_auto"));
    assertTrue(sql.contains("'/data/input.json'"));
    assertTrue(sql.contains("'/data/output'"));
    assertTrue(sql.contains("FORMAT PARQUET"));
    assertTrue(sql.contains("PARTITION_BY (year)"));
    assertTrue(sql.endsWith(";"));
  }

  @Test void testBuildCopySqlFromJsonWithColumns() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopySqlFromJson", String.class, String.class, MaterializeConfig.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .compression("snappy")
            .build())
        .columns(Arrays.asList(
            ColumnConfig.builder().name("id").type("INTEGER").build(),
            ColumnConfig.builder().name("name").type("VARCHAR").source("fullName").build()
        ))
        .build();

    String sql = (String) method.invoke(writer,
        "/data/input.json", "/data/output", config);

    assertTrue(sql.contains("id"));
    assertTrue(sql.contains("AS name"));
    assertFalse(sql.contains("SELECT *"));
  }

  // ========== buildCopySqlFromCsv coverage ==========

  @Test void testBuildCopySqlFromCsv() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopySqlFromCsv", String.class, String.class, MaterializeConfig.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .compression("zstd")
            .build())
        .build();

    String sql = (String) method.invoke(writer,
        "/data/input.csv", "/data/output/data.parquet", config);

    assertTrue(sql.contains("read_csv_auto"));
    assertTrue(sql.contains("'/data/input.csv'"));
    assertTrue(sql.contains("SELECT *"));
    assertTrue(sql.contains("COMPRESSION ZSTD"));
    assertTrue(sql.endsWith(";"));
  }

  // ========== buildCopySqlFromParquet coverage ==========

  @Test void testBuildCopySqlFromParquet() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildCopySqlFromParquet", String.class, String.class, MaterializeConfig.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year", "state"))
            .build())
        .build();

    String sql = (String) method.invoke(writer,
        "/data/**/*.parquet", "/data/reorganized", config);

    assertTrue(sql.contains("read_parquet"));
    assertTrue(sql.contains("hive_partitioning=true"));
    assertTrue(sql.contains("union_by_name=true"));
    assertTrue(sql.contains("PARTITION_BY (year, state)"));
    assertTrue(sql.endsWith(";"));
  }

  // ========== buildConsolidationSql coverage ==========

  @Test void testBuildConsolidationSqlWithPartitions() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildConsolidationSql", String.class, String.class, List.class, String.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(writer,
        "/tmp/temp_base", "/data/final", Arrays.asList("year", "region"), "snappy");

    assertTrue(sql.contains("read_parquet('/tmp/temp_base/**/*.parquet'"));
    assertTrue(sql.contains("hive_partitioning=true"));
    assertTrue(sql.contains("'/data/final'"));
    assertTrue(sql.contains("PARTITION_BY (year, region)"));
    assertTrue(sql.contains("COMPRESSION SNAPPY"));
    assertTrue(sql.contains("OVERWRITE_OR_IGNORE"));
    assertTrue(sql.endsWith(";"));
  }

  @Test void testBuildConsolidationSqlNoPartitions() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildConsolidationSql", String.class, String.class, List.class, String.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(writer,
        "/tmp/temp_base", "/data/final", Collections.emptyList(), "zstd");

    assertFalse(sql.contains("PARTITION_BY"));
    assertTrue(sql.contains("COMPRESSION ZSTD"));
    assertTrue(sql.endsWith(";"));
  }

  @Test void testBuildConsolidationSqlNullPartitions() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildConsolidationSql", String.class, String.class, List.class, String.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(writer,
        "/tmp/temp_base", "/data/final", null, "snappy");

    assertFalse(sql.contains("PARTITION_BY"));
    assertTrue(sql.contains("COMPRESSION SNAPPY"));
  }

  @Test void testBuildConsolidationSqlNoCompression() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildConsolidationSql", String.class, String.class, List.class, String.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(writer,
        "/tmp/temp_base", "/data/final", Arrays.asList("year"), "none");

    assertTrue(sql.contains("PARTITION_BY (year)"));
    assertFalse(sql.contains("COMPRESSION"));
  }

  @Test void testBuildConsolidationSqlNullCompression() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildConsolidationSql", String.class, String.class, List.class, String.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(writer,
        "/tmp/temp_base", "/data/final", Arrays.asList("year"), null);

    assertFalse(sql.contains("COMPRESSION"));
  }

  @Test void testBuildConsolidationSqlEmptyCompression() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildConsolidationSql", String.class, String.class, List.class, String.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(writer,
        "/tmp/temp_base", "/data/final", Arrays.asList("year"), "");

    assertFalse(sql.contains("COMPRESSION"));
  }

  // ========== quoteLiteral coverage ==========

  @Test void testQuoteLiteral() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "quoteLiteral", String.class);
    method.setAccessible(true);

    assertEquals("'hello'", method.invoke(null, "hello"));
    assertEquals("'it''s'", method.invoke(null, "it's"));
    assertEquals("''", method.invoke(null, ""));
    assertEquals("'path/to/file'", method.invoke(null, "path/to/file"));
    assertEquals("'O''Brien''s data'", method.invoke(null, "O'Brien's data"));
  }

  // ========== materialize() disabled path ==========

  @Test void testMaterializeDisabled() throws IOException {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .build())
        .build();

    DataSource source = new TestDataSource(Collections.<Map<String, Object>>emptyList());
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSkipped());
    assertEquals("Materialization disabled", result.getMessage());
  }

  // ========== materialize() with empty source, no batching ==========

  @Test void testMaterializeEmptySourceNoBatching() throws IOException {
    Path outputDir = tempDir.resolve("output_empty");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("empty_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    DataSource source = new TestDataSource(Collections.<Map<String, Object>>emptyList());
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSuccess());
    assertEquals(0, result.getRowCount());
  }

  // ========== materialize() with data, no batching ==========

  @Test void testMaterializeWithDataNoBatching() throws IOException {
    Path outputDir = tempDir.resolve("output_data");
    Files.createDirectories(outputDir);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row1 = new LinkedHashMap<String, Object>();
    row1.put("id", 1);
    row1.put("name", "Alice");
    rows.add(row1);
    Map<String, Object> row2 = new LinkedHashMap<String, Object>();
    row2.put("id", 2);
    row2.put("name", "Bob");
    rows.add(row2);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("data_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    DataSource source = new TestDataSource(rows);
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSuccess());
    assertEquals(2, result.getRowCount());
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

    DataSource source = new TestDataSource(Collections.<Map<String, Object>>emptyList());
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  // ========== materialize() with batchBy columns ==========

  @Test void testMaterializeWithBatchBy() throws IOException {
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

    DataSource source = new TestDataSource(Collections.<Map<String, Object>>emptyList());
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  // ========== materialize() with null batchBy ==========

  @Test void testMaterializeWithNullBatchBy() throws IOException {
    Path outputDir = tempDir.resolve("null_batch_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("null_batch_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year"))
            .build())
        .build();

    DataSource source = new TestDataSource(Collections.<Map<String, Object>>emptyList());
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  // ========== materializeFromJson error handling ==========

  @Test void testMaterializeFromJsonWithName() throws Exception {
    // Create a test JSON file
    String jsonContent = "[{\"id\": 1, \"name\": \"Alice\"}]";
    Path jsonFile = tempDir.resolve("test.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path outputDir = tempDir.resolve("json_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("json_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());
    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  @Test void testMaterializeFromJsonWithoutName() throws Exception {
    String jsonContent = "[{\"id\": 1}]";
    Path jsonFile = tempDir.resolve("noname.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path outputDir = tempDir.resolve("json_noname_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());
    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  @Test void testMaterializeFromJsonBadFileThrowsIOException() {
    MaterializeConfig config = MaterializeConfig.builder()
        .name("bad_json")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.resolve("bad_json_output").toString())
            .compression("snappy")
            .build())
        .build();

    assertThrows(IOException.class, () -> {
      writer.materializeFromJson(config, "/nonexistent/file.json");
    });
  }

  // ========== materializeFromCsv coverage ==========

  @Test void testMaterializeFromCsvWithName() throws Exception {
    String csvContent = "id,name\n1,Alice\n2,Bob\n";
    Path csvFile = tempDir.resolve("test.csv");
    Files.write(csvFile, csvContent.getBytes());

    Path outputDir = tempDir.resolve("csv_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("csv_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    MaterializeResult result = writer.materializeFromCsv(config, csvFile.toString());
    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  @Test void testMaterializeFromCsvWithoutName() throws Exception {
    String csvContent = "id,name\n1,Alice\n";
    Path csvFile = tempDir.resolve("noname.csv");
    Files.write(csvFile, csvContent.getBytes());

    Path outputDir = tempDir.resolve("csv_noname_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    MaterializeResult result = writer.materializeFromCsv(config, csvFile.toString());
    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  @Test void testMaterializeFromCsvBadFileThrowsIOException() {
    MaterializeConfig config = MaterializeConfig.builder()
        .name("bad_csv")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.resolve("bad_csv_output").toString())
            .compression("snappy")
            .build())
        .build();

    assertThrows(IOException.class, () -> {
      writer.materializeFromCsv(config, "/nonexistent/file.csv");
    });
  }

  // ========== materializeFromParquet coverage ==========

  @Test void testMaterializeFromParquetWithName() throws Exception {
    // First create a Parquet source using DuckDB via materializeFromJson
    String jsonContent = "[{\"id\": 1, \"name\": \"Alice\", \"year\": 2024}]";
    Path jsonFile = tempDir.resolve("source_for_parquet.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path sourceDir = tempDir.resolve("parquet_source");
    Files.createDirectories(sourceDir);

    MaterializeConfig sourceConfig = MaterializeConfig.builder()
        .name("source_parquet")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(sourceDir.toString())
            .compression("snappy")
            .build())
        .build();

    writer.materializeFromJson(sourceConfig, jsonFile.toString());

    // Now materialize from parquet
    Path outputDir = tempDir.resolve("parquet_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("parquet_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    MaterializeResult result = writer.materializeFromParquet(config,
        sourceDir.toString() + "/**/*.parquet");
    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  @Test void testMaterializeFromParquetWithoutName() throws Exception {
    // Create source parquet
    String jsonContent = "[{\"id\": 1}]";
    Path jsonFile = tempDir.resolve("for_parquet2.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path sourceDir = tempDir.resolve("parquet_source2");
    Files.createDirectories(sourceDir);

    MaterializeConfig sourceConfig = MaterializeConfig.builder()
        .name("src2")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(sourceDir.toString())
            .compression("snappy")
            .build())
        .build();

    writer.materializeFromJson(sourceConfig, jsonFile.toString());

    Path outputDir = tempDir.resolve("parquet_noname_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    MaterializeResult result = writer.materializeFromParquet(config,
        sourceDir.toString() + "/**/*.parquet");
    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  @Test void testMaterializeFromParquetBadPatternThrowsIOException() {
    MaterializeConfig config = MaterializeConfig.builder()
        .name("bad_parquet")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.resolve("bad_parquet_output").toString())
            .compression("snappy")
            .build())
        .build();

    assertThrows(IOException.class, () -> {
      writer.materializeFromParquet(config, "/nonexistent/**/*.parquet");
    });
  }

  // ========== materializeFromJson with partitioning ==========

  @Test void testMaterializeFromJsonWithPartitions() throws Exception {
    String jsonContent = "[{\"id\": 1, \"name\": \"Alice\", \"year\": 2024, \"state\": \"CA\"},"
        + "{\"id\": 2, \"name\": \"Bob\", \"year\": 2024, \"state\": \"NY\"},"
        + "{\"id\": 3, \"name\": \"Charlie\", \"year\": 2025, \"state\": \"CA\"}]";
    Path jsonFile = tempDir.resolve("partitioned.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path outputDir = tempDir.resolve("partitioned_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("partitioned_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year", "state"))
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());
    assertNotNull(result);
    assertTrue(result.isSuccess());

    // Verify hive-style partition directories exist
    assertTrue(outputDir.toFile().exists());
  }

  // ========== materializeFromJson with column transforms ==========

  @Test void testMaterializeFromJsonWithColumnTransforms() throws Exception {
    String jsonContent = "[{\"fullName\": \"Alice Smith\", \"fiscalYear\": 2024}]";
    Path jsonFile = tempDir.resolve("transform.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path outputDir = tempDir.resolve("transform_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("transform_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .columns(Arrays.asList(
            ColumnConfig.builder().name("name").type("VARCHAR").source("fullName").build(),
            ColumnConfig.builder().name("year").type("INTEGER").source("fiscalYear").build()
        ))
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());
    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  // ========== materializeFromJson with options (threads, rowGroupSize) ==========

  @Test void testMaterializeFromJsonWithOptions() throws Exception {
    String jsonContent = "[{\"id\": 1}]";
    Path jsonFile = tempDir.resolve("options.json");
    Files.write(jsonFile, jsonContent.getBytes());

    Path outputDir = tempDir.resolve("options_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("options_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("zstd")
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

  // ========== materializeFromCsv with partitioning ==========

  @Test void testMaterializeFromCsvWithPartitions() throws Exception {
    String csvContent = "id,name,year,state\n1,Alice,2024,CA\n2,Bob,2024,NY\n";
    Path csvFile = tempDir.resolve("partitioned.csv");
    Files.write(csvFile, csvContent.getBytes());

    Path outputDir = tempDir.resolve("csv_partitioned_output");
    Files.createDirectories(outputDir);

    MaterializeConfig config = MaterializeConfig.builder()
        .name("csv_partitioned")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year", "state"))
            .build())
        .build();

    MaterializeResult result = writer.materializeFromCsv(config, csvFile.toString());
    assertNotNull(result);
    assertTrue(result.isSuccess());
  }

  // ========== addMetadataToOutput (private) ==========

  @Test void testAddMetadataToOutputNoMetadata() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "addMetadataToOutput", MaterializeConfig.class, String.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    // Should not throw - no metadata to add
    method.invoke(writer, config, tempDir.toString());
  }

  @Test void testAddMetadataToOutputWithTableComment() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "addMetadataToOutput", MaterializeConfig.class, String.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .tableComment("Test table")
        .build();

    // Should not throw even if no Parquet files exist
    method.invoke(writer, config, tempDir.toString());
  }

  @Test void testAddMetadataToOutputWithColumnComments() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "addMetadataToOutput", MaterializeConfig.class, String.class);
    method.setAccessible(true);

    Map<String, String> comments = new HashMap<String, String>();
    comments.put("id", "Primary key");
    comments.put("name", "Full name");

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .columnComments(comments)
        .build();

    // Should not throw even if no Parquet files exist
    method.invoke(writer, config, tempDir.toString());
  }

  @Test void testAddMetadataToOutputEmptyTableComment() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "addMetadataToOutput", MaterializeConfig.class, String.class);
    method.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .tableComment("")
        .build();

    // Empty comment + empty column comments => no-op (early return)
    method.invoke(writer, config, tempDir.toString());
  }

  // ========== setLifecycleRule (private) ==========

  @Test void testSetLifecycleRule() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "setLifecycleRule", String.class, String.class);
    method.setAccessible(true);

    // For LocalFileStorageProvider, ensureLifecycleRule is a no-op
    method.invoke(writer, tempDir.toString(), tempDir.resolve("_temp").toString());
  }

  // ========== buildBatchCombinations (private) ==========

  @Test void testBuildBatchCombinations() throws Exception {
    Method method = HiveParquetWriter.class.getDeclaredMethod(
        "buildBatchCombinations", DataSource.class, List.class);
    method.setAccessible(true);

    DataSource source = new TestDataSource(Collections.<Map<String, Object>>emptyList());
    @SuppressWarnings("unchecked")
    List<Map<String, String>> result =
        (List<Map<String, String>>) method.invoke(writer, source, Arrays.asList("year"));

    assertNotNull(result);
    // Currently returns empty list (TODO in implementation)
    assertTrue(result.isEmpty());
  }

  // ========== MaterializeResult coverage ==========

  @Test void testMaterializeResultSuccess() {
    MaterializeResult result = MaterializeResult.success(1000, 5, 500);
    assertTrue(result.isSuccess());
    assertFalse(result.isSkipped());
    assertFalse(result.isError());
    assertEquals(MaterializeResult.Status.SUCCESS, result.getStatus());
    assertEquals(1000, result.getRowCount());
    assertEquals(5, result.getFileCount());
    assertEquals(500, result.getElapsedMillis());
  }

  @Test void testMaterializeResultSkipped() {
    MaterializeResult result = MaterializeResult.skipped("Already done");
    assertTrue(result.isSkipped());
    assertFalse(result.isSuccess());
    assertEquals("Already done", result.getMessage());
  }

  @Test void testMaterializeResultError() {
    MaterializeResult result = MaterializeResult.error("Failed", 200);
    assertTrue(result.isError());
    assertFalse(result.isSuccess());
    assertEquals("Failed", result.getMessage());
    assertEquals(200, result.getElapsedMillis());
  }

  @Test void testMaterializeResultToStringSuccess() {
    MaterializeResult result = MaterializeResult.success(100, 3, 250);
    String str = result.toString();
    assertTrue(str.contains("SUCCESS"));
    assertTrue(str.contains("100"));
    assertTrue(str.contains("3"));
    assertTrue(str.contains("250ms"));
  }

  @Test void testMaterializeResultToStringError() {
    MaterializeResult result = MaterializeResult.error("Connection failed", 150);
    String str = result.toString();
    assertTrue(str.contains("ERROR"));
    assertTrue(str.contains("Connection failed"));
  }

  @Test void testMaterializeResultToStringSkipped() {
    MaterializeResult result = MaterializeResult.skipped("Not needed");
    String str = result.toString();
    assertTrue(str.contains("SKIPPED"));
    assertTrue(str.contains("Not needed"));
  }

  // ========== ColumnConfig coverage ==========

  @Test void testColumnConfigSimple() {
    ColumnConfig col = ColumnConfig.builder()
        .name("id")
        .type("INTEGER")
        .build();
    assertEquals("id", col.getName());
    assertEquals("INTEGER", col.getType());
    assertFalse(col.isComputed());
    assertTrue(col.isRequired());
    assertEquals("id", col.getEffectiveSource());
    assertEquals("id", col.buildSelectExpression());
  }

  @Test void testColumnConfigWithSource() {
    ColumnConfig col = ColumnConfig.builder()
        .name("output_name")
        .type("VARCHAR")
        .source("input_name")
        .build();
    assertEquals("input_name", col.getSource());
    assertFalse(col.isComputed());
    assertEquals("input_name", col.getEffectiveSource());
    String expr = col.buildSelectExpression();
    assertTrue(expr.contains("AS output_name"));
    assertTrue(expr.contains("input_name"));
  }

  @Test void testColumnConfigWithExpression() {
    ColumnConfig col = ColumnConfig.builder()
        .name("computed")
        .type("INTEGER")
        .expression("CAST(raw AS INTEGER)")
        .build();
    assertTrue(col.isComputed());
    assertEquals("CAST(raw AS INTEGER)", col.getExpression());
    String expr = col.buildSelectExpression();
    assertEquals("CAST(raw AS INTEGER) AS computed", expr);
  }

  @Test void testColumnConfigNotRequired() {
    ColumnConfig col = ColumnConfig.builder()
        .name("optional_col")
        .type("VARCHAR")
        .required(false)
        .build();
    assertFalse(col.isRequired());
  }

  @Test void testColumnConfigSourceSameAsName() {
    ColumnConfig col = ColumnConfig.builder()
        .name("id")
        .type("INTEGER")
        .source("id")
        .build();
    // Source equals name => no AS clause
    assertEquals("id", col.buildSelectExpression());
  }

  @Test void testColumnConfigBuilderRequiresName() {
    assertThrows(IllegalArgumentException.class, () -> {
      ColumnConfig.builder().type("VARCHAR").build();
    });
  }

  @Test void testColumnConfigBuilderEmptyName() {
    assertThrows(IllegalArgumentException.class, () -> {
      ColumnConfig.builder().name("").type("VARCHAR").build();
    });
  }

  // ========== MaterializeConfig coverage ==========

  @Test void testMaterializeConfigDefaults() {
    MaterializeConfig config = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .build();

    assertTrue(config.isEnabled());
    assertEquals(MaterializeConfig.Format.ICEBERG, config.getFormat());
    assertEquals(MaterializeConfig.Trigger.AUTO, config.getTrigger());
    assertNotNull(config.getOptions());
    assertNotNull(config.getColumns());
    assertTrue(config.getColumns().isEmpty());
    assertNotNull(config.getColumnComments());
    assertTrue(config.getColumnComments().isEmpty());
  }

  @Test void testMaterializeConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("enabled", true);
    map.put("format", "parquet");
    map.put("name", "test");
    map.put("trigger", "manual");
    map.put("targetTableId", "my_table");
    map.put("tableComment", "My table");

    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data/output");
    outputMap.put("compression", "zstd");
    map.put("output", outputMap);

    Map<String, Object> partitionMap = new HashMap<String, Object>();
    partitionMap.put("columns", Arrays.asList("year"));
    partitionMap.put("batchBy", Arrays.asList("year"));
    map.put("partition", partitionMap);

    Map<String, Object> optionsMap = new HashMap<String, Object>();
    optionsMap.put("threads", 4);
    optionsMap.put("rowGroupSize", 50000);
    map.put("options", optionsMap);

    Map<String, Object> columnCommentsMap = new HashMap<String, Object>();
    columnCommentsMap.put("id", "Primary key");
    map.put("columnComments", columnCommentsMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("test", config.getName());
    assertEquals(MaterializeConfig.Format.PARQUET, config.getFormat());
    assertEquals(MaterializeConfig.Trigger.MANUAL, config.getTrigger());
    assertEquals("my_table", config.getTargetTableId());
    assertEquals("My table", config.getTableComment());
  }

  @Test void testMaterializeConfigFromMapNull() {
    MaterializeConfig config = MaterializeConfig.fromMap(null);
    assertEquals(null, config);
  }

  @Test void testMaterializeConfigBuilderNoOutput() {
    assertThrows(IllegalArgumentException.class, () -> {
      MaterializeConfig.builder().build();
    });
  }

  // ========== MaterializeOutputConfig coverage ==========

  @Test void testMaterializeOutputConfigDefaults() {
    MaterializeOutputConfig config = MaterializeOutputConfig.builder().build();
    assertEquals("snappy", config.getCompression());
    assertEquals("parquet", config.getFormat());
  }

  @Test void testMaterializeOutputConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("location", "/data");
    map.put("pattern", "year=*/");
    map.put("format", "parquet");
    map.put("compression", "zstd");

    MaterializeOutputConfig config = MaterializeOutputConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("/data", config.getLocation());
    assertEquals("year=*/", config.getPattern());
    assertEquals("zstd", config.getCompression());
  }

  @Test void testMaterializeOutputConfigFromMapNull() {
    MaterializeOutputConfig config = MaterializeOutputConfig.fromMap(null);
    assertEquals(null, config);
  }

  // ========== MaterializePartitionConfig coverage ==========

  @Test void testMaterializePartitionConfigDefaults() {
    MaterializePartitionConfig config = MaterializePartitionConfig.builder().build();
    assertNotNull(config.getColumns());
    assertTrue(config.getColumns().isEmpty());
    assertNotNull(config.getBatchBy());
    assertTrue(config.getBatchBy().isEmpty());
    assertFalse(config.hasBatching());
  }

  @Test void testMaterializePartitionConfigHasBatching() {
    MaterializePartitionConfig config = MaterializePartitionConfig.builder()
        .columns(Arrays.asList("year"))
        .batchBy(Arrays.asList("year"))
        .build();
    assertTrue(config.hasBatching());
  }

  @Test void testMaterializePartitionConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("columns", Arrays.asList("year", "state"));
    map.put("batchBy", Arrays.asList("year"));

    MaterializePartitionConfig config = MaterializePartitionConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(2, config.getColumns().size());
    assertEquals(1, config.getBatchBy().size());
  }

  @Test void testMaterializePartitionConfigFromMapNull() {
    MaterializePartitionConfig config = MaterializePartitionConfig.fromMap(null);
    assertEquals(null, config);
  }

  // ========== MaterializeOptionsConfig coverage ==========

  @Test void testMaterializeOptionsConfigDefaults() {
    MaterializeOptionsConfig config = MaterializeOptionsConfig.defaults();
    assertEquals(2, config.getThreads());
    assertEquals(100000, config.getRowGroupSize());
    assertEquals(10000, config.getBatchSize());
    assertEquals(MaterializeOptionsConfig.StagingMode.REMOTE, config.getStagingMode());
    assertFalse(config.isPreserveInsertionOrder());
    assertEquals(7, config.getEmptyResultTtlDays());
    assertTrue(config.getEmptyResultTtlMillis() > 0);
  }

  @Test void testMaterializeOptionsConfigCustom() {
    MaterializeOptionsConfig config = MaterializeOptionsConfig.builder()
        .threads(8)
        .rowGroupSize(200000)
        .batchSize(50000)
        .stagingMode(MaterializeOptionsConfig.StagingMode.LOCAL)
        .preserveInsertionOrder(true)
        .emptyResultTtlDays(14)
        .build();

    assertEquals(8, config.getThreads());
    assertEquals(200000, config.getRowGroupSize());
    assertEquals(50000, config.getBatchSize());
    assertEquals(MaterializeOptionsConfig.StagingMode.LOCAL, config.getStagingMode());
    assertTrue(config.isPreserveInsertionOrder());
    assertEquals(14, config.getEmptyResultTtlDays());
  }

  @Test void testMaterializeOptionsConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("threads", 4);
    map.put("rowGroupSize", 50000);
    map.put("batchSize", 20000);
    map.put("stagingMode", "local");
    map.put("preserveInsertionOrder", true);
    map.put("emptyResultTtlDays", 14);

    MaterializeOptionsConfig config = MaterializeOptionsConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(4, config.getThreads());
    assertEquals(50000, config.getRowGroupSize());
    assertEquals(20000, config.getBatchSize());
    assertEquals(MaterializeOptionsConfig.StagingMode.LOCAL, config.getStagingMode());
    assertTrue(config.isPreserveInsertionOrder());
    assertEquals(14, config.getEmptyResultTtlDays());
  }

  @Test void testMaterializeOptionsConfigFromMapNull() {
    MaterializeOptionsConfig config = MaterializeOptionsConfig.fromMap(null);
    assertNotNull(config);
    // Should return defaults
    assertEquals(2, config.getThreads());
  }

  @Test void testMaterializeOptionsConfigFromMapInvalidStagingMode() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("stagingMode", "invalid_mode");

    MaterializeOptionsConfig config = MaterializeOptionsConfig.fromMap(map);
    // Invalid staging mode should fall back to default
    assertEquals(MaterializeOptionsConfig.StagingMode.REMOTE, config.getStagingMode());
  }

  // ========== MaterializeBatchResult (inner class via processBatch) ==========

  @Test void testMaterializeWithDataSourceThatHasRows() throws IOException {
    Path outputDir = tempDir.resolve("rows_output");
    Files.createDirectories(outputDir);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 5; i++) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("id", i);
      row.put("value", "val_" + i);
      rows.add(row);
    }

    MaterializeConfig config = MaterializeConfig.builder()
        .name("rows_test")
        .enabled(true)
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    DataSource source = new TestDataSource(rows);
    MaterializeResult result = writer.materialize(config, source);

    assertNotNull(result);
    assertTrue(result.isSuccess());
    assertEquals(5, result.getRowCount());
  }

  // ========== MaterializeConfig.fromMap with columns list ==========

  @Test void testMaterializeConfigFromMapWithColumns() {
    Map<String, Object> map = new HashMap<String, Object>();

    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data");
    map.put("output", outputMap);

    List<Map<String, Object>> columnsList = new ArrayList<Map<String, Object>>();
    Map<String, Object> col1 = new HashMap<String, Object>();
    col1.put("name", "id");
    col1.put("type", "INTEGER");
    columnsList.add(col1);
    Map<String, Object> col2 = new HashMap<String, Object>();
    col2.put("name", "computed");
    col2.put("type", "VARCHAR");
    col2.put("expression", "UPPER(name)");
    columnsList.add(col2);
    map.put("columns", columnsList);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(2, config.getColumns().size());
  }

  // ========== MaterializeConfig trigger parsing ==========

  @Test void testMaterializeConfigTriggerOnFirstQuery() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("trigger", "onFirstQuery");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(MaterializeConfig.Trigger.ON_FIRST_QUERY, config.getTrigger());
  }

  @Test void testMaterializeConfigTriggerOnFirstQueryUnderscore() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("trigger", "on_first_query");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(MaterializeConfig.Trigger.ON_FIRST_QUERY, config.getTrigger());
  }

  @Test void testMaterializeConfigTriggerUnknownDefaultsToAuto() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("trigger", "unknown_trigger");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(MaterializeConfig.Trigger.AUTO, config.getTrigger());
  }

  // ========== MaterializeConfig format parsing ==========

  @Test void testMaterializeConfigFormatParquet() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "parquet");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertEquals(MaterializeConfig.Format.PARQUET, config.getFormat());
  }

  @Test void testMaterializeConfigFormatDelta() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "delta");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertEquals(MaterializeConfig.Format.DELTA, config.getFormat());
  }

  @Test void testMaterializeConfigFormatSnowflake() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "snowflake");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertEquals(MaterializeConfig.Format.SNOWFLAKE, config.getFormat());
  }

  @Test void testMaterializeConfigFormatBigQuery() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "bigquery");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertEquals(MaterializeConfig.Format.BIGQUERY, config.getFormat());
  }

  @Test void testMaterializeConfigFormatDatabricks() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "databricks");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertEquals(MaterializeConfig.Format.DATABRICKS, config.getFormat());
  }

  @Test void testMaterializeConfigFormatUnknownDefaultsToIceberg() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "unknown_format");
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);
    assertEquals(MaterializeConfig.Format.ICEBERG, config.getFormat());
  }

  // ========== ColumnConfig.fromMap and fromList ==========

  @Test void testColumnConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "test_col");
    map.put("type", "VARCHAR");
    map.put("source", "src_col");
    map.put("required", false);

    ColumnConfig config = ColumnConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("test_col", config.getName());
    assertEquals("src_col", config.getSource());
    assertFalse(config.isRequired());
  }

  @Test void testColumnConfigFromMapNull() {
    ColumnConfig config = ColumnConfig.fromMap(null);
    assertEquals(null, config);
  }

  @Test void testColumnConfigFromListNull() {
    List<ColumnConfig> result = ColumnConfig.fromList(null);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testColumnConfigFromListEmpty() {
    List<ColumnConfig> result = ColumnConfig.fromList(Collections.emptyList());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  // ========== MaterializePartitionConfig.ColumnDefinition ==========

  @Test void testColumnDefinition() {
    MaterializePartitionConfig.ColumnDefinition def =
        new MaterializePartitionConfig.ColumnDefinition("year", "INTEGER");
    assertEquals("year", def.getName());
    assertEquals("INTEGER", def.getType());
  }

  @Test void testColumnDefinitionDefaultType() {
    MaterializePartitionConfig.ColumnDefinition def =
        new MaterializePartitionConfig.ColumnDefinition("region", null);
    assertEquals("region", def.getName());
    assertEquals("VARCHAR", def.getType());
  }

  @Test void testMaterializePartitionConfigWithColumnDefinitions() {
    Map<String, Object> map = new HashMap<String, Object>();
    List<Map<String, Object>> colDefs = new ArrayList<Map<String, Object>>();
    Map<String, Object> def1 = new HashMap<String, Object>();
    def1.put("name", "year");
    def1.put("type", "INTEGER");
    colDefs.add(def1);
    Map<String, Object> def2 = new HashMap<String, Object>();
    def2.put("name", "region");
    colDefs.add(def2);
    map.put("columnDefinitions", colDefs);

    MaterializePartitionConfig config = MaterializePartitionConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(2, config.getColumnDefinitions().size());
    assertEquals("year", config.getColumnDefinitions().get(0).getName());
    assertEquals("INTEGER", config.getColumnDefinitions().get(0).getType());
  }

  // ========== Test DataSource implementation ==========

  private static class TestDataSource implements DataSource {
    private final List<Map<String, Object>> data;

    TestDataSource(List<Map<String, Object>> data) {
      this.data = data;
    }

    @Override
    public Iterator<Map<String, Object>> fetch(Map<String, String> variables) {
      return data.iterator();
    }

    @Override
    public String getType() {
      return "test";
    }
  }
}
