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
package org.apache.calcite.adapter.file.storage;

import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive coverage tests for {@link StorageProvider} interface default methods.
 *
 * <p>Covers the complex default methods including:
 * <ul>
 *   <li>{@code getStagingDirectory} - staging directory provisioning</li>
 *   <li>{@code writeAvroParquet(path, columns, data, recordType, recordName)} - column-based Parquet writing</li>
 *   <li>{@code writeAvroParquet(path, schema, records, recordType)} - schema-based Parquet writing</li>
 *   <li>{@code readParquet(path)} - Parquet reading</li>
 *   <li>{@code appendParquet(path, newRecords, schema)} - Parquet appending</li>
 *   <li>{@code deleteBatch} - batch delete with mixed outcomes</li>
 *   <li>{@code hasChanged} - additional edge cases</li>
 * </ul>
 */
@Tag("unit")
public class StorageProviderDefaultMethodsCoverageTest {

  @TempDir
  Path tempDir;

  // ================================================================
  // Local file-backed StorageProvider for realistic Parquet testing
  // ================================================================

  /**
   * A StorageProvider backed by a local temp directory so that
   * writeFile / openInputStream / exists / createDirectories all
   * work against real files, enabling full Parquet round-trip testing.
   */
  static class LocalTempStorageProvider implements StorageProvider {

    private final Path root;

    LocalTempStorageProvider(Path root) {
      this.root = root;
    }

    @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
      List<FileEntry> entries = new ArrayList<>();
      Path dir = root.resolve(path);
      if (Files.isDirectory(dir)) {
        Files.list(dir).forEach(p -> entries.add(
            new FileEntry(p.toString(), p.getFileName().toString(),
                Files.isDirectory(p), 0L, 0L)));
      }
      return entries;
    }

    @Override public FileMetadata getMetadata(String path) throws IOException {
      Path file = root.resolve(path);
      if (!Files.exists(file)) {
        throw new IOException("Not found: " + path);
      }
      return new FileMetadata(path, Files.size(file),
          Files.getLastModifiedTime(file).toMillis(), null, null);
    }

    @Override public InputStream openInputStream(String path) throws IOException {
      return Files.newInputStream(root.resolve(path));
    }

    @Override public Reader openReader(String path) throws IOException {
      return Files.newBufferedReader(root.resolve(path), StandardCharsets.UTF_8);
    }

    @Override public boolean exists(String path) throws IOException {
      return Files.exists(root.resolve(path));
    }

    @Override public boolean isDirectory(String path) throws IOException {
      return Files.isDirectory(root.resolve(path));
    }

    @Override public String getStorageType() {
      return "local-test";
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }

    @Override public void writeFile(String path, byte[] content) throws IOException {
      Path file = root.resolve(path);
      Files.createDirectories(file.getParent());
      Files.write(file, content);
    }

    @Override public void writeFile(String path, InputStream content) throws IOException {
      Path file = root.resolve(path);
      Files.createDirectories(file.getParent());
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int len;
      while ((len = content.read(buffer)) != -1) {
        baos.write(buffer, 0, len);
      }
      Files.write(file, baos.toByteArray());
    }

    @Override public void createDirectories(String path) throws IOException {
      Files.createDirectories(root.resolve(path));
    }

    @Override public boolean delete(String path) throws IOException {
      Path file = root.resolve(path);
      return Files.deleteIfExists(file);
    }

    @Override public void copyFile(String source, String destination) throws IOException {
      Path src = root.resolve(source);
      Path dst = root.resolve(destination);
      Files.createDirectories(dst.getParent());
      Files.copy(src, dst);
    }
  }

  /**
   * Minimal stub that throws on all abstract methods, used for testing
   * default methods that do NOT delegate to abstract methods.
   */
  static class MinimalStubProvider implements StorageProvider {
    private final String storageType;
    private FileMetadata metadataResult;
    private boolean throwOnGetMetadata;

    MinimalStubProvider(String storageType) {
      this.storageType = storageType;
    }

    void setMetadataResult(FileMetadata result) {
      this.metadataResult = result;
    }

    void setThrowOnGetMetadata(boolean throwOn) {
      this.throwOnGetMetadata = throwOn;
    }

    @Override public List<FileEntry> listFiles(String path, boolean recursive) {
      return Collections.emptyList();
    }

    @Override public FileMetadata getMetadata(String path) throws IOException {
      if (throwOnGetMetadata) {
        throw new IOException("Simulated failure");
      }
      if (metadataResult != null) {
        return metadataResult;
      }
      return new FileMetadata(path, 0L, 0L, null, null);
    }

    @Override public InputStream openInputStream(String path) {
      return new ByteArrayInputStream(new byte[0]);
    }

    @Override public Reader openReader(String path) {
      return new java.io.StringReader("");
    }

    @Override public boolean exists(String path) {
      return false;
    }

    @Override public boolean isDirectory(String path) {
      return false;
    }

    @Override public String getStorageType() {
      return storageType;
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }

  // ================================================================
  // getStagingDirectory tests
  // ================================================================

  @Nested
  @DisplayName("getStagingDirectory")
  class GetStagingDirectoryTests {

    @Test
    @DisplayName("returns staging path based on system temp dir and purpose")
    void testGetStagingDirectoryReturnsStagingPath() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      // Override createDirectories to capture the call without actual system temp dir access
      // Since getStagingDirectory uses System.getProperty("java.io.tmpdir"),
      // we test that it returns the expected format
      String tempDirProp = System.getProperty("java.io.tmpdir");
      String result = provider.getStagingDirectory("iceberg");
      String expectedPath = tempDirProp + "/.staging/iceberg";
      assertEquals(expectedPath, result);
    }

    @Test
    @DisplayName("getStagingDirectory throws when createDirectories fails")
    void testGetStagingDirectoryFailsWhenCreateDirFails() {
      MinimalStubProvider provider = new MinimalStubProvider("test");
      // createDirectories default throws UnsupportedOperationException
      assertThrows(UnsupportedOperationException.class,
          () -> provider.getStagingDirectory("etl"));
    }
  }

  // ================================================================
  // writeAvroParquet (column-based) tests -- all type branches
  // ================================================================

  @Nested
  @DisplayName("writeAvroParquet with columns")
  class WriteAvroParquetColumnTests {

    @Test
    @DisplayName("writes string columns - nullable and non-nullable")
    void testWriteStringColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("name", "string", false, "Name field"));
      columns.add(new PartitionedTableConfig.TableColumn("desc", "string", true, "Description"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("name", "Alice");
      row.put("desc", "Developer");
      data.add(row);

      Map<String, Object> row2 = new LinkedHashMap<>();
      row2.put("name", "Bob");
      row2.put("desc", null);
      data.add(row2);

      String path = "out/strings.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "StringRecord");

      assertTrue(provider.exists("out/strings.parquet"));

      // Round-trip read
      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(2, result.size());
      assertEquals("Alice", String.valueOf(result.get(0).get("name")));
    }

    @Test
    @DisplayName("writes int/integer columns - nullable and non-nullable")
    void testWriteIntColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("count", "int", false, "Count"));
      columns.add(new PartitionedTableConfig.TableColumn("opt_count", "integer", true, "Optional"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("count", 42);
      row.put("opt_count", null);
      data.add(row);

      String path = "out/ints.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "IntRecord");
      assertTrue(provider.exists(path));

      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(1, result.size());
      assertEquals(42, result.get(0).get("count"));
    }

    @Test
    @DisplayName("writes long columns - nullable and non-nullable")
    void testWriteLongColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("ts", "long", false, "Timestamp"));
      columns.add(new PartitionedTableConfig.TableColumn("opt_ts", "long", true, "Optional ts"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("ts", 1700000000L);
      row.put("opt_ts", 1700000001L);
      data.add(row);

      String path = "out/longs.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "LongRecord");
      assertTrue(provider.exists(path));

      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(1, result.size());
      assertEquals(1700000000L, result.get(0).get("ts"));
    }

    @Test
    @DisplayName("writes double columns - nullable and non-nullable")
    void testWriteDoubleColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("price", "double", false, "Price"));
      columns.add(new PartitionedTableConfig.TableColumn("opt_price", "double", true, "Optional"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("price", 99.95);
      row.put("opt_price", null);
      data.add(row);

      String path = "out/doubles.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "DoubleRecord");
      assertTrue(provider.exists(path));

      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(1, result.size());
      assertEquals(99.95, (Double) result.get(0).get("price"), 0.001);
    }

    @Test
    @DisplayName("writes boolean columns - nullable and non-nullable")
    void testWriteBooleanColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("active", "boolean", false, "Active"));
      columns.add(new PartitionedTableConfig.TableColumn("opt_flag", "boolean", true, "Optional"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("active", true);
      row.put("opt_flag", false);
      data.add(row);

      String path = "out/booleans.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "BoolRecord");
      assertTrue(provider.exists(path));

      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(1, result.size());
      assertEquals(true, result.get(0).get("active"));
      assertEquals(false, result.get(0).get("opt_flag"));
    }

    @Test
    @DisplayName("writes array<double> columns - nullable and non-nullable")
    void testWriteArrayDoubleColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("embedding", "array<double>",
          false, "Embedding vector"));
      columns.add(new PartitionedTableConfig.TableColumn("opt_vec", "array<double>",
          true, "Optional vec"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("embedding", Arrays.asList(1.0, 2.0, 3.0));
      row.put("opt_vec", null);
      data.add(row);

      String path = "out/array_doubles.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "ArrayDoubleRecord");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("writes array<float> columns - non-nullable")
    void testWriteArrayFloatColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("vec", "array<float>",
          false, "Float vector"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("vec", Arrays.asList(1.0f, 2.0f));
      data.add(row);

      String path = "out/array_floats.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "ArrayFloatRecord");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("writes array<int> columns - non-nullable")
    void testWriteArrayIntColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("ids", "array<int>",
          false, "Int array"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("ids", Arrays.asList(1, 2, 3));
      data.add(row);

      String path = "out/array_ints.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "ArrayIntRecord");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("writes array<integer> columns - non-nullable")
    void testWriteArrayIntegerColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("ids", "array<integer>",
          false, "Integer array"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("ids", Arrays.asList(10, 20));
      data.add(row);

      String path = "out/array_integers.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "ArrayIntegerRecord");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("writes array<long> columns - non-nullable")
    void testWriteArrayLongColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("timestamps", "array<long>",
          false, "Long array"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("timestamps", Arrays.asList(100L, 200L));
      data.add(row);

      String path = "out/array_longs.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "ArrayLongRecord");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("writes array<string> columns - non-nullable")
    void testWriteArrayStringColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("tags", "array<string>",
          false, "String array"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("tags", Arrays.asList("a", "b", "c"));
      data.add(row);

      String path = "out/array_strings.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "ArrayStringRecord");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("writes array<double> column - nullable")
    void testWriteArrayDoubleNullable() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("vec", "array<double>",
          true, "Nullable double array"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("vec", Arrays.asList(1.1, 2.2));
      data.add(row);

      String path = "out/array_double_nullable.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "NullableArrayRecord");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("throws on unsupported array element type")
    void testUnsupportedArrayElementType() {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("bad", "array<bytes>",
          false, "Bad type"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("bad", Arrays.asList(1));
      data.add(row);

      assertThrows(IllegalArgumentException.class,
          () -> provider.writeAvroParquet("out/bad.parquet", columns, data, "test", "BadRecord"));
    }

    @Test
    @DisplayName("throws on unsupported scalar column type")
    void testUnsupportedScalarColumnType() {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("weird", "timestamp",
          false, "Unsupported type"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("weird", "value");
      data.add(row);

      assertThrows(IllegalArgumentException.class,
          () -> provider.writeAvroParquet("out/bad.parquet", columns, data, "test", "BadRecord"));
    }

    @Test
    @DisplayName("skips computed columns in schema and record generation")
    void testSkipsComputedColumns() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("name", "string", false, "Name"));
      // Computed column has an expression -- should be skipped
      columns.add(new PartitionedTableConfig.TableColumn("computed_year", "int",
          false, "Year extracted", "EXTRACT(YEAR FROM date)"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("name", "Alice");
      row.put("computed_year", 2024);
      data.add(row);

      String path = "out/skip_computed.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "SkipComputed");
      assertTrue(provider.exists(path));

      // Read back - only "name" column should be present
      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(1, result.size());
      assertTrue(result.get(0).containsKey("name"));
      assertFalse(result.get(0).containsKey("computed_year"));
    }

    @Test
    @DisplayName("logs schema mismatch when data has extra fields not in schema")
    void testSchemaExtraFieldsInData() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("name", "string", false, "Name"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("name", "Alice");
      row.put("extra_field", "should be dropped");
      data.add(row);

      String path = "out/extra_fields.parquet";
      // This should succeed but log a warning about extra_field
      provider.writeAvroParquet(path, columns, data, "test", "ExtraFieldRecord");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("logs schema mismatch when data is missing schema columns")
    void testSchemaMissingFieldsInData() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("name", "string", true, "Name"));
      columns.add(new PartitionedTableConfig.TableColumn("age", "int", true, "Age"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("name", "Alice");
      // "age" is missing from data
      data.add(row);

      String path = "out/missing_fields.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "MissingFieldRecord");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("handles empty data records list")
    void testEmptyDataRecords() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("name", "string", false, "Name"));

      List<Map<String, Object>> data = new ArrayList<>();

      String path = "out/empty_data.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "EmptyRecord");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("writes all scalar types in one schema to cover all branches")
    void testAllScalarTypesInOneRecord() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("s", "string", false, "string"));
      columns.add(new PartitionedTableConfig.TableColumn("s_n", "string", true, "nullable string"));
      columns.add(new PartitionedTableConfig.TableColumn("i", "int", false, "int"));
      columns.add(new PartitionedTableConfig.TableColumn("i_n", "int", true, "nullable int"));
      columns.add(new PartitionedTableConfig.TableColumn("l", "long", false, "long"));
      columns.add(new PartitionedTableConfig.TableColumn("l_n", "long", true, "nullable long"));
      columns.add(new PartitionedTableConfig.TableColumn("d", "double", false, "double"));
      columns.add(new PartitionedTableConfig.TableColumn("d_n", "double", true, "nullable double"));
      columns.add(new PartitionedTableConfig.TableColumn("b", "boolean", false, "boolean"));
      columns.add(new PartitionedTableConfig.TableColumn("b_n", "boolean", true, "nullable bool"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("s", "hello");
      row.put("s_n", null);
      row.put("i", 42);
      row.put("i_n", null);
      row.put("l", 100L);
      row.put("l_n", null);
      row.put("d", 3.14);
      row.put("d_n", null);
      row.put("b", true);
      row.put("b_n", null);
      data.add(row);

      String path = "out/all_types.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "AllTypesRecord");
      assertTrue(provider.exists(path));

      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(1, result.size());
      assertEquals("hello", String.valueOf(result.get(0).get("s")));
      assertNull(result.get(0).get("s_n"));
      assertEquals(42, result.get(0).get("i"));
      assertEquals(100L, result.get(0).get("l"));
      assertEquals(true, result.get(0).get("b"));
    }

    @Test
    @DisplayName("writes multiple records to verify loop iteration")
    void testMultipleRecords() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("id", "int", false, "ID"));
      columns.add(new PartitionedTableConfig.TableColumn("value", "string", true, "Value"));

      List<Map<String, Object>> data = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", i);
        row.put("value", "val_" + i);
        data.add(row);
      }

      String path = "out/multi.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "MultiRecord");
      assertTrue(provider.exists(path));

      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(5, result.size());
    }

    @Test
    @DisplayName("handles both extra and missing fields simultaneously")
    void testExtraAndMissingFieldsTogether() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("name", "string", true, "Name"));
      columns.add(new PartitionedTableConfig.TableColumn("age", "int", true, "Age"));
      // Add a computed column to verify it is skipped in missing-field detection
      columns.add(new PartitionedTableConfig.TableColumn("computed", "int",
          false, "Computed", "1 + 1"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("name", "Alice");
      // "age" is missing from data -> missingFieldsInData
      row.put("bonus_field", "extra"); // not in schema -> extraFieldsInData
      data.add(row);

      String path = "out/extra_and_missing.parquet";
      provider.writeAvroParquet(path, columns, data, "test", "ExtraMissingRecord");
      assertTrue(provider.exists(path));
    }
  }

  // ================================================================
  // writeAvroParquet (schema-based) tests
  // ================================================================

  @Nested
  @DisplayName("writeAvroParquet with schema")
  class WriteAvroParquetSchemaTests {

    @Test
    @DisplayName("writes empty records list (creates schema-only file)")
    void testWriteEmptyRecordsList() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      org.apache.avro.Schema schema = org.apache.avro.SchemaBuilder
          .record("EmptyTest").fields()
          .name("id").type().intType().noDefault()
          .endRecord();

      List<org.apache.avro.generic.GenericRecord> records = new ArrayList<>();

      String path = "out/empty_schema.parquet";
      provider.writeAvroParquet(path, schema, records, "empty");
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("writes multiple records with schema")
    void testWriteMultipleRecordsWithSchema() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      org.apache.avro.Schema schema = org.apache.avro.SchemaBuilder
          .record("TestRecord").fields()
          .name("name").type().stringType().noDefault()
          .name("value").type().intType().noDefault()
          .endRecord();

      List<org.apache.avro.generic.GenericRecord> records = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        org.apache.avro.generic.GenericRecord record =
            new org.apache.avro.generic.GenericData.Record(schema);
        record.put("name", "item_" + i);
        record.put("value", i * 10);
        records.add(record);
      }

      String path = "out/schema_multi.parquet";
      provider.writeAvroParquet(path, schema, records, "facts");
      assertTrue(provider.exists(path));

      // Verify round-trip
      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(3, result.size());
    }
  }

  // ================================================================
  // readParquet tests
  // ================================================================

  @Nested
  @DisplayName("readParquet")
  class ReadParquetTests {

    @Test
    @DisplayName("reads back written records correctly")
    void testReadParquetRoundTrip() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);
      List<PartitionedTableConfig.TableColumn> columns = new ArrayList<>();
      columns.add(new PartitionedTableConfig.TableColumn("city", "string", false, "City"));
      columns.add(new PartitionedTableConfig.TableColumn("pop", "long", false, "Population"));

      List<Map<String, Object>> data = new ArrayList<>();
      Map<String, Object> row1 = new LinkedHashMap<>();
      row1.put("city", "NYC");
      row1.put("pop", 8000000L);
      data.add(row1);

      Map<String, Object> row2 = new LinkedHashMap<>();
      row2.put("city", "LA");
      row2.put("pop", 4000000L);
      data.add(row2);

      String path = "data/cities.parquet";
      provider.writeAvroParquet(path, columns, data, "cities", "CityRecord");

      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(2, result.size());
      assertEquals("NYC", String.valueOf(result.get(0).get("city")));
      assertEquals(8000000L, result.get(0).get("pop"));
      assertEquals("LA", String.valueOf(result.get(1).get("city")));
    }

    @Test
    @DisplayName("reads parquet with array<double> column using GenericData.Array")
    void testReadParquetWithDoubleArray() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);

      // Write a parquet file with a double array column using GenericData.Array
      // so the readParquet method's GenericData.Array branch is exercised
      org.apache.avro.Schema arraySchema = org.apache.avro.Schema.createArray(
          org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE));
      org.apache.avro.Schema schema = org.apache.avro.SchemaBuilder
          .record("VecRecord").fields()
          .name("id").type().stringType().noDefault()
          .name("vec").type(arraySchema).noDefault()
          .endRecord();

      // Use GenericData.Array explicitly to ensure it comes back as GenericData.Array
      org.apache.avro.generic.GenericData.Array<Double> avroArray =
          new org.apache.avro.generic.GenericData.Array<>(arraySchema,
              Arrays.asList(1.0, 2.0, 3.0));

      List<org.apache.avro.generic.GenericRecord> records = new ArrayList<>();
      org.apache.avro.generic.GenericRecord rec =
          new org.apache.avro.generic.GenericData.Record(schema);
      rec.put("id", "v1");
      rec.put("vec", avroArray);
      records.add(rec);

      String path = "data/vectors.parquet";
      provider.writeAvroParquet(path, schema, records, "vectors");

      // Read back
      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(1, result.size());
      assertEquals("v1", String.valueOf(result.get(0).get("id")));
      // The vec value comes back -- it should be processed by readParquet
      Object vecValue = result.get(0).get("vec");
      assertNotNull(vecValue);
      // Depending on Parquet reader implementation, it may be double[] or List
      if (vecValue instanceof double[]) {
        double[] vec = (double[]) vecValue;
        assertEquals(3, vec.length);
        assertEquals(1.0, vec[0], 0.001);
      } else {
        // If reader returns List/ArrayList, verify the contents
        assertTrue(vecValue instanceof List, "Expected List or double[] but got: " + vecValue.getClass());
        @SuppressWarnings("unchecked")
        List<Double> vecList = (List<Double>) vecValue;
        assertEquals(3, vecList.size());
        assertEquals(1.0, vecList.get(0), 0.001);
      }
    }
  }

  // ================================================================
  // appendParquet tests
  // ================================================================

  @Nested
  @DisplayName("appendParquet")
  class AppendParquetTests {

    @Test
    @DisplayName("creates new file when no existing file")
    void testAppendToNewFile() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);

      List<Map<String, Object>> newRecords = new ArrayList<>();
      Map<String, Object> rec = new LinkedHashMap<>();
      rec.put("key", "k1");
      rec.put("val", 100L);
      newRecords.add(rec);

      List<Map<String, Object>> schema = new ArrayList<>();
      Map<String, Object> col1 = new HashMap<>();
      col1.put("name", "key");
      col1.put("type", "string");
      schema.add(col1);
      Map<String, Object> col2 = new HashMap<>();
      col2.put("name", "val");
      col2.put("type", "long");
      schema.add(col2);

      String path = "cache/new.parquet";
      provider.appendParquet(path, newRecords, schema);

      assertTrue(provider.exists(path));
      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(1, result.size());
    }

    @Test
    @DisplayName("appends to existing file merging records")
    void testAppendToExistingFile() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);

      List<Map<String, Object>> schema = new ArrayList<>();
      Map<String, Object> col1 = new HashMap<>();
      col1.put("name", "key");
      col1.put("type", "string");
      schema.add(col1);
      Map<String, Object> col2 = new HashMap<>();
      col2.put("name", "count");
      col2.put("type", "int");
      schema.add(col2);

      // Write initial records
      List<Map<String, Object>> initialRecords = new ArrayList<>();
      Map<String, Object> rec1 = new LinkedHashMap<>();
      rec1.put("key", "a");
      rec1.put("count", 1);
      initialRecords.add(rec1);

      String path = "cache/append.parquet";
      provider.appendParquet(path, initialRecords, schema);

      // Append more
      List<Map<String, Object>> moreRecords = new ArrayList<>();
      Map<String, Object> rec2 = new LinkedHashMap<>();
      rec2.put("key", "b");
      rec2.put("count", 2);
      moreRecords.add(rec2);

      provider.appendParquet(path, moreRecords, schema);

      // Should have both records
      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(2, result.size());
    }

    @Test
    @DisplayName("appendParquet with all scalar types in schema definition")
    void testAppendParquetAllTypes() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);

      List<Map<String, Object>> schema = new ArrayList<>();
      schema.add(createSchemaCol("s", "string"));
      schema.add(createSchemaCol("i", "int"));
      schema.add(createSchemaCol("i2", "integer"));
      schema.add(createSchemaCol("l", "long"));
      schema.add(createSchemaCol("d", "double"));

      List<Map<String, Object>> records = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("s", "test");
      row.put("i", 1);
      row.put("i2", 2);
      row.put("l", 100L);
      row.put("d", 3.14);
      records.add(row);

      String path = "cache/all_types.parquet";
      provider.appendParquet(path, records, schema);

      List<Map<String, Object>> result = provider.readParquet(path);
      assertEquals(1, result.size());
    }

    @Test
    @DisplayName("appendParquet with array<double> type and double[] conversion")
    void testAppendParquetWithDoubleArray() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);

      List<Map<String, Object>> schema = new ArrayList<>();
      schema.add(createSchemaCol("id", "string"));
      schema.add(createSchemaCol("vec", "array<double>"));

      List<Map<String, Object>> records = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("id", "r1");
      row.put("vec", new double[]{1.0, 2.0, 3.0}); // double[] should be converted to List
      records.add(row);

      String path = "cache/array_append.parquet";
      provider.appendParquet(path, records, schema);
      assertTrue(provider.exists(path));
    }

    @Test
    @DisplayName("appendParquet throws on unsupported array element type")
    void testAppendParquetUnsupportedArrayType() {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);

      List<Map<String, Object>> schema = new ArrayList<>();
      schema.add(createSchemaCol("bad", "array<bytes>"));

      List<Map<String, Object>> records = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("bad", "value");
      records.add(row);

      assertThrows(IllegalArgumentException.class,
          () -> provider.appendParquet("cache/bad.parquet", records, schema));
    }

    @Test
    @DisplayName("appendParquet throws on unsupported scalar type")
    void testAppendParquetUnsupportedScalarType() {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);

      List<Map<String, Object>> schema = new ArrayList<>();
      schema.add(createSchemaCol("bad", "timestamp"));

      List<Map<String, Object>> records = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("bad", "value");
      records.add(row);

      assertThrows(IllegalArgumentException.class,
          () -> provider.appendParquet("cache/bad.parquet", records, schema));
    }

    @Test
    @DisplayName("appendParquet handles corrupt existing file gracefully")
    void testAppendParquetCorruptExistingFile() throws IOException {
      LocalTempStorageProvider provider = new LocalTempStorageProvider(tempDir);

      // Write a corrupt file
      provider.writeFile("cache/corrupt.parquet", "not a parquet file".getBytes(StandardCharsets.UTF_8));

      List<Map<String, Object>> schema = new ArrayList<>();
      schema.add(createSchemaCol("key", "string"));

      List<Map<String, Object>> records = new ArrayList<>();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("key", "value");
      records.add(row);

      // Should still succeed - reads fail silently and overwrite
      provider.appendParquet("cache/corrupt.parquet", records, schema);
      assertTrue(provider.exists("cache/corrupt.parquet"));

      List<Map<String, Object>> result = provider.readParquet("cache/corrupt.parquet");
      assertEquals(1, result.size());
    }

    private Map<String, Object> createSchemaCol(String name, String type) {
      Map<String, Object> col = new HashMap<>();
      col.put("name", name);
      col.put("type", type);
      return col;
    }
  }

  // ================================================================
  // deleteBatch additional tests
  // ================================================================

  @Nested
  @DisplayName("deleteBatch additional cases")
  class DeleteBatchAdditionalTests {

    @Test
    @DisplayName("deleteBatch with empty list returns zero")
    void testDeleteBatchEmptyList() throws IOException {
      MinimalStubProvider provider = new MinimalStubProvider("test") {
        @Override public boolean delete(String path) {
          return true;
        }
      };
      assertEquals(0, provider.deleteBatch(Collections.emptyList()));
    }

    @Test
    @DisplayName("deleteBatch propagates IOException from delete")
    void testDeleteBatchPropagatesException() {
      MinimalStubProvider provider = new MinimalStubProvider("test") {
        @Override public boolean delete(String path) throws IOException {
          throw new IOException("delete failed");
        }
      };
      assertThrows(IOException.class,
          () -> provider.deleteBatch(Arrays.asList("/a", "/b")));
    }

    @Test
    @DisplayName("deleteBatch counts zero when all deletes return false")
    void testDeleteBatchAllFail() throws IOException {
      MinimalStubProvider provider = new MinimalStubProvider("test") {
        @Override public boolean delete(String path) {
          return false;
        }
      };
      assertEquals(0, provider.deleteBatch(Arrays.asList("/a", "/b", "/c")));
    }
  }

  // ================================================================
  // hasChanged additional edge cases
  // ================================================================

  @Nested
  @DisplayName("hasChanged additional edge cases")
  class HasChangedAdditionalTests {

    @Test
    @DisplayName("hasChanged returns false when size, etag match even with different timestamps")
    void testHasChangedEtagMatchDifferentTimestamp() throws IOException {
      MinimalStubProvider provider = new MinimalStubProvider("test");
      StorageProvider.FileMetadata current =
          new StorageProvider.FileMetadata("/f", 100L, 5000L, null, "abc");
      StorageProvider.FileMetadata cached =
          new StorageProvider.FileMetadata("/f", 100L, 1000L, null, "abc");
      provider.setMetadataResult(current);
      assertFalse(provider.hasChanged("/f", cached));
    }

    @Test
    @DisplayName("hasChanged falls through to timestamp when current etag is null")
    void testHasChangedCurrentEtagNull() throws IOException {
      MinimalStubProvider provider = new MinimalStubProvider("test");
      StorageProvider.FileMetadata current =
          new StorageProvider.FileMetadata("/f", 100L, 1000L, null, null);
      StorageProvider.FileMetadata cached =
          new StorageProvider.FileMetadata("/f", 100L, 1000L, null, "etag123");
      provider.setMetadataResult(current);
      // Both size match, etag comparison skipped because current is null,
      // falls through to timestamp comparison -- timeDiff = 0 <= 1000, so false
      assertFalse(provider.hasChanged("/f", cached));
    }

    @Test
    @DisplayName("hasChanged falls through to timestamp when cached etag is null")
    void testHasChangedCachedEtagNull() throws IOException {
      MinimalStubProvider provider = new MinimalStubProvider("test");
      StorageProvider.FileMetadata current =
          new StorageProvider.FileMetadata("/f", 100L, 1500L, null, "etag123");
      StorageProvider.FileMetadata cached =
          new StorageProvider.FileMetadata("/f", 100L, 1000L, null, null);
      provider.setMetadataResult(current);
      // Both size match, etag comparison skipped because cached is null,
      // falls through to timestamp comparison -- timeDiff = 500 <= 1000, so false
      assertFalse(provider.hasChanged("/f", cached));
    }

    @Test
    @DisplayName("hasChanged returns true on IOException from getMetadata")
    void testHasChangedIOException() throws IOException {
      MinimalStubProvider provider = new MinimalStubProvider("test");
      provider.setThrowOnGetMetadata(true);
      StorageProvider.FileMetadata cached =
          new StorageProvider.FileMetadata("/f", 100L, 1000L, null, null);
      assertTrue(provider.hasChanged("/f", cached));
    }

    @Test
    @DisplayName("hasChanged with exactly 1000ms difference returns false (boundary)")
    void testHasChangedExactBoundary() throws IOException {
      MinimalStubProvider provider = new MinimalStubProvider("test");
      StorageProvider.FileMetadata current =
          new StorageProvider.FileMetadata("/f", 100L, 2000L, null, null);
      StorageProvider.FileMetadata cached =
          new StorageProvider.FileMetadata("/f", 100L, 1000L, null, null);
      provider.setMetadataResult(current);
      // timeDiff = 1000, which is NOT > 1000, so false
      assertFalse(provider.hasChanged("/f", cached));
    }

    @Test
    @DisplayName("hasChanged with 1001ms difference returns true (just over boundary)")
    void testHasChangedJustOverBoundary() throws IOException {
      MinimalStubProvider provider = new MinimalStubProvider("test");
      StorageProvider.FileMetadata current =
          new StorageProvider.FileMetadata("/f", 100L, 2001L, null, null);
      StorageProvider.FileMetadata cached =
          new StorageProvider.FileMetadata("/f", 100L, 1000L, null, null);
      provider.setMetadataResult(current);
      // timeDiff = 1001, which IS > 1000, so true
      assertTrue(provider.hasChanged("/f", cached));
    }
  }

  // ================================================================
  // normalizePath additional coverage
  // ================================================================

  @Nested
  @DisplayName("normalizePath additional")
  class NormalizePathAdditionalTests {

    @Test
    @DisplayName("s3a:// already correct is unchanged")
    void testS3aAlreadyCorrect() {
      assertEquals("s3a://my-bucket/key",
          StorageProvider.normalizePath("s3a://my-bucket/key"));
    }

    @Test
    @DisplayName("s3:// already correct is unchanged")
    void testS3AlreadyCorrect() {
      assertEquals("s3://my-bucket/key",
          StorageProvider.normalizePath("s3://my-bucket/key"));
    }

    @Test
    @DisplayName("hdfs:// already correct is unchanged")
    void testHdfsAlreadyCorrect() {
      assertEquals("hdfs://namenode:8020/path",
          StorageProvider.normalizePath("hdfs://namenode:8020/path"));
    }

    @Test
    @DisplayName("file:// path is left unchanged")
    void testFileProtocol() {
      assertEquals("file:///tmp/data",
          StorageProvider.normalizePath("file:///tmp/data"));
    }

    @Test
    @DisplayName("http:// path is left unchanged")
    void testHttpProtocol() {
      assertEquals("http://example.com/data.csv",
          StorageProvider.normalizePath("http://example.com/data.csv"));
    }
  }

  // ================================================================
  // UnsupportedOperationException message validation
  // ================================================================

  @Nested
  @DisplayName("UnsupportedOperationException messages")
  class UnsupportedOpMessageTests {

    @Test
    @DisplayName("readRange message includes storage type")
    void testReadRangeMessage() {
      MinimalStubProvider provider = new MinimalStubProvider("custom-storage");
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.readRange("/path", 0, 100));
      assertTrue(ex.getMessage().contains("custom-storage"));
      assertTrue(ex.getMessage().contains("Range reads"));
    }

    @Test
    @DisplayName("writeFile(byte[]) message includes storage type")
    void testWriteFileBytesMessage() {
      MinimalStubProvider provider = new MinimalStubProvider("my-provider");
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.writeFile("/path", new byte[0]));
      assertTrue(ex.getMessage().contains("my-provider"));
      assertTrue(ex.getMessage().contains("Write operations"));
    }

    @Test
    @DisplayName("writeFile(InputStream) message includes storage type")
    void testWriteFileStreamMessage() {
      MinimalStubProvider provider = new MinimalStubProvider("stream-provider");
      InputStream is = new ByteArrayInputStream(new byte[0]);
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.writeFile("/path", is));
      assertTrue(ex.getMessage().contains("stream-provider"));
    }

    @Test
    @DisplayName("createDirectories message includes storage type")
    void testCreateDirMessage() {
      MinimalStubProvider provider = new MinimalStubProvider("readonly");
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.createDirectories("/new/path"));
      assertTrue(ex.getMessage().contains("readonly"));
      assertTrue(ex.getMessage().contains("Directory creation"));
    }

    @Test
    @DisplayName("delete message includes storage type")
    void testDeleteMessage() {
      MinimalStubProvider provider = new MinimalStubProvider("immutable");
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.delete("/file"));
      assertTrue(ex.getMessage().contains("immutable"));
      assertTrue(ex.getMessage().contains("Delete operations"));
    }

    @Test
    @DisplayName("copyFile message includes storage type")
    void testCopyFileMessage() {
      MinimalStubProvider provider = new MinimalStubProvider("remote");
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.copyFile("/src", "/dst"));
      assertTrue(ex.getMessage().contains("remote"));
      assertTrue(ex.getMessage().contains("Copy operations"));
    }
  }

  // ================================================================
  // No-op default methods
  // ================================================================

  @Nested
  @DisplayName("No-op default methods")
  class NoOpDefaultTests {

    @Test
    @DisplayName("ensureLifecycleRule completes without error")
    void testEnsureLifecycleRule() throws IOException {
      MinimalStubProvider provider = new MinimalStubProvider("test");
      provider.ensureLifecycleRule("prefix/", 30);
      provider.ensureLifecycleRule("another/prefix", 1);
      provider.ensureLifecycleRule("", 365);
      // No exception means success
    }

    @Test
    @DisplayName("cleanupMacosMetadata completes without error")
    void testCleanupMacosMetadata() throws IOException {
      MinimalStubProvider provider = new MinimalStubProvider("test");
      provider.cleanupMacosMetadata("/some/path");
      provider.cleanupMacosMetadata("");
      provider.cleanupMacosMetadata("/another/path");
      // No exception means success
    }

    @Test
    @DisplayName("getS3Config returns null")
    void testGetS3Config() {
      MinimalStubProvider provider = new MinimalStubProvider("test");
      assertNull(provider.getS3Config());
    }
  }

  // ================================================================
  // FileEntry inner class coverage
  // ================================================================

  @Nested
  @DisplayName("FileEntry edge cases")
  class FileEntryEdgeCases {

    @Test
    @DisplayName("FileEntry with null path and name")
    void testNullPathAndName() {
      StorageProvider.FileEntry entry =
          new StorageProvider.FileEntry(null, null, false, -1L, -1L);
      assertNull(entry.getPath());
      assertNull(entry.getName());
      assertEquals(-1L, entry.getSize());
      assertEquals(-1L, entry.getLastModified());
      assertFalse(entry.isDirectory());
    }

    @Test
    @DisplayName("FileEntry with large size and timestamp")
    void testLargeValues() {
      StorageProvider.FileEntry entry =
          new StorageProvider.FileEntry("/path", "file", false,
              Long.MAX_VALUE, Long.MAX_VALUE);
      assertEquals(Long.MAX_VALUE, entry.getSize());
      assertEquals(Long.MAX_VALUE, entry.getLastModified());
    }
  }

  // ================================================================
  // FileMetadata inner class coverage
  // ================================================================

  @Nested
  @DisplayName("FileMetadata edge cases")
  class FileMetadataEdgeCases {

    @Test
    @DisplayName("FileMetadata with all fields populated")
    void testAllFieldsPopulated() {
      StorageProvider.FileMetadata meta =
          new StorageProvider.FileMetadata("/path/file.csv", 1024L, 1700000000L,
              "text/csv", "\"abc123\"");
      assertEquals("/path/file.csv", meta.getPath());
      assertEquals(1024L, meta.getSize());
      assertEquals(1700000000L, meta.getLastModified());
      assertEquals("text/csv", meta.getContentType());
      assertEquals("\"abc123\"", meta.getEtag());
    }

    @Test
    @DisplayName("FileMetadata with null path")
    void testNullPath() {
      StorageProvider.FileMetadata meta =
          new StorageProvider.FileMetadata(null, 0L, 0L, null, null);
      assertNull(meta.getPath());
    }

    @Test
    @DisplayName("FileMetadata with empty string etag")
    void testEmptyEtag() {
      StorageProvider.FileMetadata meta =
          new StorageProvider.FileMetadata("/path", 0L, 0L, "", "");
      assertEquals("", meta.getContentType());
      assertEquals("", meta.getEtag());
    }
  }
}
