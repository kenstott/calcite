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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.adapter.file.BaseFileTest;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Statistic;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the metadata package:
 * ConversionMetadata, FileFieldType, TableConstraints, TableBackingMetadata,
 * and InformationSchema.
 */
@Tag("integration")
public class MetadataIntegrationTest extends BaseFileTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MetadataIntegrationTest.class);

  @TempDir
  Path tempDir;

  private void writeCsv(File dir, String name, String content) throws IOException {
    try (FileWriter w = new FileWriter(new File(dir, name))) {
      w.write(content);
    }
  }

  private void writeJson(File dir, String name, String content) throws IOException {
    try (FileWriter w = new FileWriter(new File(dir, name))) {
      w.write(content);
    }
  }

  private Connection connect(String modelJson) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + addEphemeralCacheToModel(modelJson));
    applyEngineDefaults(info);
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  // --- ConversionMetadata tests ---

  @Test void testConversionMetadataCreation() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);
    assertNotNull(metadata);
  }

  @Test void testConversionMetadataFromPath() throws Exception {
    ConversionMetadata metadata = new ConversionMetadata(tempDir.toString());
    assertNotNull(metadata);
  }

  @Test void testConversionRecordBasicConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord(
            "/path/to/original.xlsx",
            "/path/to/converted.json",
            "EXCEL_TO_JSON");
    assertEquals("/path/to/original.xlsx", record.getOriginalPath());
    assertEquals("/path/to/converted.json", record.getConvertedFile());
    assertEquals("EXCEL_TO_JSON", record.getConversionType());
    assertTrue(record.timestamp > 0);
  }

  @Test void testConversionRecordWithParquetCache() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord(
            "/path/to/original.xlsx",
            "/path/to/converted.json",
            "EXCEL_TO_JSON",
            "/path/to/cache.parquet");
    assertEquals("/path/to/cache.parquet", record.getParquetCacheFile());
  }

  @Test void testConversionRecordWithHttpMetadata() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord(
            "https://example.com/data.csv",
            "/path/to/local.csv",
            "HTTP_DOWNLOAD",
            null,
            "\"abc123\"",
            1024L,
            "text/csv");
    assertEquals("\"abc123\"", record.etag);
    assertEquals(Long.valueOf(1024L), record.contentLength);
    assertEquals("text/csv", record.contentType);
  }

  @Test void testConversionRecordDefaultConstructor() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertNull(record.tableName);
    assertNull(record.getOriginalPath());
    assertNull(record.getConvertedFile());
  }

  @Test void testConversionRecordComprehensiveConstructor() {
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("name", "test_table");

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord(
            "test_table", "ParquetTranslatableTable",
            "/path/source.csv", "csv",
            "/path/original.csv", "/path/converted.json",
            "CSV_TO_JSON",
            "/path/cache.parquet", "*.parquet",
            true, "5 minutes",
            "\"etag\"", 2048L, "text/csv",
            tableConfig);

    assertEquals("test_table", record.getTableName());
    assertEquals("ParquetTranslatableTable", record.tableType);
    assertEquals("/path/source.csv", record.getSourceFile());
    assertEquals("csv", record.sourceType);
    assertEquals(true, record.refreshEnabled);
    assertEquals("5 minutes", record.refreshInterval);
    assertEquals("*.parquet", record.viewScanPattern);
    assertNotNull(record.tableConfig);
  }

  @Test void testConversionRecordIsIcebergFormat() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertFalse(record.isIcebergFormat(), "Null config should not be iceberg");

    record.tableConfig = new HashMap<String, Object>();
    assertFalse(record.isIcebergFormat(), "Empty config should not be iceberg");

    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "iceberg");
    record.tableConfig.put("materialize", materialize);
    assertTrue(record.isIcebergFormat(), "Config with iceberg format should be iceberg");
  }

  @Test void testConversionRecordIsNotIcebergFormat() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();

    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "parquet");
    record.tableConfig.put("materialize", materialize);
    assertFalse(record.isIcebergFormat(), "Parquet format should not be iceberg");
  }

  @Test void testConversionMetadataRecordAndRetrieve() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    File original = new File(dir, "original.xlsx");
    original.createNewFile();
    File converted = new File(dir, "converted.json");
    converted.createNewFile();

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    ConversionMetadata.ConversionRecord record =
        metadata.getConversionRecord(converted);
    assertNotNull(record, "Should find recorded conversion");
    assertEquals("EXCEL_TO_JSON", record.getConversionType());
  }

  @Test void testConversionMetadataRecordWithParquet() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    File original = new File(dir, "data.csv");
    original.createNewFile();
    File converted = new File(dir, "data.json");
    converted.createNewFile();
    File parquetCache = new File(dir, "data.parquet");
    parquetCache.createNewFile();

    metadata.recordConversion(original, converted, "CSV_TO_JSON", parquetCache);

    ConversionMetadata.ConversionRecord record =
        metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertNotNull(record.getParquetCacheFile());
  }

  @Test void testConversionMetadataGetAllConversions() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    File f1 = new File(dir, "a.xlsx");
    f1.createNewFile();
    File c1 = new File(dir, "a.json");
    c1.createNewFile();

    File f2 = new File(dir, "b.xlsx");
    f2.createNewFile();
    File c2 = new File(dir, "b.json");
    c2.createNewFile();

    metadata.recordConversion(f1, c1, "EXCEL_TO_JSON");
    metadata.recordConversion(f2, c2, "EXCEL_TO_JSON");

    Map<String, ConversionMetadata.ConversionRecord> all = metadata.getAllConversions();
    assertTrue(all.size() >= 2, "Should have at least 2 conversions");
  }

  @Test void testConversionMetadataSaveAndReload() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    File original = new File(dir, "source.csv");
    original.createNewFile();
    File converted = new File(dir, "source.json");
    converted.createNewFile();

    metadata.recordConversion(original, converted, "DIRECT");
    metadata.saveMetadata();

    // Create a new instance to reload from disk
    ConversionMetadata reloaded = new ConversionMetadata(dir);
    Map<String, ConversionMetadata.ConversionRecord> all = reloaded.getAllConversions();
    assertFalse(all.isEmpty(), "Reloaded metadata should have records");
  }

  @Test void testConversionMetadataHints() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    metadata.setHint("cik", "0001234567");
    metadata.setHint("form", "10-K");

    assertEquals("0001234567", metadata.getHint("cik"));
    assertEquals("10-K", metadata.getHint("form"));
    assertNull(metadata.getHint("nonexistent"));
  }

  @Test void testConversionMetadataClear() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    File original = new File(dir, "clear_test.csv");
    original.createNewFile();
    File converted = new File(dir, "clear_test.json");
    converted.createNewFile();

    metadata.recordConversion(original, converted, "DIRECT");
    assertFalse(metadata.getAllConversions().isEmpty());

    metadata.clear();
    assertTrue(metadata.getAllConversions().isEmpty(),
        "After clear, should have no conversions");
  }

  @Test void testConversionMetadataHasTableMetadata() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    assertFalse(metadata.hasTableMetadata("test_table"));

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test_table";
    metadata.putConversionRecord("test_table", record);

    assertTrue(metadata.hasTableMetadata("test_table"));
  }

  @Test void testConversionMetadataFindOriginalSource() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    File original = new File(dir, "original_find.xlsx");
    original.createNewFile();
    File converted = new File(dir, "original_find.json");
    converted.createNewFile();

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    File found = metadata.findOriginalSource(converted);
    assertNotNull(found);
    assertEquals(original.getCanonicalPath(), found.getCanonicalPath());
  }

  @Test void testConversionMetadataRecordWithConversionRecordObject() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    File converted = new File(dir, "recorded.json");
    converted.createNewFile();

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord(
            "/path/to/src.xlsx",
            converted.getAbsolutePath(),
            "EXCEL_TO_JSON");
    record.tableName = "my_table";

    metadata.recordConversion(converted, record);

    ConversionMetadata.ConversionRecord retrieved =
        metadata.getConversionRecord(converted);
    assertNotNull(retrieved);
    assertEquals("my_table", retrieved.tableName);
  }

  @Test void testConversionMetadataUpdateCachedFile() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    File converted = new File(dir, "update_cache.json");
    converted.createNewFile();
    File original = new File(dir, "update_cache.csv");
    original.createNewFile();

    metadata.recordConversion(original, converted, "DIRECT");

    File parquetFile = new File(dir, "update_cache.parquet");
    parquetFile.createNewFile();
    metadata.updateCachedFile(converted, parquetFile);

    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertNotNull(record.getParquetCacheFile());
  }

  // --- FileFieldType tests ---

  @Test void testFileFieldTypeOf() {
    assertEquals(FileFieldType.STRING, FileFieldType.of("String"));
    assertEquals(FileFieldType.INT, FileFieldType.of("int"));
    assertEquals(FileFieldType.LONG, FileFieldType.of("long"));
    assertEquals(FileFieldType.DOUBLE, FileFieldType.of("double"));
    assertEquals(FileFieldType.FLOAT, FileFieldType.of("float"));
    assertEquals(FileFieldType.BOOLEAN, FileFieldType.of("boolean"));
    assertEquals(FileFieldType.SHORT, FileFieldType.of("short"));
    assertEquals(FileFieldType.BYTE, FileFieldType.of("byte"));
    assertEquals(FileFieldType.CHAR, FileFieldType.of("char"));
    assertNull(FileFieldType.of("nonexistent"));
  }

  @Test void testFileFieldTypeTimestampAliases() {
    assertEquals(FileFieldType.TIMESTAMP, FileFieldType.of("timestamp"));
    assertEquals(FileFieldType.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        FileFieldType.of("timestamptz"));
    assertEquals(FileFieldType.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        FileFieldType.of("TimestampWithLocalTimeZone"));
  }

  @Test void testFileFieldTypeToType() {
    JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    for (FileFieldType fieldType : FileFieldType.values()) {
      RelDataType relType = fieldType.toType(typeFactory);
      assertNotNull(relType, "Type " + fieldType + " should produce a RelDataType");
      LOGGER.debug("FileFieldType {} -> {}", fieldType, relType);
    }
  }

  @Test void testFileFieldTypeEnumValues() {
    FileFieldType[] values = FileFieldType.values();
    assertTrue(values.length >= 12, "Should have at least 12 field types");

    // Verify specific types exist
    assertNotNull(FileFieldType.valueOf("STRING"));
    assertNotNull(FileFieldType.valueOf("INT"));
    assertNotNull(FileFieldType.valueOf("LONG"));
    assertNotNull(FileFieldType.valueOf("DOUBLE"));
    assertNotNull(FileFieldType.valueOf("DATE"));
    assertNotNull(FileFieldType.valueOf("TIME"));
    assertNotNull(FileFieldType.valueOf("TIMESTAMP"));
    assertNotNull(FileFieldType.valueOf("TIMESTAMP_WITH_LOCAL_TIME_ZONE"));
  }

  // --- TableBackingMetadata tests ---

  @Test void testTableBackingMetadataBasic() {
    TableBackingMetadata meta = new TableBackingMetadata("test_table");
    assertEquals("test_table", meta.getTableName());
    assertNull(meta.getOriginalSource());
    assertNull(meta.getGeneratedSource());
    assertNull(meta.getCached());
  }

  @Test void testTableBackingMetadataSetters() {
    TableBackingMetadata meta = new TableBackingMetadata("table1");
    File original = new File("/path/to/original.csv");
    File generated = new File("/path/to/generated.json");
    File cached = new File("/path/to/cached.parquet");

    meta.setOriginalSource(original);
    meta.setGeneratedSource(generated);
    meta.setCached(cached);

    assertEquals(original, meta.getOriginalSource());
    assertEquals(generated, meta.getGeneratedSource());
    assertEquals(cached, meta.getCached());
  }

  @Test void testTableBackingMetadataGetBackingFile() {
    TableBackingMetadata meta = new TableBackingMetadata("table2");
    File original = new File("/path/to/original.csv");
    File generated = new File("/path/to/generated.json");
    File cached = new File("/path/to/cached.parquet");

    meta.setOriginalSource(original);
    meta.setGeneratedSource(generated);
    meta.setCached(cached);

    // When requiresCached=true, should return cached
    assertEquals(cached, meta.getBackingFile(true));

    // When requiresCached=false, should return generatedSource
    assertEquals(generated, meta.getBackingFile(false));
  }

  @Test void testTableBackingMetadataGetBackingFileFallback() {
    TableBackingMetadata meta = new TableBackingMetadata("table3");
    File original = new File("/path/to/original.csv");
    meta.setOriginalSource(original);

    // No generated or cached - should fall back to original
    assertEquals(original, meta.getBackingFile(false));
    assertEquals(original, meta.getBackingFile(true));
  }

  @Test void testTableBackingMetadataToString() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    String str = meta.toString();
    assertTrue(str.contains("test"), "toString should contain table name");
    assertTrue(str.contains("TableBackingMetadata"));
  }

  // --- TableConstraints tests ---

  @Test void testTableConstraintsFromConfigWithPrimaryKey() {
    Map<String, Object> config = new HashMap<String, Object>();
    Map<String, Object> constraints = new HashMap<String, Object>();
    List<String> pkColumns = new ArrayList<String>();
    pkColumns.add("id");
    constraints.put("primaryKey", pkColumns);
    config.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("id", "name", "value");

    Statistic stat = TableConstraints.fromConfig(config, columnNames, 100.0);
    assertNotNull(stat);
    assertNotNull(stat.getKeys());
    LOGGER.debug("Statistic keys: {}", stat.getKeys());
  }

  @Test void testTableConstraintsFromConfigEmpty() {
    Map<String, Object> config = new HashMap<String, Object>();
    List<String> columnNames = Arrays.asList("id", "name");

    Statistic stat = TableConstraints.fromConfig(config, columnNames, null);
    assertNotNull(stat);
  }

  @Test void testTableConstraintsFromConfigWithRowCount() {
    Map<String, Object> config = new HashMap<String, Object>();
    // fromConfig requires a "constraints" key to produce a Statistic with rowCount
    Map<String, Object> constraints = new HashMap<String, Object>();
    config.put("constraints", constraints);
    List<String> columnNames = Arrays.asList("id", "name");

    Statistic stat = TableConstraints.fromConfig(config, columnNames, 5000.0);
    assertNotNull(stat);
    assertEquals(5000.0, stat.getRowCount());
  }

  @Test void testTableConstraintsFromConfigWithSchemaAndTableName() {
    Map<String, Object> config = new HashMap<String, Object>();
    List<String> pkColumns = new ArrayList<String>();
    pkColumns.add("emp_id");
    config.put("primaryKey", pkColumns);

    List<String> columnNames = Arrays.asList("emp_id", "name", "dept_id");

    Statistic stat = TableConstraints.fromConfig(config, columnNames, 1000.0,
        "hr", "employees");
    assertNotNull(stat);
  }

  @Test void testTableConstraintsCompositePrimaryKey() {
    Map<String, Object> config = new HashMap<String, Object>();
    Map<String, Object> constraints = new HashMap<String, Object>();
    List<String> pkColumns = new ArrayList<String>();
    pkColumns.add("region");
    pkColumns.add("year");
    constraints.put("primaryKey", pkColumns);
    config.put("constraints", constraints);

    List<String> columnNames = Arrays.asList("region", "year", "amount");

    Statistic stat = TableConstraints.fromConfig(config, columnNames, null);
    assertNotNull(stat);
    assertNotNull(stat.getKeys());
  }

  // --- PartitionBaseline tests ---

  @Test void testPartitionBaselineIsEmpty() {
    ConversionMetadata.PartitionBaseline baseline =
        new ConversionMetadata.PartitionBaseline();
    assertTrue(baseline.isEmpty());

    baseline.files = new HashMap<String, ConversionMetadata.FileBaseline>();
    assertTrue(baseline.isEmpty());

    baseline.files.put("file1.parquet",
        new ConversionMetadata.FileBaseline(1024L, "\"etag1\"", 1000L));
    assertFalse(baseline.isEmpty());
  }

  @Test void testFileBaselineHasChanged() {
    ConversionMetadata.FileBaseline original =
        new ConversionMetadata.FileBaseline(1024L, "\"etag1\"", 1000L);

    // Same values - no change
    ConversionMetadata.FileBaseline same =
        new ConversionMetadata.FileBaseline(1024L, "\"etag1\"", 1000L);
    assertFalse(original.hasChanged(same));

    // Different etag - changed
    ConversionMetadata.FileBaseline differentEtag =
        new ConversionMetadata.FileBaseline(1024L, "\"etag2\"", 1000L);
    assertTrue(original.hasChanged(differentEtag));

    // Different size - changed (use null etags so size comparison is reached)
    ConversionMetadata.FileBaseline sizeOriginal =
        new ConversionMetadata.FileBaseline(1024L, null, 1000L);
    ConversionMetadata.FileBaseline differentSize =
        new ConversionMetadata.FileBaseline(2048L, null, 1000L);
    assertTrue(sizeOriginal.hasChanged(differentSize));
  }

  @Test void testFileBaselineNullEtag() {
    ConversionMetadata.FileBaseline noEtag =
        new ConversionMetadata.FileBaseline(1024L, null, 1000L);

    // Both null etag, same size - not changed
    ConversionMetadata.FileBaseline sameNoEtag =
        new ConversionMetadata.FileBaseline(1024L, null, 1000L);
    assertFalse(noEtag.hasChanged(sameNoEtag));

    // Different lastModified - changed (diff must exceed 1000ms tolerance)
    ConversionMetadata.FileBaseline differentMod =
        new ConversionMetadata.FileBaseline(1024L, null, 5000L);
    assertTrue(noEtag.hasChanged(differentMod));
  }

  // --- Integration test: metadata created through schema ---

  @Test void testMetadataCreatedBySchema() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "meta_data.csv",
        "id,name,value\n"
        + "1,Alice,100\n"
        + "2,Bob,200\n");

    try (Connection conn = connect(buildTestModel("meta_schema", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM meta_schema.meta_data")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
    }
  }

  @Test void testSchemaMetadataWithMultipleFiles() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "table_a.csv", "id,val\n1,x\n2,y\n");
    writeCsv(dir, "table_b.csv", "id,val\n3,z\n");
    writeJson(dir, "table_c.json",
        "[{\"id\":4,\"val\":\"w\"}]");

    try (Connection conn = connect(buildTestModel("multi_meta", dir.getAbsolutePath()))) {
      java.sql.DatabaseMetaData dbMeta = conn.getMetaData();
      List<String> tableNames = new ArrayList<String>();
      try (ResultSet tables = dbMeta.getTables(null, "multi_meta", "%", null)) {
        while (tables.next()) {
          tableNames.add(tables.getString("TABLE_NAME").toLowerCase());
        }
      }
      assertTrue(tableNames.size() >= 3,
          "Should have at least 3 tables: " + tableNames);
    }
  }

  @Test void testConversionRecordWithTableName() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    File source = new File(dir, "named.csv");
    source.createNewFile();
    File converted = new File(dir, "named.json");
    converted.createNewFile();

    metadata.recordConversionWithTableName("my_named_table", source, converted, "DIRECT");

    assertTrue(metadata.hasTableMetadata("my_named_table"));
  }

  @Test void testConversionMetadataUpdateMaterializationInfo() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "mat_table";
    metadata.putConversionRecord("mat_table", record);

    metadata.updateMaterializationInfo("mat_table",
        "/path/to/materialized.parquet", "MATERIALIZED");

    ConversionMetadata.ConversionRecord updated =
        metadata.getAllConversions().get("mat_table");
    assertNotNull(updated);
  }

  @Test void testConversionMetadataUpdateMaterializationInfoWithRowCount() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "row_count_table";
    metadata.putConversionRecord("row_count_table", record);

    metadata.updateMaterializationInfo("row_count_table",
        "/path/to/mat.parquet", "MATERIALIZED", 5000L);

    ConversionMetadata.ConversionRecord updated =
        metadata.getAllConversions().get("row_count_table");
    assertNotNull(updated);
    assertEquals(Long.valueOf(5000L), updated.rowCount);
  }

  @Test void testConversionMetadataReload() throws Exception {
    File dir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(dir);

    File src = new File(dir, "reload_test.csv");
    src.createNewFile();
    File conv = new File(dir, "reload_test.json");
    conv.createNewFile();

    metadata.recordConversion(src, conv, "DIRECT");
    metadata.saveMetadata();

    // Reload should not throw
    metadata.reload();
    assertFalse(metadata.getAllConversions().isEmpty());
  }

  @Test void testConversionRecordHasChanged() throws Exception {
    File dir = tempDir.toFile();
    File testFile = new File(dir, "change_test.csv");
    try (FileWriter w = new FileWriter(testFile)) {
      w.write("test content");
    }

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord(
            testFile.getAbsolutePath(),
            "/path/converted.json",
            "DIRECT");

    // hasChanged checks file modification time against record timestamp
    // Since we just created the file, it may or may not have changed
    // depending on timing, but the method should not throw
    boolean changed = record.hasChanged();
    LOGGER.debug("hasChanged result: {}", changed);
  }
}
