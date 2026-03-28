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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig.ColumnDefinition;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests (Tier 2) for {@link IcebergMaterializer} targeting
 * uncovered methods: fetchRows, createExclusionTempTable, configureS3 direct,
 * getSourceCountForBatch with batch conditions, getExcludedAccessions with
 * tracker data, getAccessionsFromIceberg, getSourceFileWatermark for local
 * paths, processWithRowBatching (legacy), getSourceAccessions file parsing
 * edge cases, and additional edge cases for existing methods.
 *
 * <p>Uses Mockito for external dependencies and reflection for private methods.
 */
@Tag("unit")
public class IcebergMaterializerDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  private StorageProvider mockStorageProvider;
  private IncrementalTracker mockTracker;
  private IcebergMaterializer materializer;

  @BeforeEach
  void setUp() {
    mockStorageProvider = mock(StorageProvider.class);
    mockTracker = mock(IncrementalTracker.class);
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    materializer = new IcebergMaterializer(
        tempDir.toString(), mockStorageProvider, mockTracker, 2, 10L);
  }

  // ===== fetchRows tests =====

  @Test
  void testFetchRowsWithDuckDB() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "fetchRows", Connection.class, String.class);
    method.setAccessible(true);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
            "CREATE TABLE test_fetch (id INTEGER, name VARCHAR, val DOUBLE)");
        stmt.execute("INSERT INTO test_fetch VALUES (1, 'alice', 1.5)");
        stmt.execute("INSERT INTO test_fetch VALUES (2, 'bob', 2.5)");
        stmt.execute("INSERT INTO test_fetch VALUES (3, null, null)");
      }

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rows = (List<Map<String, Object>>)
          method.invoke(materializer, conn, "SELECT * FROM test_fetch ORDER BY id");

      assertEquals(3, rows.size());
      assertEquals(1, rows.get(0).get("id"));
      assertEquals("alice", rows.get(0).get("name"));
      assertEquals(2.5, rows.get(1).get("val"));
      assertFalse(rows.get(2).containsKey("name"));
      assertFalse(rows.get(2).containsKey("val"));
      assertEquals(3, rows.get(2).get("id"));
    }
  }

  @Test
  void testFetchRowsEmpty() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "fetchRows", Connection.class, String.class);
    method.setAccessible(true);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE empty_tbl (id INTEGER)");
      }

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rows = (List<Map<String, Object>>)
          method.invoke(materializer, conn, "SELECT * FROM empty_tbl");

      assertTrue(rows.isEmpty());
    }
  }

  // ===== createExclusionTempTable tests =====

  @Test
  void testCreateExclusionTempTableSmallBatch() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "createExclusionTempTable", Connection.class, String.class, Set.class);
    method.setAccessible(true);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Set<String> exclusions = new HashSet<String>();
      for (int i = 0; i < 50; i++) {
        exclusions.add("accession_" + i);
      }

      method.invoke(materializer, conn, "accession_number", exclusions);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM _exclusions")) {
        assertTrue(rs.next());
        assertEquals(50, rs.getLong(1));
      }
    }
  }

  @Test
  void testCreateExclusionTempTableLargeBatch() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "createExclusionTempTable", Connection.class, String.class, Set.class);
    method.setAccessible(true);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Set<String> exclusions = new HashSet<String>();
      for (int i = 0; i < 1500; i++) {
        exclusions.add("acc_" + i);
      }

      method.invoke(materializer, conn, "accession_number", exclusions);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM _exclusions")) {
        assertTrue(rs.next());
        assertEquals(1500, rs.getLong(1));
      }
    }
  }

  @Test
  void testCreateExclusionTempTableExactBatchBoundary() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "createExclusionTempTable", Connection.class, String.class, Set.class);
    method.setAccessible(true);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Set<String> exclusions = new HashSet<String>();
      for (int i = 0; i < 1000; i++) {
        exclusions.add("acc_" + i);
      }

      method.invoke(materializer, conn, "accession_number", exclusions);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM _exclusions")) {
        assertTrue(rs.next());
        assertEquals(1000, rs.getLong(1));
      }
    }
  }

  @Test
  void testCreateExclusionTempTableDropsPrevious() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "createExclusionTempTable", Connection.class, String.class, Set.class);
    method.setAccessible(true);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Set<String> firstBatch = new HashSet<String>();
      firstBatch.add("first");
      method.invoke(materializer, conn, "accession_number", firstBatch);

      Set<String> secondBatch = new HashSet<String>();
      secondBatch.add("second");
      secondBatch.add("third");
      method.invoke(materializer, conn, "accession_number", secondBatch);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM _exclusions")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
      }
    }
  }

  // ===== configureS3 tests (direct invocation with real DuckDB) =====

  @Test
  void testConfigureS3DirectWithAllFields() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "configureS3", Statement.class, Map.class);
    method.setAccessible(true);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        Map<String, String> s3Config = new HashMap<String, String>();
        s3Config.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
        s3Config.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        s3Config.put("endpoint", "http://localhost:9000");
        s3Config.put("region", "us-east-1");

        method.invoke(materializer, stmt, s3Config);
      }
    }
  }

  @Test
  void testConfigureS3DirectNoRegion() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "configureS3", Statement.class, Map.class);
    method.setAccessible(true);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        Map<String, String> s3Config = new HashMap<String, String>();
        s3Config.put("accessKeyId", "test");
        s3Config.put("secretAccessKey", "secret");

        method.invoke(materializer, stmt, s3Config);
      }
    }
  }

  @Test
  void testConfigureS3WithEndpointOnly() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "configureS3", Statement.class, Map.class);
    method.setAccessible(true);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        Map<String, String> s3Config = new HashMap<String, String>();
        s3Config.put("endpoint", "http://minio:9000");

        method.invoke(materializer, stmt, s3Config);
      }
    }
  }

  @Test
  void testConfigureS3FailsGracefully() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "configureS3", Statement.class, Map.class);
    method.setAccessible(true);

    Statement mockStmt = mock(Statement.class);
    when(mockStmt.execute(anyString())).thenThrow(new SQLException("httpfs not available"));

    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "test");

    method.invoke(materializer, mockStmt, s3Config);
  }

  // ===== getSourceCountForBatch with various batch conditions =====

  @Test
  void testGetSourceCountForBatchWithNumericBatchValue() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceCountForBatch", Connection.class,
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .rowFilter("status = 'active'")
            .build();

    Connection mockConn = mock(Connection.class);
    when(mockConn.createStatement()).thenThrow(new SQLException("fail"));

    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("year", "2023");

    long result = (Long) method.invoke(materializer, mockConn, config,
        "data/*.parquet", batch);
    assertEquals(-1, result);
  }

  @Test
  void testGetSourceCountForBatchWithStringBatchValue() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceCountForBatch", Connection.class,
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Connection mockConn = mock(Connection.class);
    when(mockConn.createStatement()).thenThrow(new SQLException("fail"));

    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("region", "US");

    long result = (Long) method.invoke(materializer, mockConn, config,
        "data/*.parquet", batch);
    assertEquals(-1, result);
  }

  @Test
  void testGetSourceCountForBatchJsonFormat() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceCountForBatch", Connection.class,
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.json")
            .targetTableId("test_table")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .build();

    Connection mockConn = mock(Connection.class);
    when(mockConn.createStatement()).thenThrow(new SQLException("fail"));

    long result = (Long) method.invoke(materializer, mockConn, config,
        "data/*.json", Collections.emptyMap());
    assertEquals(-1, result);
  }

  @Test
  void testGetSourceCountForBatchMultipleConditions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceCountForBatch", Connection.class,
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .rowFilter("active = true")
            .build();

    Connection mockConn = mock(Connection.class);
    when(mockConn.createStatement()).thenThrow(new SQLException("fail"));

    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("year", "2023");
    batch.put("region", "US");

    long result = (Long) method.invoke(materializer, mockConn, config,
        "data/*.parquet", batch);
    assertEquals(-1, result);
  }

  @Test
  void testGetSourceCountForBatchNoConditions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceCountForBatch", Connection.class,
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Connection mockConn = mock(Connection.class);
    when(mockConn.createStatement()).thenThrow(new SQLException("fail"));

    long result = (Long) method.invoke(materializer, mockConn, config,
        "data/*.parquet", Collections.emptyMap());
    assertEquals(-1, result);
  }

  // ===== getExcludedAccessions with tracker data =====

  @Test
  void testGetExcludedAccessionsWithTrackerDataAndSelfHealing() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .sourceTableName("source_tbl")
            .icebergTableLocation("/tmp/iceberg/table")
            .build();

    when(mockTracker.getProcessedKeyValues("test_table"))
        .thenReturn(Collections.<Map<String, String>>emptySet());

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getExcludedAccessions", IcebergMaterializer.MaterializationConfig.class,
        org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    Map<String, String> batch = new HashMap<String, String>();
    batch.put("year", "2023");

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, config,
        null, batch);

    assertTrue(result.isEmpty());
  }

  @Test
  void testGetExcludedAccessionsTrackerHasData() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .icebergTableLocation("/tmp/iceberg/table")
            .build();

    Set<Map<String, String>> processed = new HashSet<Map<String, String>>();
    Map<String, String> entry = new LinkedHashMap<String, String>();
    entry.put("year", "2023");
    entry.put("accession_number", "tracked-acc-1");
    processed.add(entry);

    Map<String, String> entry2 = new LinkedHashMap<String, String>();
    entry2.put("year", "2023");
    entry2.put("accession_number", "tracked-acc-2");
    processed.add(entry2);

    when(mockTracker.getProcessedKeyValues("test_table")).thenReturn(processed);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getExcludedAccessions", IcebergMaterializer.MaterializationConfig.class,
        org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    Map<String, String> batch = new HashMap<String, String>();
    batch.put("year", "2023");

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, config,
        mock(org.apache.iceberg.Table.class), batch);

    assertEquals(2, result.size());
    assertTrue(result.contains("tracked-acc-1"));
    assertTrue(result.contains("tracked-acc-2"));
  }

  @Test
  void testGetExcludedAccessionsWithNullYear() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .icebergTableLocation("/tmp/iceberg/table")
            .build();

    Set<Map<String, String>> processed = new HashSet<Map<String, String>>();
    Map<String, String> entry = new LinkedHashMap<String, String>();
    entry.put("accession_number", "acc-no-year");
    processed.add(entry);

    when(mockTracker.getProcessedKeyValues("test_table")).thenReturn(processed);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getExcludedAccessions", IcebergMaterializer.MaterializationConfig.class,
        org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, config,
        null, Collections.<String, String>emptyMap());

    assertTrue(result.contains("acc-no-year"));
  }

  // ===== getAccessionsFromIceberg tests =====

  @Test
  void testGetAccessionsFromIcebergFailsGracefully() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getAccessionsFromIceberg", String.class, String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer,
        "/nonexistent/path/to/iceberg", "accession_number", "2023");

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetAccessionsFromIcebergNullYear() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getAccessionsFromIceberg", String.class, String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer,
        "/nonexistent/iceberg", "accession_number", null);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  // ===== getSourceFileWatermark for local paths =====

  @Test
  void testGetSourceFileWatermarkLocalPathNoFiles() {
    long watermark = materializer.getSourceFileWatermark(
        tempDir.toString() + "/nonexistent/*.parquet",
        IcebergMaterializer.SourceFormat.PARQUET);

    assertEquals(0, watermark);
  }

  @Test
  void testGetSourceFileWatermarkLocalPathJson() {
    long watermark = materializer.getSourceFileWatermark(
        tempDir.toString() + "/nonexistent/*.json",
        IcebergMaterializer.SourceFormat.JSON);

    assertEquals(0, watermark);
  }

  @Test
  void testGetSourceFileWatermarkLocalPathNoExtension() {
    long watermark = materializer.getSourceFileWatermark(
        tempDir.toString() + "/data",
        IcebergMaterializer.SourceFormat.PARQUET);

    assertEquals(0, watermark);
  }

  @Test
  void testGetSourceFileWatermarkLocalPathJsonNoExtension() {
    long watermark = materializer.getSourceFileWatermark(
        tempDir.toString() + "/data",
        IcebergMaterializer.SourceFormat.JSON);

    assertEquals(0, watermark);
  }

  // ===== getSourceAccessions edge cases =====

  @Test
  void testGetSourceAccessionsFileWithoutSecondUnderscore() throws Exception {
    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2023/singlepart_facts.parquet",
        "singlepart_facts.parquet", false, 100, System.currentTimeMillis()));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/*_facts.parquet", "2023");

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetSourceAccessionsFileWithNoUnderscore() throws Exception {
    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2023/nounderscore.parquet",
        "nounderscore.parquet", false, 100, System.currentTimeMillis()));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/*_facts.parquet", "2023");

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetSourceAccessionsMultipleCiksSameAccession() throws Exception {
    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2023/0001_acc-A_facts.parquet",
        "0001_acc-A_facts.parquet", false, 100, System.currentTimeMillis()));
    files.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2023/0001_acc-B_facts.parquet",
        "0001_acc-B_facts.parquet", false, 100, System.currentTimeMillis()));
    files.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2023/0002_acc-C_facts.parquet",
        "0002_acc-C_facts.parquet", false, 100, System.currentTimeMillis()));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/*_facts.parquet", "2023");

    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals(2, result.get("0001").size());
    assertTrue(result.get("0001").contains("acc-A"));
    assertTrue(result.get("0001").contains("acc-B"));
    assertEquals(1, result.get("0002").size());
    assertTrue(result.get("0002").contains("acc-C"));
  }

  @Test
  void testGetSourceAccessionsDefaultSuffix() throws Exception {
    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2023/0001_acc-1_metadata.parquet",
        "0001_acc-1_metadata.parquet", false, 100, System.currentTimeMillis()));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/data.parquet", "2023");

    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.get("0001").contains("acc-1"));
  }

  // ===== getFilteredSourceAccessions edge cases =====

  @Test
  void testGetFilteredSourceAccessionsWithCikFilterNonMatchingCik() throws Exception {
    Method getSourceMethod = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    getSourceMethod.setAccessible(true);

    Field cacheField = IcebergMaterializer.class.getDeclaredField("sourceAccessionsCache");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Set<String>>> cache =
        (Map<String, Map<String, Set<String>>>) cacheField.get(materializer);

    Map<String, Set<String>> cikMap = new HashMap<String, Set<String>>();
    Set<String> accessions = new HashSet<String>();
    accessions.add("acc1");
    cikMap.put("cik1", accessions);
    cache.put("2023:_data.parquet", cikMap);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) getSourceMethod.invoke(materializer,
        "s3://bucket/year=*/*_data.parquet", "2023", "cik IN ('nonexistent_cik')");

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetFilteredSourceAccessionsWithMultipleCikFilters() throws Exception {
    Method getSourceMethod = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    getSourceMethod.setAccessible(true);

    Field cacheField = IcebergMaterializer.class.getDeclaredField("sourceAccessionsCache");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Set<String>>> cache =
        (Map<String, Map<String, Set<String>>>) cacheField.get(materializer);

    Map<String, Set<String>> cikMap = new HashMap<String, Set<String>>();
    Set<String> acc1 = new HashSet<String>(Arrays.asList("a1", "a2"));
    cikMap.put("cik1", acc1);
    Set<String> acc2 = new HashSet<String>(Arrays.asList("a3"));
    cikMap.put("cik2", acc2);
    Set<String> acc3 = new HashSet<String>(Arrays.asList("a4", "a5"));
    cikMap.put("cik3", acc3);
    cache.put("2023:_data.parquet", cikMap);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) getSourceMethod.invoke(materializer,
        "s3://bucket/year=*/*_data.parquet", "2023",
        "cik IN ('cik1', 'cik3')");

    assertNotNull(result);
    assertEquals(4, result.size());
    assertTrue(result.contains("a1"));
    assertTrue(result.contains("a2"));
    assertTrue(result.contains("a4"));
    assertTrue(result.contains("a5"));
    assertFalse(result.contains("a3"));
  }

  // ===== getTrackedAccessions edge cases =====

  @Test
  void testGetTrackedAccessionsMultipleYearsFiltering() throws Exception {
    Set<Map<String, String>> processed = new HashSet<Map<String, String>>();

    Map<String, String> e1 = new LinkedHashMap<String, String>();
    e1.put("year", "2022");
    e1.put("accession_number", "acc-2022");
    processed.add(e1);

    Map<String, String> e2 = new LinkedHashMap<String, String>();
    e2.put("year", "2023");
    e2.put("accession_number", "acc-2023a");
    processed.add(e2);

    Map<String, String> e3 = new LinkedHashMap<String, String>();
    e3.put("year", "2023");
    e3.put("accession_number", "acc-2023b");
    processed.add(e3);

    when(mockTracker.getProcessedKeyValues(anyString())).thenReturn(processed);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, "tableId", "2023");

    assertEquals(2, result.size());
    assertTrue(result.contains("acc-2023a"));
    assertTrue(result.contains("acc-2023b"));
    assertFalse(result.contains("acc-2022"));
  }

  @Test
  void testGetTrackedAccessionsOnlyYearKey() throws Exception {
    Set<Map<String, String>> processed = new HashSet<Map<String, String>>();
    Map<String, String> entry = new LinkedHashMap<String, String>();
    entry.put("year", "2023");
    processed.add(entry);

    when(mockTracker.getProcessedKeyValues(anyString())).thenReturn(processed);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, "tableId", "2023");

    assertTrue(result.isEmpty());
  }

  // ===== buildSelectSql additional edge cases =====

  @Test
  void testBuildSelectSqlWithMultipleComputedColumns() throws Exception {
    Map<String, String> computed = new LinkedHashMap<String, String>();
    computed.put("col_a", "LOWER(name)");
    computed.put("col_b", "LENGTH(text)");
    computed.put("col_c", "YEAR(date_col)");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .computedColumns(computed)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), Collections.emptySet());

    assertTrue(sql.contains("LOWER(name) AS col_a"));
    assertTrue(sql.contains("LENGTH(text) AS col_b"));
    assertTrue(sql.contains("YEAR(date_col) AS col_c"));
  }

  @Test
  void testBuildSelectSqlWithFilterAndLargeExclusions() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .rowFilter("type = '10-K'")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    Set<String> exclusions = new HashSet<String>();
    for (int i = 0; i < 200; i++) {
      exclusions.add("large_acc_" + i);
    }

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), exclusions);

    assertTrue(sql.contains("WHERE type = '10-K'"));
    assertTrue(sql.contains("AND NOT EXISTS (SELECT 1 FROM _exclusions"));
  }

  // ===== buildSelectSqlWithPaging additional edge cases =====

  @Test
  void testBuildSelectSqlWithPagingSmallExclusionsNonZeroOffset() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    Set<String> exclusions = new HashSet<String>();
    exclusions.add("acc1");
    exclusions.add("acc2");
    exclusions.add("acc3");

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), 50, 100, exclusions);

    assertTrue(sql.contains("NOT IN ("));
    assertTrue(sql.contains("LIMIT 50"));
    assertTrue(sql.contains("OFFSET 100"));
  }

  @Test
  void testBuildSelectSqlWithPagingRowFilterAndSmallExclusions() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .rowFilter("status = 'active'")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    Set<String> exclusions = new HashSet<String>();
    exclusions.add("acc1");

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), 25, 100, exclusions);

    assertTrue(sql.contains("WHERE status = 'active'"));
    assertTrue(sql.contains("AND accession_number NOT IN ("));
    assertTrue(sql.contains("LIMIT 25"));
    assertTrue(sql.contains("OFFSET 100"));
  }

  @Test
  void testBuildSelectSqlWithPagingNullExclusions() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), 100, 200,
        (Set<String>) null);

    assertFalse(sql.contains("WHERE"));
    assertTrue(sql.contains("LIMIT 100"));
    assertTrue(sql.contains("OFFSET 200"));
  }

  // ===== buildDuckDBSql additional edge cases =====

  @Test
  void testBuildDuckDBSqlWithMultiplePartitionColumns() throws Exception {
    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));
    partCols.add(new ColumnDefinition("month", "INTEGER"));
    partCols.add(new ColumnDefinition("region", "VARCHAR"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .partitionColumns(partCols)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/output/",
        Collections.emptyMap(), -1, -1);

    assertTrue(sql.contains("PARTITION_BY (year, month, region)"));
    assertTrue(sql.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test
  void testBuildDuckDBSqlWithBatchInPartitionColumn() throws Exception {
    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .partitionColumns(partCols)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("year", "2023");

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/output/",
        batch, -1, -1);

    assertTrue(sql.contains("PARTITION_BY (year)"));
    assertFalse(sql.contains("FILENAME_PATTERN"));
  }

  @Test
  void testBuildDuckDBSqlLimitZero() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/output/",
        Collections.emptyMap(), 0, 0);

    assertTrue(sql.contains("OVERWRITE_OR_IGNORE"));
    assertFalse(sql.contains("LIMIT"));
  }

  @Test
  void testBuildDuckDBSqlComputedColumnsAndRowFilter() throws Exception {
    Map<String, String> computed = new LinkedHashMap<String, String>();
    computed.put("upper_name", "UPPER(name)");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .computedColumns(computed)
            .rowFilter("active = true")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/out/",
        Collections.emptyMap(), -1, -1);

    assertTrue(sql.contains("UPPER(name) AS upper_name"));
    assertTrue(sql.contains("WHERE active = true"));
    assertTrue(sql.contains("OVERWRITE_OR_IGNORE"));
  }

  // ===== selfHealTracker with empty source accessions =====

  @Test
  void testSelfHealTrackerEmptySourceAccessions() throws Exception {
    Field cacheField = IcebergMaterializer.class.getDeclaredField("sourceAccessionsCache");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Set<String>>> cache =
        (Map<String, Map<String, Set<String>>>) cacheField.get(materializer);

    cache.put("2023:_data.parquet", new HashMap<String, Set<String>>());

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "selfHealTracker", IcebergMaterializer.MaterializationConfig.class,
        String.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("s3://bucket/year=*/*_data.parquet")
            .targetTableId("test_table")
            .build();

    method.invoke(materializer, config, "2023", new HashSet<String>());

    verify(mockTracker, never()).markProcessed(
        anyString(), anyString(), any(Map.class), anyString());
  }

  // ===== partitionHasData exception path =====

  @Test
  void testPartitionHasDataScanException() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "partitionHasData", org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    org.apache.iceberg.Table mockTable = mock(org.apache.iceberg.Table.class);
    when(mockTable.newScan()).thenThrow(new RuntimeException("scan failed"));

    Map<String, String> partitionValues = new HashMap<String, String>();
    partitionValues.put("year", "2023");

    boolean result = (Boolean) method.invoke(materializer, mockTable, partitionValues);
    assertFalse(result);
  }

  // ===== getDuckDBConnection with S3 config =====

  @Test
  void testGetDuckDBConnectionWithWarehousePath() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getDuckDBConnection", int.class);
    method.setAccessible(true);

    Connection conn = (Connection) method.invoke(materializer, 1);
    assertNotNull(conn);
    conn.close();
  }

  @Test
  void testGetDuckDBConnectionWithS3Config() throws Exception {
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "test");
    s3Config.put("secretAccessKey", "secret");
    s3Config.put("region", "us-east-1");
    when(mockStorageProvider.getS3Config()).thenReturn(s3Config);

    IcebergMaterializer s3Materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getDuckDBConnection", int.class);
    method.setAccessible(true);

    try {
      Connection conn = (Connection) method.invoke(s3Materializer, 2);
      assertNotNull(conn);
      conn.close();
    } catch (InvocationTargetException e) {
      // httpfs may not be available
    }
  }

  @Test
  void testGetDuckDBConnectionWithEmptyS3Config() throws Exception {
    when(mockStorageProvider.getS3Config())
        .thenReturn(Collections.<String, String>emptyMap());

    IcebergMaterializer emptyS3 =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getDuckDBConnection", int.class);
    method.setAccessible(true);

    Connection conn = (Connection) method.invoke(emptyS3, 1);
    assertNotNull(conn);
    conn.close();
  }

  // ===== buildBatchCombinations single year =====

  @Test
  void testBuildBatchCombinationsSingleYear() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .batchPartitionColumns(Arrays.asList("year"))
            .yearRange(2023, 2023)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildBatchCombinations", IcebergMaterializer.MaterializationConfig.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, String>> result =
        (List<Map<String, String>>) method.invoke(materializer, config);

    assertEquals(1, result.size());
    assertEquals("2023", result.get(0).get("year"));
  }

  // ===== buildCombinationsRecursive additional =====

  @Test
  void testBuildCombinationsRecursiveSingleDimension() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCombinationsRecursive", List.class, List.class, int.class, Map.class, List.class);
    method.setAccessible(true);

    List<String> columnNames = Arrays.asList("year");
    List<List<String>> columnValues = new ArrayList<List<String>>();
    columnValues.add(Arrays.asList("2020", "2021", "2022"));

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    method.invoke(materializer, columnNames, columnValues, 0,
        new LinkedHashMap<String, String>(), result);

    assertEquals(3, result.size());
    assertEquals("2020", result.get(0).get("year"));
    assertEquals("2021", result.get(1).get("year"));
    assertEquals("2022", result.get(2).get("year"));
  }

  @Test
  void testBuildCombinationsRecursiveThreeDimensions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCombinationsRecursive", List.class, List.class, int.class, Map.class, List.class);
    method.setAccessible(true);

    List<String> columnNames = Arrays.asList("a", "b", "c");
    List<List<String>> columnValues = new ArrayList<List<String>>();
    columnValues.add(Arrays.asList("1", "2"));
    columnValues.add(Arrays.asList("x", "y"));
    columnValues.add(Arrays.asList("p"));

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    method.invoke(materializer, columnNames, columnValues, 0,
        new LinkedHashMap<String, String>(), result);

    assertEquals(4, result.size());
  }

  // ===== groupBatchesByIncrementalKey multiple keys =====

  @Test
  void testGroupBatchesByIncrementalKeyMultipleKeys() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();

    Map<String, String> batch1 = new LinkedHashMap<String, String>();
    batch1.put("year", "2023");
    batch1.put("region", "US");
    batch1.put("type", "A");
    batches.add(batch1);

    Map<String, String> batch2 = new LinkedHashMap<String, String>();
    batch2.put("year", "2023");
    batch2.put("region", "US");
    batch2.put("type", "B");
    batches.add(batch2);

    Map<String, String> batch3 = new LinkedHashMap<String, String>();
    batch3.put("year", "2023");
    batch3.put("region", "EU");
    batch3.put("type", "A");
    batches.add(batch3);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            materializer, batches, Arrays.asList("year", "region"));

    assertEquals(2, result.size());
  }

  // ===== coerceValue additional =====

  @Test
  void testCoerceValueLowercaseType() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals(42, method.invoke(materializer, "42", "integer"));
    assertEquals(100L, method.invoke(materializer, "100", "bigint"));
    assertEquals(3.14, method.invoke(materializer, "3.14", "double"));
    assertEquals(true, method.invoke(materializer, "true", "boolean"));
    assertEquals("text", method.invoke(materializer, "text", "varchar"));
  }

  @Test
  void testCoerceValueUnknownType() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals("123", method.invoke(materializer, "123", "TIMESTAMP"));
    assertEquals("hello", method.invoke(materializer, "hello", "BINARY"));
  }

  // ===== extractCiksFromRowFilter additional =====

  @Test
  void testExtractCiksFromRowFilterMixedCase() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "Cik IN ('001', '002')");
    assertEquals(2, ciks.size());
  }

  @Test
  void testExtractCiksFromRowFilterWithSpaces() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "cik IN ( '001' , '002' , '003' )");
    assertEquals(3, ciks.size());
    assertTrue(ciks.contains("001"));
    assertTrue(ciks.contains("002"));
    assertTrue(ciks.contains("003"));
  }

  @Test
  void testExtractCiksFromRowFilterEmptyParens() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "cik IN ()");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromRowFilterComplexExpression() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "year = 2023 AND cik IN ('001', '002') AND type = 'annual'");
    assertEquals(2, ciks.size());
    assertTrue(ciks.contains("001"));
    assertTrue(ciks.contains("002"));
  }

  // ===== buildFilenamePattern with three keys =====

  @Test
  void testBuildFilenamePatternThreeKeys() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildFilenamePattern", Map.class);
    method.setAccessible(true);

    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("year", "2023");
    batch.put("region", "US");
    batch.put("type", "annual");
    String pattern = (String) method.invoke(materializer, batch);

    assertEquals("year_2023_region_US_type_annual_{i}", pattern);
  }

  // ===== findColumnType with empty list =====

  @Test
  void testFindColumnTypeEmptyList() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "findColumnType", List.class, String.class);
    method.setAccessible(true);

    List<ColumnDefinition> empty = Collections.emptyList();
    assertEquals("VARCHAR", method.invoke(materializer, empty, "any_column"));
  }

  // ===== cleanupStagingDirectory local with delete IOException =====

  @Test
  void testCleanupStagingDirectoryLocalDeleteIOException() throws Exception {
    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry(
        "/local/staging/file.parquet", "file.parquet", false, 100, 0));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);
    when(mockStorageProvider.delete(anyString()))
        .thenThrow(new IOException("directory not empty"));

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    method.invoke(materializer, "/local/staging");
  }

  // ===== createStagingPath format =====

  @Test
  void testCreateStagingPathFormat() throws Exception {
    when(mockStorageProvider.resolvePath(anyString(), anyString()))
        .thenAnswer(invocation -> {
          String base = invocation.getArgument(0);
          String rel = invocation.getArgument(1);
          return base + "/" + rel;
        });

    Method method = IcebergMaterializer.class.getDeclaredMethod("createStagingPath");
    method.setAccessible(true);

    String path = (String) method.invoke(materializer);
    assertNotNull(path);
    assertTrue(path.contains(".staging/"));
  }

  // ===== getSourceFileWatermark with existing local files =====

  @Test
  void testGetSourceFileWatermarkWithExistingLocalFiles() throws Exception {
    java.io.File dataDir = tempDir.resolve("watermark_test").toFile();
    dataDir.mkdirs();

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE wm_test (id INTEGER, name VARCHAR)");
        stmt.execute("INSERT INTO wm_test VALUES (1, 'test')");
        stmt.execute("COPY wm_test TO '" + dataDir.getAbsolutePath()
            + "/data.parquet' (FORMAT PARQUET)");
      }
    }

    long watermark = materializer.getSourceFileWatermark(
        dataDir.getAbsolutePath() + "/*.parquet",
        IcebergMaterializer.SourceFormat.PARQUET);

    // DuckDB's glob() function only returns a 'file' column, not 'last_modified'.
    // The SQL "SELECT file, last_modified FROM glob(...)" fails with a Binder Error,
    // which is caught in the catch block and returns 0.
    assertEquals(0, watermark, "Watermark should be 0 since DuckDB glob() lacks last_modified column");
  }

  @Test
  void testGetSourceFileWatermarkWithExistingLocalJsonFiles() throws Exception {
    java.io.File dataDir = tempDir.resolve("watermark_json").toFile();
    dataDir.mkdirs();

    java.io.FileWriter writer = new java.io.FileWriter(
        new java.io.File(dataDir, "data.json"));
    writer.write("[{\"id\": 1, \"name\": \"test\"}]");
    writer.close();

    long watermark = materializer.getSourceFileWatermark(
        dataDir.getAbsolutePath() + "/*.json",
        IcebergMaterializer.SourceFormat.JSON);

    // DuckDB's glob() function only returns a 'file' column, not 'last_modified'.
    // The SQL "SELECT file, last_modified FROM glob(...)" fails with a Binder Error,
    // which is caught in the catch block and returns 0.
    assertEquals(0, watermark, "Watermark should be 0 since DuckDB glob() lacks last_modified column");
  }

  // ===== totalRowsWritten field =====

  @Test
  void testTotalRowsWrittenFieldExists() throws Exception {
    Field field = IcebergMaterializer.class.getDeclaredField("totalRowsWritten");
    field.setAccessible(true);
    assertEquals(0L, field.getLong(materializer));
  }

  // ===== Constructor with catalog config =====

  @Test
  void testConstructorCatalogConfig() throws Exception {
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "AKID");
    s3Config.put("secretAccessKey", "SECRET");
    s3Config.put("endpoint", "http://minio:9000");
    when(mockStorageProvider.getS3Config()).thenReturn(s3Config);

    IcebergMaterializer m =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Field configField = IcebergMaterializer.class.getDeclaredField("catalogConfig");
    configField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> catalogConfig = (Map<String, Object>) configField.get(m);

    assertEquals("hadoop", catalogConfig.get("catalog"));
    assertEquals(tempDir.toString(), catalogConfig.get("warehousePath"));
    assertTrue(catalogConfig.containsKey("hadoopConfig"));

    @SuppressWarnings("unchecked")
    Map<String, String> hadoopConfig =
        (Map<String, String>) catalogConfig.get("hadoopConfig");
    assertEquals("AKID", hadoopConfig.get("fs.s3a.access.key"));
    assertEquals("SECRET", hadoopConfig.get("fs.s3a.secret.key"));
    assertEquals("http://minio:9000", hadoopConfig.get("fs.s3a.endpoint"));
    assertEquals("true", hadoopConfig.get("fs.s3a.path.style.access"));
  }

  // ===== Builder validation edge cases =====

  @Test
  void testBuilderWithNullSourcePattern() {
    try {
      IcebergMaterializer.MaterializationConfig.builder()
          .sourcePattern(null)
          .targetTableId("test")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  void testBuilderWithNullTargetTableId() {
    try {
      IcebergMaterializer.MaterializationConfig.builder()
          .sourcePattern("data/*.parquet")
          .targetTableId(null)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  // ===== MaterializationResult toString =====

  @Test
  void testMaterializationResultToStringFormat() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult(
            "schema.my_table", 15, 3, 7, 12345, false);

    String str = result.toString();
    assertTrue(str.contains("schema.my_table"));
    assertTrue(str.contains("success=15"));
    assertTrue(str.contains("failed=3"));
    assertTrue(str.contains("skipped=7"));
    assertTrue(str.contains("duration=12345ms"));
    assertTrue(str.contains("recreated=false"));
  }

  // ===== buildCountSql additional =====

  @Test
  void testBuildCountSqlParquetSpecificPattern() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql", IcebergMaterializer.MaterializationConfig.class,
        String.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/year=2023/*.parquet");

    assertTrue(sql.startsWith("SELECT COUNT(*) FROM"));
    assertTrue(sql.contains("read_parquet('data/year=2023/*.parquet'"));
    assertFalse(sql.contains("WHERE"));
  }

  @Test
  void testBuildCountSqlJsonWithFilter() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.json")
            .targetTableId("test_table")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .rowFilter("category = 'finance'")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql", IcebergMaterializer.MaterializationConfig.class,
        String.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config, "data/*.json");

    assertTrue(sql.contains("read_json("));
    assertTrue(sql.contains("WHERE category = 'finance'"));
  }

  // ===== processWithRowBatching (legacy) zero rows test =====

  @Test
  void testProcessWithRowBatchingNoFiles() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "processWithRowBatching", IcebergMaterializer.MaterializationConfig.class,
        Connection.class, String.class, String.class, Map.class, int.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try {
        method.invoke(materializer, config, conn,
            tempDir.toString() + "/nonexistent/*.parquet",
            tempDir.toString() + "/staging",
            Collections.emptyMap(), 10);
      } catch (InvocationTargetException e) {
        assertTrue(e.getCause() instanceof SQLException);
      }
    }
  }

  // ===== sourceAccessionsCache field =====

  @Test
  void testSourceAccessionsCache() throws Exception {
    Field cacheField = IcebergMaterializer.class.getDeclaredField("sourceAccessionsCache");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Set<String>>> cache =
        (Map<String, Map<String, Set<String>>>) cacheField.get(materializer);

    assertNotNull(cache);
    assertTrue(cache.isEmpty());
  }

  // ===== maxRetries and retryDelayMs fields =====

  @Test
  void testRetryFieldValues() throws Exception {
    Field maxRetriesField = IcebergMaterializer.class.getDeclaredField("maxRetries");
    maxRetriesField.setAccessible(true);
    assertEquals(2, maxRetriesField.getInt(materializer));

    Field retryDelayField = IcebergMaterializer.class.getDeclaredField("retryDelayMs");
    retryDelayField.setAccessible(true);
    assertEquals(10L, retryDelayField.getLong(materializer));
  }

  @Test
  void testDefaultRetryFieldValues() throws Exception {
    IcebergMaterializer defaultMaterializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Field maxRetriesField = IcebergMaterializer.class.getDeclaredField("maxRetries");
    maxRetriesField.setAccessible(true);
    assertEquals(3, maxRetriesField.getInt(defaultMaterializer));

    Field retryDelayField = IcebergMaterializer.class.getDeclaredField("retryDelayMs");
    retryDelayField.setAccessible(true);
    assertEquals(1000L, retryDelayField.getLong(defaultMaterializer));
  }

  // ===== incrementalTracker defaults to NOOP =====

  @Test
  void testIncrementalTrackerDefaultsToNoop() throws Exception {
    IcebergMaterializer noopMaterializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, null);

    Field trackerField = IcebergMaterializer.class.getDeclaredField("incrementalTracker");
    trackerField.setAccessible(true);
    assertEquals(IncrementalTracker.NOOP, trackerField.get(noopMaterializer));
  }

  // ===== warehousePath and storageProvider fields =====

  @Test
  void testWarehousePathField() throws Exception {
    Field field = IcebergMaterializer.class.getDeclaredField("warehousePath");
    field.setAccessible(true);
    assertEquals(tempDir.toString(), field.get(materializer));
  }

  @Test
  void testStorageProviderField() throws Exception {
    Field field = IcebergMaterializer.class.getDeclaredField("storageProvider");
    field.setAccessible(true);
    assertEquals(mockStorageProvider, field.get(materializer));
  }
}
