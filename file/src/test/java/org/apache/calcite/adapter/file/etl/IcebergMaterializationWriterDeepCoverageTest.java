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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link IcebergMaterializationWriter} targeting uncovered methods:
 * constructor, initialize validation, buildCatalogConfig, convertToS3aScheme,
 * buildHadoopS3Config, buildHadoopConfiguration, mapToIcebergType,
 * getEnvInt, and error handling paths.
 */
@Tag("unit")
public class IcebergMaterializationWriterDeepCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider mockStorage;
  private IncrementalTracker mockTracker;

  @BeforeEach
  void setUp() {
    mockStorage = mock(StorageProvider.class);
    mockTracker = mock(IncrementalTracker.class);
  }

  // --- Constructor ---

  @Test void testConstructor() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);
    assertNotNull(writer);
  }

  @Test void testConstructorNullTracker() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", null);
    assertNotNull(writer);
  }

  // --- initialize validation ---

  @Test void testInitializeNullConfig() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(null));
  }

  @Test void testInitializeDisabledConfig() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    assertThrows(IOException.class, () -> writer.initialize(config));
  }

  @Test void testInitializeWrongFormat() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder().pattern("test/").build())
        .build();
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(config));
  }

  // --- convertToS3aScheme via reflection ---

  @Test void testConvertToS3aScheme() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("convertToS3aScheme", String.class);
    method.setAccessible(true);

    assertEquals("s3a://bucket/path", method.invoke(writer, "s3://bucket/path"));
    assertEquals("/local/path", method.invoke(writer, "/local/path"));
    assertNull(method.invoke(writer, (Object) null));
  }

  // --- buildHadoopS3Config via reflection ---

  @Test void testBuildHadoopS3Config() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("buildHadoopS3Config", Map.class);
    method.setAccessible(true);

    Map<String, String> s3Config = new HashMap<>();
    s3Config.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    s3Config.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    s3Config.put("endpoint", "https://s3.example.com");
    s3Config.put("region", "us-east-1");

    @SuppressWarnings("unchecked")
    Map<String, String> result = (Map<String, String>) method.invoke(writer, s3Config);

    assertNotNull(result);
    assertEquals("AKIAIOSFODNN7EXAMPLE", result.get("fs.s3a.access.key"));
    assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", result.get("fs.s3a.secret.key"));
    assertEquals("https://s3.example.com", result.get("fs.s3a.endpoint"));
    assertEquals("true", result.get("fs.s3a.path.style.access"));
    assertEquals("us-east-1", result.get("fs.s3a.endpoint.region"));
    assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", result.get("fs.s3a.impl"));
  }

  @Test void testBuildHadoopS3ConfigMinimal() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("buildHadoopS3Config", Map.class);
    method.setAccessible(true);

    // Only required fields
    Map<String, String> s3Config = new HashMap<>();

    @SuppressWarnings("unchecked")
    Map<String, String> result = (Map<String, String>) method.invoke(writer, s3Config);

    assertNotNull(result);
    assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", result.get("fs.s3a.impl"));
    assertNull(result.get("fs.s3a.access.key"));
    assertNull(result.get("fs.s3a.endpoint"));
  }

  // --- mapToIcebergType via reflection ---

  @Test void testMapToIcebergType() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("mapToIcebergType", String.class);
    method.setAccessible(true);

    assertEquals("STRING", method.invoke(writer, "VARCHAR"));
    assertEquals("STRING", method.invoke(writer, "varchar"));
    assertEquals("STRING", method.invoke(writer, "TEXT"));
    assertEquals("STRING", method.invoke(writer, "STRING"));
    assertEquals("INT", method.invoke(writer, "INTEGER"));
    assertEquals("INT", method.invoke(writer, "INT"));
    assertEquals("LONG", method.invoke(writer, "BIGINT"));
    assertEquals("DOUBLE", method.invoke(writer, "DOUBLE"));
    assertEquals("DOUBLE", method.invoke(writer, "FLOAT"));
    assertEquals("BOOLEAN", method.invoke(writer, "BOOLEAN"));
    assertEquals("DATE", method.invoke(writer, "DATE"));
    assertEquals("TIMESTAMP", method.invoke(writer, "TIMESTAMP"));
  }

  // --- buildCatalogConfig via reflection ---

  @Test void testBuildCatalogConfigNullIcebergConfig() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(writer, (Object) null);
    assertNotNull(result);
    assertEquals("hadoop", result.get("catalog"));
    assertEquals("/warehouse", result.get("warehousePath"));
  }

  @Test void testBuildCatalogConfigHadoopType() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    MaterializeConfig.IcebergConfig icebergConfig = MaterializeConfig.IcebergConfig.builder()
        .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
        .warehousePath("/custom/warehouse")
        .build();

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(writer, icebergConfig);
    assertNotNull(result);
    assertEquals("hadoop", result.get("catalog"));
    assertEquals("/custom/warehouse", result.get("warehousePath"));
  }

  @Test void testBuildCatalogConfigRestType() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    MaterializeConfig.IcebergConfig icebergConfig = MaterializeConfig.IcebergConfig.builder()
        .catalogType(MaterializeConfig.IcebergConfig.CatalogType.REST)
        .restUri("http://rest-catalog:8181")
        .build();

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(writer, icebergConfig);
    assertNotNull(result);
    assertEquals("rest", result.get("catalog"));
    assertEquals("http://rest-catalog:8181", result.get("uri"));
  }

  @Test void testBuildCatalogConfigHiveType() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    MaterializeConfig.IcebergConfig icebergConfig = MaterializeConfig.IcebergConfig.builder()
        .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HIVE)
        .build();

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(writer, icebergConfig);
    assertNotNull(result);
    assertEquals("hive", result.get("catalog"));
  }

  @Test void testBuildCatalogConfigS3WarehousePath() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "s3://my-bucket/warehouse", mockTracker);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(writer, (Object) null);
    assertNotNull(result);
    // Should convert s3:// to s3a://
    assertEquals("s3a://my-bucket/warehouse", result.get("warehousePath"));
  }

  @Test void testBuildCatalogConfigWithS3Credentials() throws Exception {
    Map<String, String> s3Config = new HashMap<>();
    s3Config.put("accessKeyId", "AKID");
    s3Config.put("secretAccessKey", "SECRET");

    when(mockStorage.getS3Config()).thenReturn(s3Config);

    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(writer, (Object) null);
    assertNotNull(result);
    assertTrue(result.containsKey("hadoopConfig"));
  }

  // --- buildHadoopConfiguration via reflection ---

  @Test void testBuildHadoopConfiguration() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    // First set catalogConfig
    Field catalogConfigField = IcebergMaterializationWriter.class.getDeclaredField("catalogConfig");
    catalogConfigField.setAccessible(true);

    Map<String, Object> catalogConfig = new HashMap<>();
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.s3a.access.key", "AKID");
    catalogConfig.put("hadoopConfig", hadoopConfig);
    catalogConfigField.set(writer, catalogConfig);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("buildHadoopConfiguration");
    method.setAccessible(true);

    Object conf = method.invoke(writer);
    assertNotNull(conf);
  }

  @Test void testBuildHadoopConfigurationNoHadoopConfig() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(mockStorage, "/warehouse", mockTracker);

    Field catalogConfigField = IcebergMaterializationWriter.class.getDeclaredField("catalogConfig");
    catalogConfigField.setAccessible(true);

    Map<String, Object> catalogConfig = new HashMap<>();
    // No hadoopConfig entry
    catalogConfigField.set(writer, catalogConfig);

    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("buildHadoopConfiguration");
    method.setAccessible(true);

    Object conf = method.invoke(writer);
    assertNotNull(conf);
  }

  // --- getEnvInt static method ---

  @Test void testGetEnvIntDefault() throws Exception {
    Method method = IcebergMaterializationWriter.class
        .getDeclaredMethod("getEnvInt", String.class, int.class);
    method.setAccessible(true);

    // Non-existent env var should return default
    int result = (int) method.invoke(null, "NONEXISTENT_ENV_VAR_12345", 42);
    assertEquals(42, result);
  }
}
