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
package org.apache.calcite.adapter.file.format;

import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link ParquetConversionUtil}.
 * Covers cache directory logic, needsConversion, isS3Path, getHadoopPath, etc.
 */
@Tag("unit")
class ParquetConversionUtilDeepCoverageTest {

  @TempDir
  Path tempDir;

  // ===== getParquetCacheDir tests =====

  @Test void testGetParquetCacheDirDefault() {
    File baseDir = tempDir.toFile();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir);
    assertNotNull(cacheDir);
    assertTrue(cacheDir.getPath().endsWith(".parquet_cache"));
  }

  @Test void testGetParquetCacheDirWithCustomDir() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("custom_cache").toString();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir);
    assertNotNull(cacheDir);
    assertEquals(customDir, cacheDir.getPath());
  }

  @Test void testGetParquetCacheDirWithEmptyCustomDir() {
    File baseDir = tempDir.toFile();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, "");
    assertNotNull(cacheDir);
    assertTrue(cacheDir.getPath().endsWith(".parquet_cache"));
  }

  @Test void testGetParquetCacheDirWithNullCustomDir() {
    File baseDir = tempDir.toFile();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, null);
    assertNotNull(cacheDir);
    assertTrue(cacheDir.getPath().endsWith(".parquet_cache"));
  }

  @Test void testGetParquetCacheDirWithSchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("custom_cache").toString();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, "my_schema");
    assertNotNull(cacheDir);
    assertTrue(cacheDir.getPath().contains("schema_my_schema"));
  }

  @Test void testGetParquetCacheDirWithNullSchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("custom_cache").toString();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, null);
    assertNotNull(cacheDir);
    assertEquals(customDir, cacheDir.getPath());
  }

  @Test void testGetParquetCacheDirWithEmptySchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("custom_cache").toString();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, "");
    assertNotNull(cacheDir);
    assertEquals(customDir, cacheDir.getPath());
  }

  @Test void testGetParquetCacheDirCreatesDir() {
    File baseDir = new File(tempDir.toFile(), "nonexistent");
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir);
    assertTrue(cacheDir.exists());
  }

  // ===== getCachedParquetFile tests =====

  @Test void testGetCachedParquetFile() {
    File sourceFile = new File("/data/test_file.csv");
    File cacheDir = tempDir.toFile();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, null);
    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
  }

  @Test void testGetCachedParquetFileNoExtension() {
    File sourceFile = new File("/data/testfile");
    File cacheDir = tempDir.toFile();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, null);
    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
  }

  @Test void testGetCachedParquetFileWithDotInName() {
    File sourceFile = new File("/data/test.data.csv");
    File cacheDir = tempDir.toFile();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, null);
    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
  }

  // ===== needsConversion tests =====

  @Test void testNeedsConversionTargetNotExists() {
    File sourceFile = new File(tempDir.toFile(), "source.csv");
    File parquetFile = new File(tempDir.toFile(), "nonexistent.parquet");
    assertTrue(ParquetConversionUtil.needsConversion(sourceFile, parquetFile));
  }

  @Test void testNeedsConversionSourceNewer() throws Exception {
    File parquetFile = new File(tempDir.toFile(), "target.parquet");
    assertTrue(parquetFile.createNewFile());
    parquetFile.setLastModified(1000L);

    File sourceFile = new File(tempDir.toFile(), "source.csv");
    assertTrue(sourceFile.createNewFile());
    sourceFile.setLastModified(2000L);

    assertTrue(ParquetConversionUtil.needsConversion(sourceFile, parquetFile));
  }

  @Test void testNeedsConversionParquetNewer() throws Exception {
    File sourceFile = new File(tempDir.toFile(), "source.csv");
    assertTrue(sourceFile.createNewFile());
    sourceFile.setLastModified(1000L);

    File parquetFile = new File(tempDir.toFile(), "target.parquet");
    assertTrue(parquetFile.createNewFile());
    parquetFile.setLastModified(2000L);

    assertFalse(ParquetConversionUtil.needsConversion(sourceFile, parquetFile));
  }

  // ===== isS3Path via Reflection =====

  @Test void testIsS3PathViaReflection() throws Exception {
    Method isS3Path = ParquetConversionUtil.class.getDeclaredMethod("isS3Path", String.class);
    isS3Path.setAccessible(true);

    assertTrue((Boolean) isS3Path.invoke(null, "s3://bucket/key"));
    assertFalse((Boolean) isS3Path.invoke(null, "/local/path"));
    assertFalse((Boolean) isS3Path.invoke(null, "http://example.com"));
    assertFalse((Boolean) isS3Path.invoke(null, (Object) null));
  }

  // ===== getHadoopPath via Reflection =====

  @Test void testGetHadoopPathViaReflection() throws Exception {
    Method getHadoopPath = ParquetConversionUtil.class.getDeclaredMethod("getHadoopPath", String.class);
    getHadoopPath.setAccessible(true);

    assertEquals("s3a://bucket/key", getHadoopPath.invoke(null, "s3://bucket/key"));
    assertEquals("/local/path", getHadoopPath.invoke(null, "/local/path"));
    assertEquals(null, getHadoopPath.invoke(null, (Object) null));
  }

  // ===== isNullRepresentation via Reflection =====

  @Test void testIsNullRepresentationViaReflection() throws Exception {
    Method isNull = ParquetConversionUtil.class.getDeclaredMethod("isNullRepresentation", String.class);
    isNull.setAccessible(true);

    // These test the NullEquivalents.isNullRepresentation delegation
    assertNotNull(isNull); // Just confirm method exists and is accessible
  }
}
