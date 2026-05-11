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
package org.apache.calcite.adapter.file.format.parquet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for ParquetConversionUtil covering cache directory management,
 * file path resolution, and conversion utilities.
 */
@Tag("unit")
public class ParquetConversionUtilCoverageTest {

  @TempDir
  java.nio.file.Path tempDir;

  @Test void testGetParquetCacheDirDefault() {
    File baseDir = tempDir.toFile();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir);

    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().endsWith(".parquet_cache"));
    assertTrue(cacheDir.exists());
  }

  @Test void testGetParquetCacheDirWithCustomDir() {
    File baseDir = tempDir.toFile();
    String customDir = new File(tempDir.toFile(), "custom_cache").getAbsolutePath();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir);

    assertNotNull(cacheDir);
    assertEquals(customDir, cacheDir.getAbsolutePath());
    assertTrue(cacheDir.exists());
  }

  @Test void testGetParquetCacheDirWithSchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = new File(tempDir.toFile(), "cache").getAbsolutePath();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(
        baseDir, customDir, "myschema");

    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().contains("schema_myschema"));
    assertTrue(cacheDir.exists());
  }

  @Test void testGetParquetCacheDirNullCustomDir() {
    File baseDir = tempDir.toFile();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, null);

    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().endsWith(".parquet_cache"));
  }

  @Test void testGetParquetCacheDirEmptyCustomDir() {
    File baseDir = tempDir.toFile();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, "");

    assertNotNull(cacheDir);
    // Empty string should fall back to default
    assertTrue(cacheDir.getAbsolutePath().endsWith(".parquet_cache"));
  }

  @Test void testGetParquetCacheDirNullSchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = new File(tempDir.toFile(), "cache2").getAbsolutePath();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(
        baseDir, customDir, null);

    assertNotNull(cacheDir);
    // Without schema name, should use custom dir directly
    assertEquals(customDir, cacheDir.getAbsolutePath());
  }

  @Test void testGetParquetCacheDirEmptySchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = new File(tempDir.toFile(), "cache3").getAbsolutePath();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(
        baseDir, customDir, "");

    assertNotNull(cacheDir);
    // Empty schema name should use custom dir directly
    assertEquals(customDir, cacheDir.getAbsolutePath());
  }

  @Test void testGetCachedParquetFile() throws IOException {
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache");
    cacheDir.mkdirs();

    File sourceFile = new File(tempDir.toFile(), "data.csv");
    try (FileWriter writer = new FileWriter(sourceFile)) {
      writer.write("a,b\n1,2\n");
    }

    File result = ParquetConversionUtil.getCachedParquetFile(
        sourceFile, cacheDir, false, "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
    assertTrue(result.getAbsolutePath().contains(".parquet_cache"));
  }

  @Test void testGetCachedParquetFileWithJson() throws IOException {
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache");
    cacheDir.mkdirs();

    File sourceFile = new File(tempDir.toFile(), "data.json");
    try (FileWriter writer = new FileWriter(sourceFile)) {
      writer.write("[{\"a\":1}]");
    }

    File result = ParquetConversionUtil.getCachedParquetFile(
        sourceFile, cacheDir, false, "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
    assertEquals("data.parquet", result.getName());
  }

  @Test void testGetCachedParquetFileNoExtension() throws IOException {
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache");
    cacheDir.mkdirs();

    File sourceFile = new File(tempDir.toFile(), "datafile");
    try (FileWriter writer = new FileWriter(sourceFile)) {
      writer.write("a,b\n1,2\n");
    }

    File result = ParquetConversionUtil.getCachedParquetFile(
        sourceFile, cacheDir, false, "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
  }

  @Test void testGetCachedParquetFileMultipleExtensions() throws IOException {
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache");
    cacheDir.mkdirs();

    File sourceFile = new File(tempDir.toFile(), "data.test.csv");
    try (FileWriter writer = new FileWriter(sourceFile)) {
      writer.write("a,b\n1,2\n");
    }

    File result = ParquetConversionUtil.getCachedParquetFile(
        sourceFile, cacheDir, false, "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
    // Should strip last extension only
    assertTrue(result.getName().startsWith("data"));
  }

  @Test void testGetParquetCacheDirCreatesDirectory() {
    File baseDir = new File(tempDir.toFile(), "nonexistent");
    // baseDir doesn't exist yet
    assertNotNull(baseDir);

    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir);

    // Should have created the directory
    assertTrue(cacheDir.exists());
    assertTrue(cacheDir.isDirectory());
  }

  @Test void testGetParquetCacheDirWithBothNulls() {
    File baseDir = tempDir.toFile();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(
        baseDir, null, null);

    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().endsWith(".parquet_cache"));
  }

  @Test void testGetCachedParquetFileWithSpecialChars() throws IOException {
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache");
    cacheDir.mkdirs();

    // Create a file with characters that need sanitization
    File sourceFile = new File(tempDir.toFile(), "my data file.csv");
    try (FileWriter writer = new FileWriter(sourceFile)) {
      writer.write("a,b\n1,2\n");
    }

    File result = ParquetConversionUtil.getCachedParquetFile(
        sourceFile, cacheDir, false, "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
  }
}
