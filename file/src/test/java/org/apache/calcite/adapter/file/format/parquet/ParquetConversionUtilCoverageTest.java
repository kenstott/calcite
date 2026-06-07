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
    File cacheDir =
        ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, "myschema");

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
    File cacheDir =
        ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, null);

    assertNotNull(cacheDir);
    // Without schema name, should use custom dir directly
    assertEquals(customDir, cacheDir.getAbsolutePath());
  }

  @Test void testGetParquetCacheDirEmptySchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = new File(tempDir.toFile(), "cache3").getAbsolutePath();
    File cacheDir =
        ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, "");

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

    File result =
        ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "UNCHANGED");

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

    File result =
        ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "UNCHANGED");

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

    File result =
        ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "UNCHANGED");

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

    File result =
        ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "UNCHANGED");

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
    File cacheDir =
        ParquetConversionUtil.getParquetCacheDir(baseDir, null, null);

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

    File result =
        ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
  }
}
