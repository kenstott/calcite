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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ParquetConversionUtil}.
 */
@Tag("unit")
public class ParquetConversionUtilTest {

  @TempDir
  public File tempDir;

  @Test void testGetParquetCacheDirDefaultReturnsParquetCache() {
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(tempDir);
    assertNotNull(cacheDir);
    assertEquals(".parquet_cache", cacheDir.getName());
    assertEquals(tempDir, cacheDir.getParentFile());
    assertTrue(cacheDir.exists(), "Cache directory should be created");
    assertTrue(cacheDir.isDirectory(), "Cache directory should be a directory");
  }

  @Test void testGetParquetCacheDirWithCustomPath() {
    File customDir = new File(tempDir, "my_custom_cache");
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(tempDir, customDir.getAbsolutePath());
    assertEquals(customDir.getAbsolutePath(), cacheDir.getAbsolutePath());
    assertTrue(cacheDir.exists(), "Custom cache directory should be created");
  }

  @Test void testGetParquetCacheDirWithSchemaName() {
    File customDir = new File(tempDir, "shared_cache");
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(
        tempDir, customDir.getAbsolutePath(), "myschema");
    assertEquals("schema_myschema", cacheDir.getName());
    assertEquals(customDir.getAbsolutePath(), cacheDir.getParentFile().getAbsolutePath());
    assertTrue(cacheDir.exists(), "Schema-specific cache directory should be created");
  }

  @Test void testGetParquetCacheDirWithNullCustomFallsBackToDefault() {
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(tempDir, null);
    assertEquals(".parquet_cache", cacheDir.getName());
    assertEquals(tempDir, cacheDir.getParentFile());
  }

  @Test void testGetParquetCacheDirWithEmptyCustomFallsBackToDefault() {
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(tempDir, "");
    assertEquals(".parquet_cache", cacheDir.getName());
    assertEquals(tempDir, cacheDir.getParentFile());
  }

  @Test void testGetCachedParquetFileBasicName() {
    File sourceFile = new File(tempDir, "sales.csv");
    File cacheDir = new File(tempDir, "cache");
    cacheDir.mkdirs();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "UNCHANGED");
    assertEquals("sales.parquet", result.getName());
    assertEquals(cacheDir, result.getParentFile());
  }

  @Test void testGetCachedParquetFileStripsOnlyLastExtension() {
    File sourceFile = new File(tempDir, "report.xlsx");
    File cacheDir = new File(tempDir, "cache");
    cacheDir.mkdirs();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "UNCHANGED");
    assertEquals("report.parquet", result.getName());
  }

  @Test void testGetCachedParquetFileNoExtension() {
    File sourceFile = new File(tempDir, "datafile");
    File cacheDir = new File(tempDir, "cache");
    cacheDir.mkdirs();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "UNCHANGED");
    assertEquals("datafile.parquet", result.getName());
  }

  @Test void testGetCachedParquetFileMultipleDots() {
    File sourceFile = new File(tempDir, "my.data.file.csv");
    File cacheDir = new File(tempDir, "cache");
    cacheDir.mkdirs();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "UNCHANGED");
    // sanitizeIdentifier replaces dots with underscores
    assertEquals("my_data_file.parquet", result.getName());
  }

  @Test void testCacheDirCreatedIfNotExists() {
    File baseDir = new File(tempDir, "newbase");
    baseDir.mkdirs();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir);
    assertTrue(cacheDir.exists(), "Cache dir should be auto-created by getParquetCacheDir");
    assertTrue(cacheDir.isDirectory());
  }

  @Test void testGetParquetCacheDirWithSchemaNameNoCustomDir() {
    // When customCacheDir is null but schemaName is provided, should use default .parquet_cache
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(tempDir, null, "testschema");
    assertEquals(".parquet_cache", cacheDir.getName());
    assertTrue(cacheDir.exists());
  }
}
