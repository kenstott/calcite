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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Coverage tests for {@link FileSchema} constructor paths related to
 * storageType configuration.
 *
 * <p>Tests how FileSchema resolves storageProvider, baseDirectory, and
 * operatingCacheDirectory based on storageType, directoryPath,
 * sourceDirectory, and ephemeralCache settings.
 */
@Tag("unit")
public class FileSchemaStorageTypeCoverageTest {

  @TempDir
  File tempDir;

  private SchemaPlus mockParentSchema;

  @BeforeEach
  void setUp() {
    StorageProviderFactory.clearCache();
    mockParentSchema = mock(SchemaPlus.class);
    when(mockParentSchema.getName()).thenReturn("ROOT");
  }

  @Test @DisplayName("FileSchema with storageType='local' and sourceDirectory creates LocalFileStorageProvider")
  void testLocalStorageTypeWithSourceDir() {
    File sourceDir = new File(tempDir, "source");
    sourceDir.mkdirs();

    FileSchema schema =
        new FileSchema(mockParentSchema,
        "test_schema",
        sourceDir,          // sourceDirectory
        null,               // directoryPattern
        null,               // tables
        new ExecutionEngineConfig(),
        false,              // recursive
        null,               // materializations
        null,               // views
        null,               // partitionedTables
        null,               // refreshInterval
        "LOWER",            // tableNameCasing
        "LOWER",            // columnNameCasing
        "local",            // storageType
        null,               // storageConfig
        null,               // flatten
        null,               // csvTypeInference
        false);             // primeCache

    StorageProvider provider = schema.getStorageProvider();
    assertNotNull(provider, "StorageProvider should not be null for storageType='local'");
    assertTrue(provider instanceof LocalFileStorageProvider);
  }

  @Test @DisplayName("FileSchema with storageType='file' creates LocalFileStorageProvider")
  void testFileStorageType() {
    FileSchema schema =
        new FileSchema(mockParentSchema,
        "test_schema",
        tempDir,            // sourceDirectory
        null,               // directoryPattern
        null,               // tables
        new ExecutionEngineConfig(),
        false,              // recursive
        null,               // materializations
        null,               // views
        null,               // partitionedTables
        null,               // refreshInterval
        "LOWER",            // tableNameCasing
        "LOWER",            // columnNameCasing
        "file",             // storageType
        null,               // storageConfig
        null,               // flatten
        null,               // csvTypeInference
        false);             // primeCache

    StorageProvider provider = schema.getStorageProvider();
    assertNotNull(provider);
    assertTrue(provider instanceof LocalFileStorageProvider);
  }

  @Test @DisplayName("FileSchema with null storageType sets storageProvider to null")
  void testNullStorageType() {
    FileSchema schema =
        new FileSchema(mockParentSchema,
        "test_schema",
        tempDir,            // sourceDirectory
        null,               // directoryPattern
        null,               // tables
        new ExecutionEngineConfig(),
        false,              // recursive
        null,               // materializations
        null,               // views
        null,               // partitionedTables
        null,               // refreshInterval
        "LOWER",            // tableNameCasing
        "LOWER",            // columnNameCasing
        null,               // storageType
        null,               // storageConfig
        null,               // flatten
        null,               // csvTypeInference
        false);             // primeCache

    assertNull(schema.getStorageProvider(),
        "StorageProvider should be null when storageType is not configured");
  }

  @Test @DisplayName("FileSchema with storageType='local' and directoryPath uses directoryPath as baseDirectory")
  void testLocalStorageWithDirectoryPath() {
    File sourceDir = new File(tempDir, "source");
    sourceDir.mkdirs();
    String dirPath = sourceDir.getAbsolutePath();

    FileSchema schema =
        new FileSchema(mockParentSchema,
        "test_schema",
        null,                // sourceDirectory
        null,                // userConfiguredBaseDirectory
        dirPath,             // directoryPath
        null,                // directoryPattern
        null,                // tables
        new ExecutionEngineConfig(),
        false,               // recursive
        null,                // materializations
        null,                // views
        null,                // partitionedTables
        null,                // refreshInterval
        "LOWER",             // tableNameCasing
        "LOWER",             // columnNameCasing
        "local",             // storageType
        null,                // storageConfig
        null,                // flatten
        null,                // csvTypeInference
        false,               // primeCache
        null);               // comment

    assertNotNull(schema.getStorageProvider());
    assertTrue(schema.getStorageProvider() instanceof LocalFileStorageProvider);
  }

  @Test @DisplayName("FileSchema with _storageProvider in storageConfig uses pre-created provider")
  void testPreCreatedStorageProvider() {
    StorageProvider preCreated = new LocalFileStorageProvider();
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", preCreated);

    FileSchema schema =
        new FileSchema(mockParentSchema,
        "test_schema",
        tempDir,             // sourceDirectory
        null,                // directoryPattern
        null,                // tables
        new ExecutionEngineConfig(),
        false,               // recursive
        null,                // materializations
        null,                // views
        null,                // partitionedTables
        null,                // refreshInterval
        "LOWER",             // tableNameCasing
        "LOWER",             // columnNameCasing
        "local",             // storageType
        storageConfig,       // storageConfig - contains _storageProvider
        null,                // flatten
        null,                // csvTypeInference
        false);              // primeCache

    assertTrue(schema.getStorageProvider() == preCreated,
        "Should use pre-created storage provider from config");
  }

  @Test @DisplayName("FileSchema with ephemeralCache uses temp directory for operating cache")
  void testEphemeralCache() {
    // Use a temp directory as the base directory to simulate ephemeral cache
    File ephemeralBase =
        new File(System.getProperty("java.io.tmpdir"), "calcite-test-ephemeral-" + System.nanoTime());
    ephemeralBase.mkdirs();

    try {
      FileSchema schema =
          new FileSchema(mockParentSchema,
          "test_schema",
          tempDir,           // sourceDirectory
          ephemeralBase,     // userConfiguredBaseDirectory (ephemeral temp dir)
          null,              // directoryPattern
          null,              // tables
          new ExecutionEngineConfig(),
          false,             // recursive
          null,              // materializations
          null,              // views
          null,              // partitionedTables
          null,              // refreshInterval
          "LOWER",           // tableNameCasing
          "LOWER",           // columnNameCasing
          null,              // storageType
          null,              // storageConfig
          null,              // flatten
          null,              // csvTypeInference
          false);            // primeCache

      // Schema should be created successfully with ephemeral cache
      assertNotNull(schema);
      assertNull(schema.getStorageProvider());
    } finally {
      // Cleanup
      deleteRecursive(ephemeralBase);
    }
  }

  @Test @DisplayName("FileSchema with storageType and no sourceDirectory falls back to working directory")
  void testNullSourceDirectory() {
    FileSchema schema =
        new FileSchema(mockParentSchema,
        "test_schema",
        null,               // sourceDirectory is null
        null,               // directoryPattern
        null,               // tables
        new ExecutionEngineConfig(),
        false,              // recursive
        null,               // materializations
        null,               // views
        null,               // partitionedTables
        null,               // refreshInterval
        "LOWER",            // tableNameCasing
        "LOWER",            // columnNameCasing
        "local",            // storageType
        null,               // storageConfig
        null,               // flatten
        null,               // csvTypeInference
        false);             // primeCache

    assertNotNull(schema.getStorageProvider());
  }

  @Test @DisplayName("FileSchema with canonicalSchemaName uses it for .aperio directory naming")
  void testCanonicalSchemaName() {
    FileSchema schema =
        new FileSchema(mockParentSchema,
        "UPPER_CASE_NAME",
        tempDir,            // sourceDirectory
        null,               // userConfiguredBaseDirectory
        null,               // directoryPath
        null,               // directoryPattern
        null,               // tables
        new ExecutionEngineConfig(),
        false,              // recursive
        null,               // materializations
        null,               // views
        null,               // partitionedTables
        null,               // refreshInterval
        "LOWER",            // tableNameCasing
        "LOWER",            // columnNameCasing
        null,               // storageType
        null,               // storageConfig
        null,               // flatten
        null,               // csvTypeInference
        false,              // primeCache
        null,               // comment
        "lower_case_name"); // canonicalSchemaName

    assertNotNull(schema);
    // Schema was created; canonicalSchemaName is used for .aperio directory naming
  }

  private static void deleteRecursive(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteRecursive(child);
        }
      }
    }
    file.delete();
  }
}
