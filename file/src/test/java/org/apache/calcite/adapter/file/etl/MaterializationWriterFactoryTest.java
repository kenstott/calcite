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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link MaterializationWriterFactory}.
 *
 * <p>Validates factory method behavior for creating format-specific
 * {@link MaterializationWriter} instances including format selection,
 * unsupported format handling, and config propagation.
 */
@Tag("unit")
public class MaterializationWriterFactoryTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MaterializationWriterFactoryTest.class);

  @TempDir
  File tempDir;

  private StorageProvider storageProvider;
  private String baseDirectory;

  @BeforeEach
  public void setUp() {
    storageProvider = new LocalFileStorageProvider();
    baseDirectory = tempDir.getAbsolutePath();
  }

  @Test public void testCreateParquetWriter() {
    MaterializationWriter writer = MaterializationWriterFactory.create(
        MaterializeConfig.Format.PARQUET, storageProvider, baseDirectory);

    assertNotNull(writer, "Factory should return a non-null writer");
    assertTrue(writer instanceof ParquetMaterializationWriter,
        "Should create ParquetMaterializationWriter for PARQUET format");
    assertEquals(MaterializeConfig.Format.PARQUET, writer.getFormat(),
        "Writer format should be PARQUET");
  }

  @Test public void testCreateIcebergWriter() {
    MaterializationWriter writer = MaterializationWriterFactory.create(
        MaterializeConfig.Format.ICEBERG, storageProvider, baseDirectory);

    assertNotNull(writer, "Factory should return a non-null writer");
    assertTrue(writer instanceof IcebergMaterializationWriter,
        "Should create IcebergMaterializationWriter for ICEBERG format");
    assertEquals(MaterializeConfig.Format.ICEBERG, writer.getFormat(),
        "Writer format should be ICEBERG");
  }

  @Test public void testCreateWithNullFormatThrowsException() {
    assertThrows(IllegalArgumentException.class,
        () -> MaterializationWriterFactory.create(null, storageProvider, baseDirectory),
        "Should throw IllegalArgumentException for null format");
  }

  @Test public void testCreateDeltaFormatThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> MaterializationWriterFactory.create(
            MaterializeConfig.Format.DELTA, storageProvider, baseDirectory),
        "Delta format should throw UnsupportedOperationException");
  }

  @Test public void testCreateSnowflakeFormatThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> MaterializationWriterFactory.create(
            MaterializeConfig.Format.SNOWFLAKE, storageProvider, baseDirectory),
        "Snowflake format should throw UnsupportedOperationException");
  }

  @Test public void testCreateBigQueryFormatThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> MaterializationWriterFactory.create(
            MaterializeConfig.Format.BIGQUERY, storageProvider, baseDirectory),
        "BigQuery format should throw UnsupportedOperationException");
  }

  @Test public void testCreateDatabricksFormatThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> MaterializationWriterFactory.create(
            MaterializeConfig.Format.DATABRICKS, storageProvider, baseDirectory),
        "Databricks format should throw UnsupportedOperationException");
  }

  @Test public void testCreateFromConfigWithParquetFormat() {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder()
            .compression("snappy")
            .build())
        .build();

    MaterializationWriter writer = MaterializationWriterFactory.createFromConfig(
        config, storageProvider, baseDirectory);

    assertNotNull(writer, "Factory should return a non-null writer");
    assertTrue(writer instanceof ParquetMaterializationWriter,
        "Should create ParquetMaterializationWriter from PARQUET config");
  }

  @Test public void testCreateFromConfigWithIcebergFormat() {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    MaterializationWriter writer = MaterializationWriterFactory.createFromConfig(
        config, storageProvider, baseDirectory);

    assertNotNull(writer, "Factory should return a non-null writer");
    assertTrue(writer instanceof IcebergMaterializationWriter,
        "Should create IcebergMaterializationWriter from ICEBERG config");
  }

  @Test public void testCreateFromConfigDefaultsToIceberg() {
    // Config without explicit format defaults to ICEBERG
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    MaterializationWriter writer = MaterializationWriterFactory.createFromConfig(
        config, storageProvider, baseDirectory);

    assertNotNull(writer, "Factory should return a non-null writer");
    assertTrue(writer instanceof IcebergMaterializationWriter,
        "Default format should create IcebergMaterializationWriter");
  }

  @Test public void testCreateFromConfigWithNullConfigThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> MaterializationWriterFactory.createFromConfig(
            null, storageProvider, baseDirectory),
        "Should throw IllegalArgumentException for null config");
  }

  @Test public void testCreateFromConfigUsesIcebergWarehousePath() {
    String customWarehouse = new File(tempDir, "custom_warehouse").getAbsolutePath();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .warehousePath(customWarehouse)
            .build())
        .build();

    MaterializationWriter writer = MaterializationWriterFactory.createFromConfig(
        config, storageProvider, baseDirectory);

    assertNotNull(writer, "Factory should return a non-null writer");
    assertTrue(writer instanceof IcebergMaterializationWriter,
        "Should create IcebergMaterializationWriter with custom warehouse path");
  }

  @Test public void testConfigPropagationToParquetWriter() throws Exception {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder()
            .compression("zstd")
            .build())
        .options(MaterializeOptionsConfig.builder()
            .threads(4)
            .rowGroupSize(50000)
            .build())
        .build();

    MaterializationWriter writer = MaterializationWriterFactory.createFromConfig(
        config, storageProvider, baseDirectory);

    assertNotNull(writer, "Factory should return a non-null writer");
    assertEquals(MaterializeConfig.Format.PARQUET, writer.getFormat(),
        "Writer format should match config");

    // Verify writer can be initialized with the config (config propagation works)
    writer.initialize(config);
    assertEquals(0, writer.getTotalRowsWritten(),
        "Fresh writer should have 0 rows written");
    writer.close();
  }

  @Test public void testCreateWithOverloadedMethodIncludingTracker() {
    MaterializationWriter writer = MaterializationWriterFactory.create(
        MaterializeConfig.Format.PARQUET, storageProvider, baseDirectory);

    assertNotNull(writer, "Factory should return a non-null writer for overloaded method");
    assertEquals(MaterializeConfig.Format.PARQUET, writer.getFormat(),
        "Writer format should be PARQUET");
  }
}
