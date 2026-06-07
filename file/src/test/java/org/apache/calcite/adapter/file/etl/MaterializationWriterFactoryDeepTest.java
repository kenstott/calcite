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
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Deep tests for {@link MaterializationWriterFactory}.
 */
@Tag("unit")
class MaterializationWriterFactoryDeepTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  @Test void testCreateParquetWriter() {
    MaterializationWriter writer =
        MaterializationWriterFactory.create(MaterializeConfig.Format.PARQUET, storageProvider, tempDir.toString());

    assertNotNull(writer);
    assertEquals(MaterializeConfig.Format.PARQUET, writer.getFormat());
  }

  @Test void testCreateIcebergWriter() {
    MaterializationWriter writer =
        MaterializationWriterFactory.create(MaterializeConfig.Format.ICEBERG, storageProvider, tempDir.toString());

    assertNotNull(writer);
    assertEquals(MaterializeConfig.Format.ICEBERG, writer.getFormat());
  }

  @Test void testCreateWithNullFormat() {
    assertThrows(IllegalArgumentException.class,
        () -> MaterializationWriterFactory.create(null, storageProvider, tempDir.toString()));
  }

  @Test void testCreateDeltaFormatNotImplemented() {
    assertThrows(UnsupportedOperationException.class,
        () -> MaterializationWriterFactory.create(
            MaterializeConfig.Format.DELTA, storageProvider, tempDir.toString()));
  }

  @Test void testCreateSnowflakeFormatNotImplemented() {
    assertThrows(UnsupportedOperationException.class,
        () -> MaterializationWriterFactory.create(
            MaterializeConfig.Format.SNOWFLAKE, storageProvider, tempDir.toString()));
  }

  @Test void testCreateBigqueryFormatNotImplemented() {
    assertThrows(UnsupportedOperationException.class,
        () -> MaterializationWriterFactory.create(
            MaterializeConfig.Format.BIGQUERY, storageProvider, tempDir.toString()));
  }

  @Test void testCreateDatabricksFormatNotImplemented() {
    assertThrows(UnsupportedOperationException.class,
        () -> MaterializationWriterFactory.create(
            MaterializeConfig.Format.DATABRICKS, storageProvider, tempDir.toString()));
  }

  @Test void testCreateWithIncrementalTracker() {
    MaterializationWriter writer =
        MaterializationWriterFactory.create(MaterializeConfig.Format.PARQUET, storageProvider, tempDir.toString(),
        IncrementalTracker.NOOP);

    assertNotNull(writer);
    assertEquals(MaterializeConfig.Format.PARQUET, writer.getFormat());
  }

  @Test void testCreateFromConfig() {
    MaterializeConfig config = MaterializeConfig.builder()
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .build())
        .build();

    MaterializationWriter writer =
        MaterializationWriterFactory.createFromConfig(config, storageProvider, tempDir.toString());

    assertNotNull(writer);
    assertEquals(MaterializeConfig.Format.PARQUET, writer.getFormat());
  }

  @Test void testCreateFromConfigWithNullConfig() {
    assertThrows(IllegalArgumentException.class,
        () -> MaterializationWriterFactory.createFromConfig(
            null, storageProvider, tempDir.toString()));
  }

  @Test void testCreateFromConfigDefaultFormat() {
    // Config without format should default to ICEBERG
    MaterializeConfig config = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .build())
        .build();

    MaterializationWriter writer =
        MaterializationWriterFactory.createFromConfig(config, storageProvider, tempDir.toString());

    assertNotNull(writer);
    // Default format is ICEBERG
    assertEquals(MaterializeConfig.Format.ICEBERG, writer.getFormat());
  }

  @Test void testCreateFromConfigWithTracker() {
    MaterializeConfig config = MaterializeConfig.builder()
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .build())
        .build();

    MaterializationWriter writer =
        MaterializationWriterFactory.createFromConfig(config, storageProvider, tempDir.toString(), IncrementalTracker.NOOP);

    assertNotNull(writer);
  }

  @Test void testCreateFromConfigWithIcebergWarehousePath() {
    MaterializeConfig config = MaterializeConfig.builder()
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .warehousePath(tempDir.resolve("warehouse").toString())
            .build())
        .build();

    MaterializationWriter writer =
        MaterializationWriterFactory.createFromConfig(config, storageProvider, tempDir.toString());

    assertNotNull(writer);
    assertEquals(MaterializeConfig.Format.ICEBERG, writer.getFormat());
  }

  // --- MaterializationWriter interface default methods ---

  @Test void testMaterializationWriterDefaultStoreEtlProperties() {
    MaterializationWriter writer =
        MaterializationWriterFactory.create(MaterializeConfig.Format.PARQUET, storageProvider, tempDir.toString());

    // Default implementation should not throw
    writer.storeEtlProperties("hash123", "sig456", 1000);
  }

  @Test void testMaterializationWriterDefaultGetEtlProperty() {
    MaterializationWriter writer =
        MaterializationWriterFactory.create(MaterializeConfig.Format.PARQUET, storageProvider, tempDir.toString());

    // Default returns null
    assertEquals(null, writer.getEtlProperty("etl.config-hash"));
  }
}
