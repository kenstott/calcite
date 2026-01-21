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

/**
 * Factory for creating format-specific {@link MaterializationWriter} instances.
 *
 * <p>This factory centralizes the creation logic for different output formats:
 * <ul>
 *   <li>{@code ICEBERG} - Creates {@link IcebergMaterializationWriter}</li>
 *   <li>{@code PARQUET} - Creates {@link ParquetMaterializationWriter}</li>
 * </ul>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * StorageProvider storage = StorageProviderFactory.create("/data");
 * MaterializeConfig config = MaterializeConfig.builder()
 *     .format(Format.ICEBERG)
 *     .output(...)
 *     .build();
 *
 * MaterializationWriter writer = MaterializationWriterFactory.create(
 *     config.getFormat(), storage, "/data/warehouse");
 * writer.initialize(config);
 * writer.writeBatch(data, partitionVars);
 * writer.commit();
 * writer.close();
 * }</pre>
 *
 * @see MaterializationWriter
 * @see MaterializeConfig.Format
 */
public final class MaterializationWriterFactory {

  private MaterializationWriterFactory() {
    // Utility class - no instantiation
  }

  /**
   * Creates a MaterializationWriter for the specified format.
   *
   * @param format The output format (ICEBERG or PARQUET)
   * @param storageProvider Storage provider for file operations
   * @param baseDirectory Base directory for output files
   * @return A format-specific MaterializationWriter
   * @throws IllegalArgumentException if format is null or unsupported
   */
  public static MaterializationWriter create(
      MaterializeConfig.Format format,
      StorageProvider storageProvider,
      String baseDirectory) {
    return create(format, storageProvider, baseDirectory, IncrementalTracker.NOOP);
  }

  /**
   * Creates a MaterializationWriter for the specified format with incremental tracking.
   *
   * @param format The output format (ICEBERG or PARQUET)
   * @param storageProvider Storage provider for file operations
   * @param baseDirectory Base directory for output files
   * @param incrementalTracker Tracker for incremental processing (Iceberg only)
   * @return A format-specific MaterializationWriter
   * @throws IllegalArgumentException if format is null or unsupported
   */
  public static MaterializationWriter create(
      MaterializeConfig.Format format,
      StorageProvider storageProvider,
      String baseDirectory,
      IncrementalTracker incrementalTracker) {

    if (format == null) {
      throw new IllegalArgumentException("Format cannot be null");
    }

    switch (format) {
      case ICEBERG:
        return new IcebergMaterializationWriter(storageProvider, baseDirectory, incrementalTracker);
      case PARQUET:
        return new ParquetMaterializationWriter(storageProvider, baseDirectory);
      case DELTA:
        throw new UnsupportedOperationException(
            "Delta Lake format not yet implemented. "
            + "Planned: Stage to Parquet then commit via delta-standalone library.");
      case SNOWFLAKE:
        throw new UnsupportedOperationException(
            "Snowflake format not yet implemented. "
            + "Planned: Stage to Parquet, upload to Snowflake stage, COPY INTO target table.");
      case BIGQUERY:
        throw new UnsupportedOperationException(
            "BigQuery format not yet implemented. "
            + "Planned: Stage to Parquet, upload to GCS, load via bq or Storage Write API.");
      case DATABRICKS:
        throw new UnsupportedOperationException(
            "Databricks format not yet implemented. "
            + "Planned: Stage to cloud storage, register in Unity Catalog via REST API.");
      default:
        throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }

  /**
   * Creates a MaterializationWriter from a complete MaterializeConfig.
   *
   * <p>This method extracts format-specific settings from the config and
   * creates the appropriate writer.
   *
   * @param config Complete materialization configuration
   * @param storageProvider Storage provider for file operations
   * @param baseDirectory Base directory for output files
   * @return A configured MaterializationWriter
   */
  public static MaterializationWriter createFromConfig(
      MaterializeConfig config,
      StorageProvider storageProvider,
      String baseDirectory) {
    return createFromConfig(config, storageProvider, baseDirectory, IncrementalTracker.NOOP);
  }

  /**
   * Creates a MaterializationWriter from a complete MaterializeConfig with incremental tracking.
   *
   * @param config Complete materialization configuration
   * @param storageProvider Storage provider for file operations
   * @param baseDirectory Base directory for output files
   * @param incrementalTracker Tracker for incremental processing
   * @return A configured MaterializationWriter
   */
  public static MaterializationWriter createFromConfig(
      MaterializeConfig config,
      StorageProvider storageProvider,
      String baseDirectory,
      IncrementalTracker incrementalTracker) {

    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }

    MaterializeConfig.Format format = config.getFormat();
    if (format == null) {
      format = MaterializeConfig.Format.ICEBERG; // Default
    }

    // For Iceberg, use warehouse path from iceberg config if available
    String effectiveBaseDir = baseDirectory;
    if (format == MaterializeConfig.Format.ICEBERG && config.getIceberg() != null) {
      String warehousePath = config.getIceberg().getWarehousePath();
      if (warehousePath != null && !warehousePath.isEmpty()) {
        effectiveBaseDir = warehousePath;
      }
    }

    return create(format, storageProvider, effectiveBaseDir, incrementalTracker);
  }
}
