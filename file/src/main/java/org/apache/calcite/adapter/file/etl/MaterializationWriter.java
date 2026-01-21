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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface for materializing data to different storage formats.
 *
 * <p>Implementations handle writing data to specific formats:
 * <ul>
 *   <li>{@code ICEBERG} - Writes to Iceberg tables with atomic commits</li>
 *   <li>{@code PARQUET} - Writes to hive-partitioned Parquet files</li>
 * </ul>
 *
 * <p>The writer follows a batch-oriented lifecycle:
 * <ol>
 *   <li>{@link #initialize(MaterializeConfig)} - Prepare writer with configuration</li>
 *   <li>{@link #writeBatch(Iterator, Map)} - Write data batches with partition context</li>
 *   <li>{@link #commit()} - Finalize all writes atomically (for Iceberg)</li>
 *   <li>{@link #close()} - Release resources</li>
 * </ol>
 *
 * @see MaterializeConfig
 * @see MaterializeConfig.Format
 */
public interface MaterializationWriter extends Closeable {

  /**
   * Initializes the writer with the given configuration.
   *
   * <p>For Iceberg format, this ensures the target table exists and prepares
   * the writer for batch processing. For Parquet format, this sets up the
   * output directory structure.
   *
   * @param config Materialization configuration
   * @throws IOException if initialization fails
   */
  void initialize(MaterializeConfig config) throws IOException;

  /**
   * Writes a batch of data with the given partition context.
   *
   * <p>The partition variables are used to:
   * <ul>
   *   <li>Determine partition paths for hive-partitioned output</li>
   *   <li>Filter existing data for Iceberg overwrite operations</li>
   *   <li>Track incremental processing progress</li>
   * </ul>
   *
   * @param data Iterator over row data (Map&lt;String, Object&gt;)
   * @param partitionVariables Partition column values for this batch
   * @return Number of rows written
   * @throws IOException if write fails
   */
  long writeBatch(Iterator<Map<String, Object>> data,
      Map<String, String> partitionVariables) throws IOException;

  /**
   * Commits all pending writes atomically.
   *
   * <p>For Iceberg format, this commits all staged files as a single transaction.
   * For Parquet format, this is typically a no-op since files are written directly.
   *
   * @throws IOException if commit fails
   */
  void commit() throws IOException;

  /**
   * Returns the total number of rows written across all batches.
   *
   * @return Total row count
   */
  long getTotalRowsWritten();

  /**
   * Returns the total number of files written.
   *
   * @return Total file count
   */
  int getTotalFilesWritten();

  /**
   * Returns the format this writer handles.
   *
   * @return The materialization format
   */
  MaterializeConfig.Format getFormat();

  /**
   * Returns the location of the materialized table.
   *
   * <p>For Iceberg format, this returns the Iceberg table location (e.g.,
   * {@code s3://bucket/warehouse/table_name}) which contains the metadata folder.
   * DuckDB's {@code iceberg_scan()} function requires this location.
   *
   * <p>For Parquet format, this returns the output directory pattern.
   *
   * @return The table location URI, or null if not yet initialized
   */
  String getTableLocation();

  /**
   * Stores ETL completion metadata in the table properties.
   *
   * <p>For Iceberg format, this stores properties in the Iceberg table metadata
   * which can be read on subsequent connections to skip dimension expansion.
   * For other formats, this is a no-op.
   *
   * @param configHash Hash of the dimension configuration
   * @param dimensionSignature Signature of expanded dimension combinations
   * @param rowCount Total row count in the table
   */
  default void storeEtlProperties(String configHash, String dimensionSignature, long rowCount) {
    // Default no-op for formats that don't support table properties
  }

  /**
   * Reads an ETL property from the table metadata.
   *
   * <p>For Iceberg format, reads from Iceberg table properties.
   * For other formats, returns null.
   *
   * @param key Property key (e.g., "etl.config-hash", "etl.signature")
   * @return Property value, or null if not found or not supported
   */
  default String getEtlProperty(String key) {
    return null;
  }
}
