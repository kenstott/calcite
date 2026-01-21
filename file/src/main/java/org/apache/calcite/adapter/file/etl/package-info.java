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

/**
 * ETL capabilities for the File adapter.
 *
 * <p>This package provides components for Extract-Transform-Load operations:
 *
 * <h2>Core Components</h2>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.etl.HiveParquetWriter} - Materializes data
 *       to hive-partitioned Parquet files using DuckDB</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.MaterializeConfig} - Configuration for
 *       materialization operations</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.DataSource} - Interface for data sources
 *       that can be materialized</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.EtlPipelineConfig} - Complete pipeline
 *       configuration combining source, dimensions, columns, and materialization</li>
 * </ul>
 *
 * <h2>Extensibility Hooks</h2>
 * <p>Hooks allow adapters to inject business-specific logic into the generic ETL pipeline:
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.etl.ResponseTransformer} - Transform raw API
 *       response before parsing</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.RowTransformer} - Transform individual rows
 *       during materialization</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.Validator} - Validate rows before writing
 *       to output</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.DimensionResolver} - Custom dimension value
 *       resolution</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.HooksConfig} - Configuration for all hooks</li>
 * </ul>
 *
 * <h2>Context Objects</h2>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.etl.RequestContext} - Context passed to
 *       ResponseTransformer with URL, parameters, headers, and dimension values</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.RowContext} - Context passed to
 *       RowTransformer with dimension values, table config, and row number</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.ValidationResult} - Result of validation
 *       with action (valid, drop, warn, fail)</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * StorageProvider storageProvider = StorageProviderFactory.createFromUrl("s3://bucket/");
 * HiveParquetWriter writer = new HiveParquetWriter(storageProvider, "/data/output");
 *
 * MaterializeConfig config = MaterializeConfig.builder()
 *     .output(MaterializeOutputConfig.builder()
 *         .location("/data/output")
 *         .format("parquet")
 *         .compression("snappy")
 *         .build())
 *     .partition(MaterializePartitionConfig.builder()
 *         .columns(Arrays.asList("year", "region"))
 *         .batchBy(Arrays.asList("year"))
 *         .build())
 *     .build();
 *
 * writer.materialize(config, dataSource);
 * }</pre>
 *
 * @see org.apache.calcite.adapter.file.partition.ParquetReorganizer
 */
package org.apache.calcite.adapter.file.etl;
