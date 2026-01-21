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
 * Table implementations for the file adapter.
 *
 * <p>This package contains various table implementations that provide
 * access to data from different file formats including CSV, JSON, and Parquet.
 * Tables can be scannable, translatable, or both, and may support features
 * like partitioning and glob patterns.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.table.FileTable} - Base table implementation</li>
 *   <li>{@link org.apache.calcite.adapter.file.table.CsvTable} - CSV file tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.table.JsonTable} - JSON file tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.table.ParquetScannableTable} - Parquet scannable tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.table.GlobParquetTable} - Glob pattern Parquet tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.table.PartitionedParquetTable} - Partitioned Parquet tables</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.table;
