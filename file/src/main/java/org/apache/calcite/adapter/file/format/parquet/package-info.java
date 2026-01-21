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
 * Parquet format-specific utilities and converters.
 *
 * <p>This package provides utilities for working with Apache Parquet files,
 * including conversion utilities, caching mechanisms, and direct writers.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil} - Utilities for converting to/from Parquet</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.parquet.DirectParquetWriter} - Direct Parquet file writing</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.parquet.ConcurrentParquetCache} - Thread-safe Parquet caching</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.format.parquet;
