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
 * Parquet-native execution engines.
 *
 * <p>This package provides execution engines specifically optimized for
 * Apache Parquet files. These engines leverage Parquet's columnar format
 * and built-in features like predicate pushdown and column pruning.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.execution.parquet.ParquetExecutionEngine} - Native Parquet execution engine</li>
 *   <li>{@link org.apache.calcite.adapter.file.execution.parquet.ParquetFileEnumerator} - Direct Parquet file enumeration</li>
 *   <li>{@link org.apache.calcite.adapter.file.execution.parquet.ParquetEnumerableFactory} - Factory for creating Parquet enumerables</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.execution.parquet;
