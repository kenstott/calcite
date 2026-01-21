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
 * Optimized vectorized execution engines.
 *
 * <p>This package contains execution engines that process data in batches
 * using vectorized operations. These engines are optimized for performance
 * with batch processing and SIMD-friendly operations.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.execution.vectorized.VectorizedFileEnumerator} - Base vectorized file processing</li>
 *   <li>{@link org.apache.calcite.adapter.file.execution.vectorized.VectorizedCsvEnumerator} - Vectorized CSV processing</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.execution.vectorized;
