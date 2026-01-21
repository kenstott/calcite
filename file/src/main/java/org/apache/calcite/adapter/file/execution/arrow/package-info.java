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
 * Apache Arrow-based columnar execution engines.
 *
 * <p>This package provides execution engines that leverage Apache Arrow's
 * columnar memory format for efficient data processing. Arrow's columnar
 * format enables SIMD optimizations and cache-efficient operations.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.execution.arrow.VectorizedArrowExecutionEngine} - Arrow-based vectorized execution</li>
 *   <li>{@link org.apache.calcite.adapter.file.execution.arrow.UniversalDataBatchAdapter} - Adapts various data formats to Arrow batches</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.execution.arrow;
