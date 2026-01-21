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
 * CSV format-specific utilities and processing.
 *
 * <p>This package provides utilities for working with CSV files,
 * including stream readers, table factories, and optimization rules.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.format.csv.CsvStreamReader} - Streaming CSV file reader</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.csv.CsvTableFactory} - Factory for creating CSV tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.csv.CsvProjectTableScanRule} - Optimization rule for CSV projections</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.format.csv;
