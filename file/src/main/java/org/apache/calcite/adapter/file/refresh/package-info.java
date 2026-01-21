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
 * Auto-refreshing table support for dynamic file sources.
 *
 * <p>This package provides components for creating tables that automatically
 * refresh their data when the underlying files change or at specified intervals.
 * This is particularly useful for monitoring log files or data feeds.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.RefreshableTable} - Interface for refreshable tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.AbstractRefreshableTable} - Base implementation for refreshable tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.RefreshInterval} - Configuration for refresh intervals</li>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.RefreshableCsvTable} - Auto-refreshing CSV tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.RefreshableJsonTable} - Auto-refreshing JSON tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.RefreshablePartitionedParquetTable} - Auto-refreshing partitioned Parquet tables</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.refresh;
