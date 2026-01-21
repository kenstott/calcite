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
 * Partitioning support for distributed file tables.
 *
 * <p>This package provides components for detecting and managing partitioned
 * tables where data is distributed across multiple files following naming
 * conventions or directory structures.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.partition.PartitionDetector} - Detects partitioning schemes in file systems</li>
 *   <li>{@link org.apache.calcite.adapter.file.partition.PartitionedTableConfig} - Configuration for partitioned tables</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.partition;
