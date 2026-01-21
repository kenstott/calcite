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
 * Statistics collection and management for file adapter tables.
 *
 * <p>Provides interfaces and implementations for collecting table and column
 * statistics including HyperLogLog sketches for COUNT(DISTINCT) optimization.
 */
package org.apache.calcite.adapter.file.statistics;
