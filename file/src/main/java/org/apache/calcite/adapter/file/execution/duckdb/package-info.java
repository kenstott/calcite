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
 * DuckDB-based execution engine for high-performance analytical queries.
 *
 * <p>This package provides:
 * <ul>
 *   <li>SQL pushdown to DuckDB's columnar engine</li>
 *   <li>Reuse of Parquet conversion pipeline</li>
 *   <li>DuckDB connection management</li>
 *   <li>Fallback to Parquet engine when DuckDB unavailable</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.execution.duckdb;
