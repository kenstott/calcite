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
 * Row-by-row execution engines using the Linq4j framework.
 *
 * <p>This package contains enumerator implementations that process data
 * one row at a time using Calcite's Linq4j framework. These are the traditional
 * execution engines suitable for smaller datasets and streaming operations.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.execution.linq4j.CsvEnumerator} - Row-by-row CSV processing</li>
 *   <li>{@link org.apache.calcite.adapter.file.execution.linq4j.JsonEnumerator} - Row-by-row JSON processing</li>
 *   <li>{@link org.apache.calcite.adapter.file.execution.linq4j.ParquetEnumerator} - Row-by-row Parquet processing</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.execution.linq4j;
