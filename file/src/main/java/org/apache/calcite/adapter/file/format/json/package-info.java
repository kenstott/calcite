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
 * JSON format-specific utilities and processing.
 *
 * <p>This package provides utilities for working with JSON files,
 * including flattening nested structures, multi-table extraction,
 * and shared data management.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.format.json.JsonFlattener} - Flattens nested JSON structures</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.json.JsonMultiTableFactory} - Extracts multiple tables from JSON</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.json.JsonSearchConfig} - Configuration for JSON searching</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.json.SharedJsonData} - Manages shared JSON data across tables</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.format.json;
