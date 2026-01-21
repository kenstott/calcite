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
 * Metadata and schema information for the file adapter.
 *
 * <p>This package provides components for managing metadata about files,
 * schemas, and field types, including compatibility with various database
 * metadata standards.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.metadata.InformationSchema} - Standard SQL information schema tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.metadata.PostgresMetadataSchema} - PostgreSQL-compatible metadata views</li>
 *   <li>{@link org.apache.calcite.adapter.file.metadata.RemoteFileMetadata} - Metadata for remote file sources</li>
 *   <li>{@link org.apache.calcite.adapter.file.metadata.FileFieldType} - Type system for file fields</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.metadata;
