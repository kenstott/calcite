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
 * Materialized view support for the file adapter.
 *
 * <p>This package provides components for creating and managing materialized
 * views over file-based data sources. Materialized views can significantly
 * improve query performance by pre-computing and caching results.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.materialized.MaterializedViewTable} - Table representing a materialized view</li>
 *   <li>{@link org.apache.calcite.adapter.file.materialized.MaterializedViewUtil} - Utilities for materialized view management</li>
 *   <li>{@link org.apache.calcite.adapter.file.materialized.RefreshableMaterializedViewTable} - Auto-refreshing materialized views</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.materialized;
