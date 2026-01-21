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
 * Caching and concurrency management for the file adapter.
 *
 * <p>This package provides utilities for managing concurrent access to files,
 * distributed locking mechanisms, and spillover management for large datasets.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.cache.ConcurrentSpilloverManager} - Manages spillover of large datasets to disk</li>
 *   <li>{@link org.apache.calcite.adapter.file.cache.SourceFileLockManager} - File-based locking for concurrent access</li>
 *   <li>{@link org.apache.calcite.adapter.file.cache.RedisDistributedLock} - Redis-based distributed locking (optional)</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.cache;
