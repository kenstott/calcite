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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.StorageProvider;

/**
 * Optional interface for {@link DataProvider} implementations that need access
 * to a configured {@link StorageProvider} for caching.
 *
 * <p>When {@link SchemaLifecycleProcessor} creates a DataProvider and detects it
 * implements this interface, it calls {@link #setStorageProvider} with the schema's
 * configured cache storage provider before the first {@code fetch()} call.
 */
public interface StorageAwareDataProvider extends DataProvider {

  /**
   * Called by the framework before the first fetch() invocation.
   * The provided storage provider has full credentials for reading and writing
   * to the schema's configured cache directory.
   *
   * @param storageProvider the schema's cache storage provider (may be S3, local, etc.)
   * @param cacheDirectory  the base cache directory path (e.g., s3://bucket/geo)
   */
  void setStorageProvider(StorageProvider storageProvider, String cacheDirectory);
}
