/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
