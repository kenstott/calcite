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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Context for schema-level lifecycle processing.
 *
 * <p>Provides access to schema configuration, shared resources, and
 * a mutable attributes map for passing state between lifecycle phases.
 *
 * @see SchemaLifecycleListener
 * @see SchemaLifecycleProcessor
 */
public class SchemaContext {

  private final SchemaConfig config;
  private final StorageProvider storageProvider;
  private final StorageProvider sourceStorageProvider;  // For raw/source data
  private final IncrementalTracker incrementalTracker;
  private final String sourceDirectory;
  private final String materializeDirectory;
  private final Map<String, Object> attributes;

  private SchemaContext(Builder builder) {
    this.config = builder.config;
    this.storageProvider = builder.storageProvider;
    // Default to main storageProvider if sourceStorageProvider not set
    this.sourceStorageProvider = builder.sourceStorageProvider != null
        ? builder.sourceStorageProvider : builder.storageProvider;
    this.incrementalTracker = builder.incrementalTracker != null
        ? builder.incrementalTracker : IncrementalTracker.NOOP;
    this.sourceDirectory = builder.sourceDirectory;
    this.materializeDirectory = builder.materializeDirectory;
    this.attributes = new HashMap<String, Object>();
  }

  /**
   * Returns the schema configuration.
   */
  public SchemaConfig getConfig() {
    return config;
  }

  /**
   * Returns the schema name.
   */
  public String getSchemaName() {
    return config.getName();
  }

  /**
   * Returns the list of table configurations.
   */
  public List<EtlPipelineConfig> getTables() {
    return config.getTables();
  }

  /**
   * Returns the storage provider for materialized data (parquet output).
   */
  public StorageProvider getStorageProvider() {
    return storageProvider;
  }

  /**
   * Returns the storage provider for source/raw data (cache).
   *
   * <p>Use this for reading/writing raw data files, HTTP response cache, etc.
   * Falls back to main storageProvider if not explicitly set.
   */
  public StorageProvider getSourceStorageProvider() {
    return sourceStorageProvider;
  }

  /**
   * Returns the incremental tracker for processing state.
   */
  public IncrementalTracker getIncrementalTracker() {
    return incrementalTracker;
  }

  /**
   * Returns the source directory for file-based sources.
   */
  public String getSourceDirectory() {
    return sourceDirectory;
  }

  /**
   * Returns the materialize directory for output files.
   */
  public String getMaterializeDirectory() {
    return materializeDirectory;
  }

  /**
   * Returns the base directory for output.
   * @deprecated Use {@link #getMaterializeDirectory()} instead
   */
  @Deprecated
  public String getBaseDirectory() {
    return materializeDirectory;
  }

  /**
   * Returns mutable attributes map for sharing state between phases.
   *
   * <p>Use this to pass data from beforeSchema() to afterSchema(), or
   * to share state with TableLifecycleListeners.
   */
  public Map<String, Object> getAttributes() {
    return attributes;
  }

  /**
   * Gets an attribute value.
   */
  @SuppressWarnings("unchecked")
  public <T> T getAttribute(String key) {
    return (T) attributes.get(key);
  }

  /**
   * Sets an attribute value.
   */
  public void setAttribute(String key, Object value) {
    attributes.put(key, value);
  }

  // Bulk download cache path prefix
  private static final String BULK_DOWNLOAD_PATH_PREFIX = "bulkDownload.path.";

  /**
   * Sets the cached file path for a bulk download.
   *
   * <p>Called during the bulk download phase after a file is downloaded.
   * Tables can then look up the cached path by bulk download name.
   *
   * @param bulkDownloadName Name of the bulk download
   * @param variableKey Unique key for this dimension combination (e.g., "year=2023,frequency=annual")
   * @param cachePath Local path to the cached file
   */
  public void setBulkDownloadPath(String bulkDownloadName, String variableKey, String cachePath) {
    String key = BULK_DOWNLOAD_PATH_PREFIX + bulkDownloadName + "." + variableKey;
    attributes.put(key, cachePath);
  }

  /**
   * Gets the cached file path for a bulk download.
   *
   * @param bulkDownloadName Name of the bulk download
   * @param variableKey Unique key for this dimension combination
   * @return Cached file path, or null if not downloaded
   */
  public String getBulkDownloadPath(String bulkDownloadName, String variableKey) {
    String key = BULK_DOWNLOAD_PATH_PREFIX + bulkDownloadName + "." + variableKey;
    return (String) attributes.get(key);
  }

  /**
   * Gets the bulk download configuration by name.
   *
   * @param name Bulk download name
   * @return BulkDownloadConfig, or null if not found
   */
  public BulkDownloadConfig getBulkDownload(String name) {
    return config.getBulkDownload(name);
  }

  /**
   * Creates a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for SchemaContext.
   */
  public static class Builder {
    private SchemaConfig config;
    private StorageProvider storageProvider;
    private StorageProvider sourceStorageProvider;
    private IncrementalTracker incrementalTracker;
    private String sourceDirectory;
    private String materializeDirectory;

    public Builder config(SchemaConfig config) {
      this.config = config;
      return this;
    }

    public Builder storageProvider(StorageProvider storageProvider) {
      this.storageProvider = storageProvider;
      return this;
    }

    /**
     * Sets the storage provider for source/raw data (cache).
     *
     * <p>If not set, falls back to main storageProvider.
     */
    public Builder sourceStorageProvider(StorageProvider sourceStorageProvider) {
      this.sourceStorageProvider = sourceStorageProvider;
      return this;
    }

    public Builder incrementalTracker(IncrementalTracker incrementalTracker) {
      this.incrementalTracker = incrementalTracker;
      return this;
    }

    public Builder sourceDirectory(String sourceDirectory) {
      this.sourceDirectory = sourceDirectory;
      return this;
    }

    public Builder materializeDirectory(String materializeDirectory) {
      this.materializeDirectory = materializeDirectory;
      return this;
    }

    /**
     * @deprecated Use {@link #materializeDirectory(String)} instead
     */
    @Deprecated
    public Builder baseDirectory(String baseDirectory) {
      this.materializeDirectory = baseDirectory;
      return this;
    }

    public SchemaContext build() {
      if (config == null) {
        throw new IllegalArgumentException("Schema config is required");
      }
      if (storageProvider == null) {
        throw new IllegalArgumentException("Storage provider is required");
      }
      return new SchemaContext(this);
    }
  }
}
