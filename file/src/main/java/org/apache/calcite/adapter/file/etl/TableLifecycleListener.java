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

/**
 * Lifecycle listener for table-level events during ETL processing.
 *
 * <p>Implementations can perform setup, validation, and cleanup
 * for individual tables. Common use cases:
 * <ul>
 *   <li>Validate table configuration before processing</li>
 *   <li>Setup table-specific resources</li>
 *   <li>Post-process results (indexing, statistics)</li>
 *   <li>Custom error handling per table</li>
 * </ul>
 *
 * <h3>Lifecycle Order</h3>
 * <pre>
 * 1. beforeTable()
 * 2. resolveDimensions(), shouldProcessTable()
 * 3. beforeSource()
 * 4. [fetch data from HTTP/file]
 * 5. afterSource() OR onSourceError()
 * 6. beforeMaterialize()
 * 7. [write to Parquet/Iceberg]
 * 8. afterMaterialize() OR onMaterializeError()
 * 9. afterTable() OR onTableError()
 * </pre>
 *
 * @see SchemaLifecycleListener
 * @see SchemaLifecycleProcessor
 */
public interface TableLifecycleListener {

  /**
   * Called before a table is processed.
   *
   * <p>Use this to:
   * <ul>
   *   <li>Validate table configuration</li>
   *   <li>Setup table-specific resources</li>
   *   <li>Check prerequisites</li>
   * </ul>
   *
   * @param context Table processing context
   * @throws Exception If setup fails (skips this table)
   */
  void beforeTable(TableContext context) throws Exception;

  /**
   * Called after a table has been processed successfully.
   *
   * <p>Use this to:
   * <ul>
   *   <li>Update indexes or statistics</li>
   *   <li>Validate output</li>
   *   <li>Trigger downstream processes</li>
   * </ul>
   *
   * @param context Table processing context
   * @param result Processing result for this table
   */
  void afterTable(TableContext context, EtlResult result);

  /**
   * Called when table processing fails.
   *
   * <p>Use this for table-specific error handling. Return value determines
   * whether to continue with remaining tables.
   *
   * @param context Table processing context
   * @param error The exception that caused the failure
   * @return true to continue processing other tables, false to abort schema
   */
  boolean onTableError(TableContext context, Exception error);

  // ========== SOURCE PHASE HOOKS ==========

  /**
   * Called before data is fetched from the source.
   *
   * <p>Use this to:
   * <ul>
   *   <li>Log or track source fetching</li>
   *   <li>Set up rate limiting</li>
   *   <li>Validate source availability</li>
   * </ul>
   *
   * @param context Table processing context
   */
  default void beforeSource(TableContext context) { }

  /**
   * Called after data has been successfully fetched from the source.
   *
   * <p>Use this to:
   * <ul>
   *   <li>Log fetch statistics</li>
   *   <li>Validate fetched data</li>
   *   <li>Transform data before materialization</li>
   * </ul>
   *
   * @param context Table processing context
   * @param result Source fetch result with statistics
   */
  default void afterSource(TableContext context, SourceResult result) { }

  /**
   * Called when source fetching fails.
   *
   * <p>Return value determines whether to continue with remaining batches.
   *
   * @param context Table processing context
   * @param error The exception that caused the failure
   * @return true to continue processing other batches, false to abort table
   */
  default boolean onSourceError(TableContext context, Exception error) {
    return true; // Continue with other batches by default
  }

  // ========== MATERIALIZE PHASE HOOKS ==========

  /**
   * Called before data is written to the output format.
   *
   * <p>Use this to:
   * <ul>
   *   <li>Log or track materialization progress</li>
   *   <li>Prepare output location</li>
   *   <li>Validate output configuration</li>
   * </ul>
   *
   * @param context Table processing context
   */
  default void beforeMaterialize(TableContext context) { }

  /**
   * Called after data has been successfully written to the output format.
   *
   * <p>Use this to:
   * <ul>
   *   <li>Log write statistics</li>
   *   <li>Update metadata or indexes</li>
   *   <li>Trigger downstream processes</li>
   * </ul>
   *
   * @param context Table processing context
   * @param result Materialization result with statistics
   */
  default void afterMaterialize(TableContext context, MaterializeResult result) { }

  /**
   * Called when materialization fails.
   *
   * <p>Return value determines whether to continue with remaining batches.
   *
   * @param context Table processing context
   * @param error The exception that caused the failure
   * @return true to continue processing other batches, false to abort table
   */
  default boolean onMaterializeError(TableContext context, Exception error) {
    return true; // Continue with other batches by default
  }

  // ========== DATA PROVIDER HOOK ==========

  /**
   * Called to provide data for a batch. If this returns non-null,
   * the built-in HttpSource is skipped entirely.
   *
   * <p>Use this for custom data sources not supported by built-in fetchers:
   * <ul>
   *   <li>FTP/SFTP downloads</li>
   *   <li>Database queries</li>
   *   <li>Message queue consumption</li>
   *   <li>Custom API integrations</li>
   * </ul>
   *
   * <p>Example:
   * <pre>{@code
   * .fetchData("econ.ftp_inventory", (ctx, vars) -> {
   *   SourceConfig source = ctx.getTableConfig().getSource();
   *   Map<String, Object> ftpConfig = source.getCustomConfig();
   *   return ftpClient.downloadAndParse(ftpConfig, vars);
   * })
   * }</pre>
   *
   * @param context Table processing context
   * @param variables Dimension values for this batch
   * @return Iterator of records, or null to use default HttpSource
   */
  default java.util.Iterator<java.util.Map<String, Object>> fetchData(
      TableContext context,
      java.util.Map<String, String> variables) {
    return null; // Use default HttpSource
  }

  /**
   * Called to write data for a batch. If this returns a non-negative value,
   * the built-in MaterializationWriter is skipped entirely.
   *
   * <p>Use this for custom output destinations not supported by built-in writers:
   * <ul>
   *   <li>Custom database inserts</li>
   *   <li>Message queue publishing</li>
   *   <li>Custom file formats</li>
   *   <li>Real-time streaming outputs</li>
   * </ul>
   *
   * <p>Example:
   * <pre>{@code
   * .writeData("econ.realtime_prices", (ctx, data, vars) -> {
   *   long count = 0;
   *   while (data.hasNext()) {
   *     kafkaProducer.send(data.next());
   *     count++;
   *   }
   *   return count;
   * })
   * }</pre>
   *
   * @param context Table processing context
   * @param data Iterator of records to write
   * @param variables Dimension values for this batch
   * @return Number of rows written, or -1 to use default MaterializationWriter
   */
  default long writeData(
      TableContext context,
      java.util.Iterator<java.util.Map<String, Object>> data,
      java.util.Map<String, String> variables) {
    return -1; // Use default MaterializationWriter
  }

  // ========== CALLBACK METHODS ==========
  // These are called by the processor to get table-specific configuration

  /**
   * Called to resolve dimensions for a table.
   *
   * <p>Override this to provide dynamic dimensions based on table metadata,
   * API catalogs, or other sources. The returned dimensions override any
   * static dimensions in the table config.
   *
   * @param context Table processing context
   * @param staticDimensions Dimensions from YAML config (may be empty)
   * @return Resolved dimensions, or null to use static dimensions
   */
  default java.util.Map<String, DimensionConfig> resolveDimensions(
      TableContext context,
      java.util.Map<String, DimensionConfig> staticDimensions) {
    return null; // Use static dimensions by default
  }

  /**
   * Called to get an API key or credential for a table's source.
   *
   * <p>The processor calls this when it encounters a parameter like
   * "{env:BLS_API_KEY}" in the source configuration.
   *
   * @param context Table processing context
   * @param keyName The key name (e.g., "BLS_API_KEY", "FRED_API_KEY")
   * @return The API key value, or null if not available
   */
  default String resolveApiKey(TableContext context, String keyName) {
    return null;
  }

  /**
   * Called to determine if a table is enabled.
   *
   * <p>Override this to filter tables based on source, enabled features,
   * or other criteria. Disabled tables are:
   * <ul>
   *   <li>Skipped during ETL (no source download or materialization)</li>
   *   <li>Excluded from the final schema metadata</li>
   * </ul>
   *
   * <p>This corresponds to the YAML `enabled` flag in hooks configuration.
   *
   * @param context Table processing context with table metadata
   * @return true if table is enabled, false to disable it
   */
  default boolean isTableEnabled(TableContext context) {
    return true; // All tables enabled by default
  }

  /**
   * @deprecated Use {@link #isTableEnabled(TableContext)} instead
   */
  @Deprecated
  default boolean shouldProcessTable(TableContext context) {
    return isTableEnabled(context);
  }

  /**
   * No-op implementation for convenience.
   */
  TableLifecycleListener NOOP = new TableLifecycleListener() {
    @Override public void beforeTable(TableContext context) { }
    @Override public void afterTable(TableContext context, EtlResult result) { }
    @Override public boolean onTableError(TableContext context, Exception error) {
      return true; // Continue with other tables
    }
  };
}
