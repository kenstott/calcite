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

import java.util.Map;

/**
 * Lifecycle listener for schema-level events during ETL processing.
 *
 * <p>Implementations can perform setup, teardown, and error handling
 * at the schema level. Common use cases:
 * <ul>
 *   <li>Authenticate with APIs before processing tables</li>
 *   <li>Setup shared resources (connection pools, caches)</li>
 *   <li>Run maintenance after all tables processed</li>
 *   <li>Generate summary reports</li>
 *   <li>Custom bulk download logic (SharePoint, S3, FTP, etc.)</li>
 * </ul>
 *
 * <h3>Lifecycle Order</h3>
 * <pre>
 * 1. beforeSchema()
 * 2. [for each bulk download: downloadBulkFile()]
 * 3. [for each table: TableLifecycleListener events]
 * 4. afterSchema() OR onSchemaError()
 * </pre>
 *
 * @see TableLifecycleListener
 * @see SchemaLifecycleProcessor
 */
public interface SchemaLifecycleListener {

  /**
   * Called before any tables are processed.
   *
   * <p>Use this to:
   * <ul>
   *   <li>Authenticate with external services</li>
   *   <li>Initialize shared resources</li>
   *   <li>Validate schema-level configuration</li>
   * </ul>
   *
   * @param context Schema processing context
   * @throws Exception If setup fails (aborts schema processing)
   */
  void beforeSchema(SchemaContext context) throws Exception;

  /**
   * Called after all tables have been processed successfully.
   *
   * <p>Use this to:
   * <ul>
   *   <li>Run maintenance tasks</li>
   *   <li>Generate reports</li>
   *   <li>Cleanup resources</li>
   * </ul>
   *
   * @param context Schema processing context
   * @param result Aggregated results from all tables
   */
  void afterSchema(SchemaContext context, SchemaResult result);

  /**
   * Called when schema processing fails.
   *
   * <p>Use this for error handling and cleanup when processing cannot continue.
   *
   * @param context Schema processing context
   * @param error The exception that caused the failure
   */
  void onSchemaError(SchemaContext context, Exception error);

  /**
   * Called to download a bulk file during the bulk download phase.
   *
   * <p>Override this method to implement custom download logic for sources
   * that require special handling:
   * <ul>
   *   <li>SharePoint - requires Microsoft Graph API auth</li>
   *   <li>S3 with assumed roles - requires AWS credential chain</li>
   *   <li>FTP servers - requires FTP protocol handling</li>
   *   <li>Internal file shares - requires CIFS/SMB mounting</li>
   * </ul>
   *
   * <p>The default implementation returns null, which tells the processor
   * to use the built-in HTTP download logic.
   *
   * <h3>Example Implementation</h3>
   * <pre>{@code
   * public String downloadBulkFile(SchemaContext ctx, BulkDownloadConfig cfg,
   *     Map<String, String> vars, String targetPath) {
   *   String sourceUrl = cfg.resolveUrl(vars);
   *   if (sourceUrl.startsWith("sharepoint://")) {
   *     return sharePointClient.downloadToPath(sourceUrl, targetPath);
   *   }
   *   return null; // Use default HTTP download
   * }
   * }</pre>
   *
   * @param context Schema processing context
   * @param bulkConfig Bulk download configuration
   * @param variables Resolved dimension variables for this download
   * @param targetPath Target file path where the download should be saved
   * @return Path to downloaded file, or null to use default HTTP download
   * @throws Exception If custom download fails
   */
  default String downloadBulkFile(SchemaContext context, BulkDownloadConfig bulkConfig,
      Map<String, String> variables, String targetPath) throws Exception {
    return null; // Use default HTTP download
  }

  /**
   * No-op implementation for convenience.
   */
  SchemaLifecycleListener NOOP = new SchemaLifecycleListener() {
    @Override public void beforeSchema(SchemaContext context) { }
    @Override public void afterSchema(SchemaContext context, SchemaResult result) { }
    @Override public void onSchemaError(SchemaContext context, Exception error) { }
  };
}
