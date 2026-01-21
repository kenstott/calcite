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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Functional interface for providing data to the ETL pipeline.
 *
 * <p>Implementations can fetch data from any source: HTTP APIs, FTP servers,
 * databases, message queues, etc.
 *
 * <p>This interface is used by {@link EtlPipeline} to fetch data for each
 * batch (dimension combination). If a custom DataProvider is supplied and
 * returns non-null, the built-in HttpSource is skipped.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * // Custom FTP data provider
 * DataProvider ftpProvider = (config, variables) -> {
 *     String host = config.getSource().getCustomConfig().get("host");
 *     String path = substituteVariables(config.getSource().getCustomConfig().get("path"), variables);
 *     return ftpClient.downloadAndParse(host, path);
 * };
 *
 * EtlPipeline pipeline = new EtlPipeline(config, materializeDir, ftpProvider);
 * }</pre>
 *
 * @see EtlPipeline
 * @see TableLifecycleListener#fetchData
 */
@FunctionalInterface
public interface DataProvider {

  /**
   * Fetches data for a batch.
   *
   * @param config Pipeline configuration with source settings
   * @param variables Dimension values for this batch
   * @return Iterator of records (each record is a Map of column name to value)
   * @throws IOException If data fetching fails
   */
  Iterator<Map<String, Object>> fetch(EtlPipelineConfig config, Map<String, String> variables)
      throws IOException;

  /**
   * Default provider that returns null, indicating built-in HttpSource should be used.
   */
  DataProvider DEFAULT = (config, variables) -> null;
}
