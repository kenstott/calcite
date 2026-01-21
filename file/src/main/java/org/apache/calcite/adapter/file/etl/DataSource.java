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
 * Interface for data sources that can be materialized to Parquet.
 *
 * <p>DataSource implementations provide data as an iterator of Maps,
 * where each Map represents a row with column name to value mappings.
 *
 * <h3>Implementations</h3>
 * <ul>
 *   <li>File-based: JSON, CSV files read via DuckDB</li>
 *   <li>HTTP-based: REST API responses (future Phase 3)</li>
 *   <li>Query-based: SQL query results (future)</li>
 * </ul>
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * DataSource source = JsonDataSource.fromFile("/path/to/data.json");
 * Iterator<Map<String, Object>> data = source.fetch(variables);
 * }</pre>
 *
 * @see HiveParquetWriter
 */
public interface DataSource {

  /**
   * Fetches data from the source with variable substitution.
   *
   * @param variables Variable values for substitution (e.g., year, region)
   * @return Iterator of rows, each as a Map of column name to value
   * @throws IOException If data cannot be fetched
   */
  Iterator<Map<String, Object>> fetch(Map<String, String> variables) throws IOException;

  /**
   * Returns the source type identifier.
   *
   * @return Source type (e.g., "file", "http", "query")
   */
  String getType();

  /**
   * Returns an estimate of total row count, or -1 if unknown.
   * Used for progress reporting during materialization.
   */
  default long estimateRowCount() {
    return -1;
  }

  /**
   * Closes resources associated with this data source.
   */
  default void close() throws IOException {
    // Default: no-op
  }
}
