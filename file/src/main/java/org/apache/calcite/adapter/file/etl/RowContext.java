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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Context information passed to {@link RowTransformer} during row transformation.
 *
 * <p>RowContext provides access to the dimension values, table configuration,
 * and row number during row-by-row transformation. This allows transformers
 * to make context-aware decisions during transformation.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * public class MyRowTransformer implements RowTransformer {
 *     public Map<String, Object> transform(Map<String, Object> row, RowContext context) {
 *         String year = context.getDimensionValues().get("year");
 *         long rowNum = context.getRowNumber();
 *         // Transform based on context
 *         return transformedRow;
 *     }
 * }
 * }</pre>
 *
 * @see RowTransformer
 */
public class RowContext {

  private final Map<String, String> dimensionValues;
  private final EtlPipelineConfig tableConfig;
  private final long rowNumber;

  private RowContext(Builder builder) {
    this.dimensionValues = builder.dimensionValues != null
        ? Collections.unmodifiableMap(new LinkedHashMap<String, String>(builder.dimensionValues))
        : Collections.<String, String>emptyMap();
    this.tableConfig = builder.tableConfig;
    this.rowNumber = builder.rowNumber;
  }

  /**
   * Returns the dimension values for the current batch.
   *
   * @return Unmodifiable map of dimension name to value
   */
  public Map<String, String> getDimensionValues() {
    return dimensionValues;
  }

  /**
   * Returns the table/pipeline configuration.
   *
   * @return The ETL pipeline configuration, or null if not set
   */
  public EtlPipelineConfig getTableConfig() {
    return tableConfig;
  }

  /**
   * Returns the current row number (0-based index within the current batch).
   *
   * @return The row number
   */
  public long getRowNumber() {
    return rowNumber;
  }

  /**
   * Creates a new builder for RowContext.
   *
   * @return A new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("RowContext{rowNumber=").append(rowNumber);
    if (!dimensionValues.isEmpty()) {
      sb.append(", dimensionValues=").append(dimensionValues);
    }
    if (tableConfig != null) {
      sb.append(", tableName='").append(tableConfig.getName()).append("'");
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Builder for RowContext.
   */
  public static class Builder {
    private Map<String, String> dimensionValues;
    private EtlPipelineConfig tableConfig;
    private long rowNumber;

    /**
     * Sets the dimension values for the current batch.
     *
     * @param dimensionValues Map of dimension name to value
     * @return This builder
     */
    public Builder dimensionValues(Map<String, String> dimensionValues) {
      this.dimensionValues = dimensionValues;
      return this;
    }

    /**
     * Sets the table/pipeline configuration.
     *
     * @param tableConfig The ETL pipeline configuration
     * @return This builder
     */
    public Builder tableConfig(EtlPipelineConfig tableConfig) {
      this.tableConfig = tableConfig;
      return this;
    }

    /**
     * Sets the current row number.
     *
     * @param rowNumber The row number (0-based)
     * @return This builder
     */
    public Builder rowNumber(long rowNumber) {
      this.rowNumber = rowNumber;
      return this;
    }

    /**
     * Builds the RowContext.
     *
     * @return A new RowContext instance
     */
    public RowContext build() {
      return new RowContext(this);
    }
  }
}
