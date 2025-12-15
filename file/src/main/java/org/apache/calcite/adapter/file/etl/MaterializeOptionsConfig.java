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
 * Configuration for DuckDB and Parquet writer options.
 *
 * <p>Specifies performance and resource settings:
 * <ul>
 *   <li>threads - Number of DuckDB threads for parallel processing</li>
 *   <li>rowGroupSize - Number of rows per Parquet row group</li>
 *   <li>preserveInsertionOrder - Whether to maintain row order (default: false for performance)</li>
 * </ul>
 *
 * <h3>YAML Configuration</h3>
 * <pre>{@code
 * options:
 *   threads: 4
 *   rowGroupSize: 100000
 *   preserveInsertionOrder: false
 * }</pre>
 *
 * <h3>Performance Considerations</h3>
 * <ul>
 *   <li>Higher thread count speeds up I/O-bound operations but uses more memory</li>
 *   <li>Larger row groups improve compression but require more memory to buffer</li>
 *   <li>Disabling insertion order preservation improves performance significantly</li>
 * </ul>
 */
public class MaterializeOptionsConfig {
  private static final int DEFAULT_THREADS = 2;
  private static final int DEFAULT_ROW_GROUP_SIZE = 100000;
  private static final boolean DEFAULT_PRESERVE_INSERTION_ORDER = false;

  private final int threads;
  private final int rowGroupSize;
  private final boolean preserveInsertionOrder;

  private MaterializeOptionsConfig(Builder builder) {
    this.threads = builder.threads > 0 ? builder.threads : DEFAULT_THREADS;
    this.rowGroupSize = builder.rowGroupSize > 0 ? builder.rowGroupSize : DEFAULT_ROW_GROUP_SIZE;
    this.preserveInsertionOrder = builder.preserveInsertionOrder != null
        ? builder.preserveInsertionOrder : DEFAULT_PRESERVE_INSERTION_ORDER;
  }

  /**
   * Returns the number of DuckDB threads to use.
   * Higher values may speed up I/O-bound operations but use more memory.
   */
  public int getThreads() {
    return threads;
  }

  /**
   * Returns the target number of rows per Parquet row group.
   * Larger row groups improve compression but require more memory to buffer.
   */
  public int getRowGroupSize() {
    return rowGroupSize;
  }

  /**
   * Returns whether to preserve insertion order.
   * Setting this to false improves performance but rows may be reordered.
   */
  public boolean isPreserveInsertionOrder() {
    return preserveInsertionOrder;
  }

  /**
   * Creates a new builder for MaterializeOptionsConfig.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a MaterializeOptionsConfig with default settings.
   */
  public static MaterializeOptionsConfig defaults() {
    return builder().build();
  }

  /**
   * Creates a MaterializeOptionsConfig from a YAML/JSON map.
   *
   * @param map Configuration map with keys: threads, rowGroupSize, preserveInsertionOrder
   * @return MaterializeOptionsConfig instance
   */
  public static MaterializeOptionsConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return defaults();
    }

    Builder builder = builder();

    Object threadsObj = map.get("threads");
    if (threadsObj instanceof Number) {
      builder.threads(((Number) threadsObj).intValue());
    }

    Object rowGroupSizeObj = map.get("rowGroupSize");
    if (rowGroupSizeObj instanceof Number) {
      builder.rowGroupSize(((Number) rowGroupSizeObj).intValue());
    }

    Object preserveOrderObj = map.get("preserveInsertionOrder");
    if (preserveOrderObj instanceof Boolean) {
      builder.preserveInsertionOrder((Boolean) preserveOrderObj);
    }

    return builder.build();
  }

  /**
   * Builder for MaterializeOptionsConfig.
   */
  public static class Builder {
    private int threads;
    private int rowGroupSize;
    private Boolean preserveInsertionOrder;

    public Builder threads(int threads) {
      this.threads = threads;
      return this;
    }

    public Builder rowGroupSize(int rowGroupSize) {
      this.rowGroupSize = rowGroupSize;
      return this;
    }

    public Builder preserveInsertionOrder(boolean preserveInsertionOrder) {
      this.preserveInsertionOrder = preserveInsertionOrder;
      return this;
    }

    public MaterializeOptionsConfig build() {
      return new MaterializeOptionsConfig(this);
    }
  }
}
