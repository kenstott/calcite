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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Configuration for partitioning and batching during materialization.
 *
 * <p>Specifies how data should be partitioned and processed:
 * <ul>
 *   <li>columns - Partition columns for DuckDB PARTITION_BY</li>
 *   <li>batchBy - Columns to use for batch processing (to avoid OOM)</li>
 * </ul>
 *
 * <h3>YAML Configuration</h3>
 * <pre>{@code
 * partition:
 *   columns: [year, region]  # DuckDB PARTITION_BY columns
 *   batchBy: [year]          # Process one year at a time
 * }</pre>
 *
 * <h3>Batching Strategy</h3>
 * <p>When processing large datasets, batching prevents OOM by processing
 * data in chunks. For example, with {@code batchBy: [year]}, the writer
 * will process each year separately rather than loading all data at once.
 */
public class MaterializePartitionConfig {

  private final List<String> columns;
  private final List<String> batchBy;

  private MaterializePartitionConfig(Builder builder) {
    this.columns = builder.columns != null
        ? Collections.unmodifiableList(new ArrayList<String>(builder.columns))
        : Collections.<String>emptyList();
    this.batchBy = builder.batchBy != null
        ? Collections.unmodifiableList(new ArrayList<String>(builder.batchBy))
        : Collections.<String>emptyList();
  }

  /**
   * Returns the partition columns for DuckDB PARTITION_BY.
   * These columns determine the directory structure of output files.
   */
  public List<String> getColumns() {
    return columns;
  }

  /**
   * Returns the columns used for batch processing.
   * Data is processed in batches based on unique values of these columns
   * to avoid loading the entire dataset into memory.
   */
  public List<String> getBatchBy() {
    return batchBy;
  }

  /**
   * Checks if batching is configured.
   */
  public boolean hasBatching() {
    return !batchBy.isEmpty();
  }

  /**
   * Creates a new builder for MaterializePartitionConfig.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a MaterializePartitionConfig from a YAML/JSON map.
   *
   * @param map Configuration map with keys: columns, batchBy
   * @return MaterializePartitionConfig instance
   */
  @SuppressWarnings("unchecked")
  public static MaterializePartitionConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    Builder builder = builder();

    Object columnsObj = map.get("columns");
    if (columnsObj instanceof List) {
      List<String> columns = new ArrayList<String>();
      for (Object col : (List<?>) columnsObj) {
        if (col instanceof String) {
          columns.add((String) col);
        }
      }
      builder.columns(columns);
    }

    Object batchByObj = map.get("batchBy");
    if (batchByObj instanceof List) {
      List<String> batchBy = new ArrayList<String>();
      for (Object col : (List<?>) batchByObj) {
        if (col instanceof String) {
          batchBy.add((String) col);
        }
      }
      builder.batchBy(batchBy);
    }

    return builder.build();
  }

  /**
   * Builder for MaterializePartitionConfig.
   */
  public static class Builder {
    private List<String> columns;
    private List<String> batchBy;

    public Builder columns(List<String> columns) {
      this.columns = columns;
      return this;
    }

    public Builder batchBy(List<String> batchBy) {
      this.batchBy = batchBy;
      return this;
    }

    public MaterializePartitionConfig build() {
      return new MaterializePartitionConfig(this);
    }
  }
}
