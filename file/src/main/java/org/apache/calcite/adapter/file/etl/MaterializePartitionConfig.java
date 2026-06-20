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
  private final List<ColumnDefinition> columnDefinitions;
  private final Map<String, String> valueSource;

  private MaterializePartitionConfig(Builder builder) {
    this.columns = builder.columns != null
        ? Collections.unmodifiableList(new ArrayList<String>(builder.columns))
        : Collections.<String>emptyList();
    this.batchBy = builder.batchBy != null
        ? Collections.unmodifiableList(new ArrayList<String>(builder.batchBy))
        : Collections.<String>emptyList();
    this.columnDefinitions = builder.columnDefinitions != null
        ? Collections.unmodifiableList(new ArrayList<ColumnDefinition>(builder.columnDefinitions))
        : Collections.<ColumnDefinition>emptyList();
    this.valueSource = builder.valueSource != null
        ? Collections.unmodifiableMap(new java.util.LinkedHashMap<String, String>(builder.valueSource))
        : Collections.<String, String>emptyMap();
  }

  /**
   * Definition of a partition column with name and type.
   */
  public static class ColumnDefinition {
    private final String name;
    private final String type;

    public ColumnDefinition(String name, String type) {
      this.name = name;
      this.type = type != null ? type : "VARCHAR";
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }
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
   * Returns the column definitions with name and type information.
   * Used for creating Iceberg schemas with correct partition column types.
   */
  public List<ColumnDefinition> getColumnDefinitions() {
    return columnDefinitions;
  }

  /**
   * Optional mapping of partition column -&gt; the combo/fetch variable its value is sourced from,
   * when that differs from the column name. E.g. {@code {year: effective_year}} means the
   * {@code year} partition directory is written from the combo's {@code effective_year} value
   * (geo TIGER: a requested year falls back to the latest published effective_year). Used by the
   * self-heal / skip-if-materialized projection so a combo is matched to the partition it will
   * actually produce. Empty = identity (each partition column sources from the same-named variable).
   */
  public Map<String, String> getValueSource() {
    return valueSource;
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

    Object columnDefsObj = map.get("columnDefinitions");
    if (columnDefsObj instanceof List) {
      List<ColumnDefinition> columnDefs = new ArrayList<ColumnDefinition>();
      for (Object defObj : (List<?>) columnDefsObj) {
        if (defObj instanceof Map) {
          Map<String, Object> defMap = (Map<String, Object>) defObj;
          String name = (String) defMap.get("name");
          String type = (String) defMap.get("type");
          if (name != null) {
            columnDefs.add(new ColumnDefinition(name, type));
          }
        }
      }
      builder.columnDefinitions(columnDefs);
    }

    Object valueSourceObj = map.get("valueSource");
    if (valueSourceObj instanceof Map) {
      Map<String, String> vs = new java.util.LinkedHashMap<String, String>();
      for (Map.Entry<String, Object> e : ((Map<String, Object>) valueSourceObj).entrySet()) {
        if (e.getValue() != null) {
          vs.put(e.getKey(), String.valueOf(e.getValue()));
        }
      }
      builder.valueSource(vs);
    }

    return builder.build();
  }

  /**
   * Builder for MaterializePartitionConfig.
   */
  public static class Builder {
    private List<String> columns;
    private List<String> batchBy;
    private List<ColumnDefinition> columnDefinitions;
    private Map<String, String> valueSource;

    public Builder columns(List<String> columns) {
      this.columns = columns;
      return this;
    }

    public Builder valueSource(Map<String, String> valueSource) {
      this.valueSource = valueSource;
      return this;
    }

    public Builder batchBy(List<String> batchBy) {
      this.batchBy = batchBy;
      return this;
    }

    public Builder columnDefinitions(List<ColumnDefinition> columnDefinitions) {
      this.columnDefinitions = columnDefinitions;
      return this;
    }

    public MaterializePartitionConfig build() {
      return new MaterializePartitionConfig(this);
    }
  }
}
