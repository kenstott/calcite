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
package org.apache.calcite.adapter.file.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Configuration for a partitioned table that spans multiple files.
 */
public class PartitionedTableConfig {
  private final String name;
  private final String pattern;
  private final String type;
  private final PartitionConfig partitions;
  private final String comment;
  private final Map<String, String> columnComments;
  private final List<TableColumn> columns;
  private final List<AlternatePartitionConfig> alternatePartitions;
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedTableConfig.class);

  public PartitionedTableConfig(String name, String pattern, String type,
                                PartitionConfig partitions) {
    this(name, pattern, type, partitions, null, null, null, null);
  }

  public PartitionedTableConfig(String name, String pattern, String type,
                                PartitionConfig partitions, String comment,
                                Map<String, String> columnComments) {
    this(name, pattern, type, partitions, comment, columnComments, null, null);
  }

  public PartitionedTableConfig(String name, String pattern, String type,
                                PartitionConfig partitions, String comment,
                                Map<String, String> columnComments, List<TableColumn> columns) {
    this(name, pattern, type, partitions, comment, columnComments, columns, null);
  }

  public PartitionedTableConfig(String name, String pattern, String type,
                                PartitionConfig partitions, String comment,
                                Map<String, String> columnComments, List<TableColumn> columns,
                                List<AlternatePartitionConfig> alternatePartitions) {
    this.name = name;
    this.pattern = pattern;
    this.type = type != null ? type : "partitioned";
    this.partitions = partitions;
    this.comment = comment;
    this.columnComments = columnComments;
    this.columns = columns;
    this.alternatePartitions = alternatePartitions;
  }

  public String getName() {
    return name;
  }

  public String getPattern() {
    return pattern;
  }

  public String getType() {
    return type;
  }

  public PartitionConfig getPartitions() {
    return partitions;
  }

  public String getComment() {
    return comment;
  }

  public Map<String, String> getColumnComments() {
    return columnComments;
  }

  public List<TableColumn> getColumns() {
    return columns;
  }

  public List<AlternatePartitionConfig> getAlternatePartitions() {
    return alternatePartitions;
  }

  /**
   * Configuration for partition scheme.
   */
  public static class PartitionConfig {
    private final String style;
    private final List<String> columns;
    private final List<ColumnDefinition> columnDefinitions;
    private final String regex;
    private final List<ColumnMapping> columnMappings;

    public PartitionConfig(String style, List<String> columns,
                           List<ColumnDefinition> columnDefinitions,
                           String regex, List<ColumnMapping> columnMappings) {
      this.style = style != null ? style : "auto";
      this.columns = columns;
      this.columnDefinitions = columnDefinitions;
      this.regex = regex;
      this.columnMappings = columnMappings;
    }

    public String getStyle() {
      return style;
    }

    public List<String> getColumns() {
      return columns;
    }

    public List<ColumnDefinition> getColumnDefinitions() {
      return columnDefinitions;
    }

    public String getRegex() {
      return regex;
    }

    public List<ColumnMapping> getColumnMappings() {
      return columnMappings;
    }
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
   * Maps a regex group to a column.
   */
  public static class ColumnMapping {
    private final String name;
    private final int group;
    private final String type;

    public ColumnMapping(String name, int group, String type) {
      this.name = name;
      this.group = group;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public int getGroup() {
      return group;
    }

    public String getType() {
      return type;
    }
  }

  /**
   * Configuration for an alternate partition scheme.
   * Alternate partitions provide different physical layouts of the same logical data,
   * allowing the query optimizer to select the best layout based on query predicates.
   */
  public static class AlternatePartitionConfig {
    private final String name;
    private final String pattern;
    private final PartitionConfig partition;
    private final String comment;
    private final List<String> batchPartitionColumns;
    private final Map<String, String> columnMappings;
    private final int threads;
    private final List<String> incrementalKeys;

    public AlternatePartitionConfig(String name, String pattern, PartitionConfig partition,
        String comment) {
      this(name, pattern, partition, comment, null, null, 0, null);
    }

    public AlternatePartitionConfig(String name, String pattern, PartitionConfig partition,
        String comment, List<String> batchPartitionColumns, Map<String, String> columnMappings) {
      this(name, pattern, partition, comment, batchPartitionColumns, columnMappings, 0, null);
    }

    public AlternatePartitionConfig(String name, String pattern, PartitionConfig partition,
        String comment, List<String> batchPartitionColumns, Map<String, String> columnMappings,
        int threads) {
      this(name, pattern, partition, comment, batchPartitionColumns, columnMappings, threads, null);
    }

    public AlternatePartitionConfig(String name, String pattern, PartitionConfig partition,
        String comment, List<String> batchPartitionColumns, Map<String, String> columnMappings,
        int threads, List<String> incrementalKeys) {
      this.name = name;
      this.pattern = pattern;
      this.partition = partition;
      this.comment = comment;
      this.batchPartitionColumns = batchPartitionColumns;
      this.columnMappings = columnMappings;
      this.threads = threads;
      this.incrementalKeys = incrementalKeys;
    }

    public String getName() {
      return name;
    }

    public String getPattern() {
      return pattern;
    }

    public PartitionConfig getPartition() {
      return partition;
    }

    public String getComment() {
      return comment;
    }

    /**
     * Returns columns used for batching during reorganization (e.g., [year, geo_fips_set]).
     * This helps process large datasets in manageable chunks to avoid OOM.
     */
    public List<String> getBatchPartitionColumns() {
      return batchPartitionColumns;
    }

    /**
     * Returns column mappings from target column name to source column name.
     * Used when source data has different column names than the alternate's partition keys.
     */
    public Map<String, String> getColumnMappings() {
      return columnMappings;
    }

    /**
     * Returns the number of DuckDB threads to use for this reorganization.
     * Higher values may speed up I/O-bound operations but use more memory.
     * Default is 2 if not specified.
     */
    public int getThreads() {
      return threads;
    }

    /**
     * Returns the incremental keys for this alternate partition.
     * When set, only batches with new values for these keys will be processed.
     * For example, if incrementalKeys = [year], only new years will be processed
     * on subsequent runs, avoiding full rebuild.
     *
     * @return List of column names used as incremental keys, or null if full rebuild is required
     */
    public List<String> getIncrementalKeys() {
      return incrementalKeys;
    }

    /**
     * Checks whether this alternate partition supports incremental processing.
     *
     * @return true if incremental keys are defined
     */
    public boolean supportsIncremental() {
      return incrementalKeys != null && !incrementalKeys.isEmpty();
    }

    /**
     * Returns the number of partition keys in this alternate partition scheme.
     * Used by the optimizer to select the scheme with the most keys.
     */
    public int getPartitionKeyCount() {
      if (partition == null || partition.getColumnDefinitions() == null) {
        return 0;
      }
      return partition.getColumnDefinitions().size();
    }
  }

  /**
   * Metadata for a table column including type, nullability, comment, and optional expression.
   * Columns with an expression are computed columns, where the expression is a SQL expression
   * that will be evaluated by the execution engine (e.g., DuckDB).
   *
   * <p>Example expressions:
   * <ul>
   *   <li>Date/time: {@code EXTRACT(YEAR FROM date)}</li>
   *   <li>String operations: {@code SUBSTR(fips, 1, 2)}</li>
   *   <li>Embeddings: {@code embed(text)::FLOAT[384]}</li>
   *   <li>Calculations: {@code price * quantity}</li>
   *   <li>Geospatial: {@code h3_latlng_to_cell(lat, lon, 7)}</li>
   * </ul>
   */
  public static class TableColumn {
    private final String name;
    private final String type;
    private final boolean nullable;
    private final String comment;
    private final String expression;
    private final String csvColumn;

    public TableColumn(String name, String type, boolean nullable, String comment) {
      this(name, type, nullable, comment, null, null);
    }

    public TableColumn(String name, String type, boolean nullable, String comment,
        String expression) {
      this(name, type, nullable, comment, expression, null);
    }

    public TableColumn(String name, String type, boolean nullable, String comment,
        String expression, String csvColumn) {
      this.name = name;
      this.type = type;
      this.nullable = nullable;
      this.comment = comment;
      this.expression = expression;
      this.csvColumn = csvColumn;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public boolean isNullable() {
      return nullable;
    }

    public String getComment() {
      return comment;
    }

    /**
     * Returns the SQL expression for this column, or null if this is a regular column.
     * The expression is evaluated by the execution engine during data conversion.
     *
     * @return SQL expression string, or null for regular columns
     */
    public String getExpression() {
      return expression;
    }

    /**
     * Checks whether this column has a non-empty expression.
     *
     * @return true if expression is defined and not empty
     */
    public boolean hasExpression() {
      return expression != null && !expression.trim().isEmpty();
    }

    /**
     * Checks whether this is a computed column (has an expression).
     *
     * @return true if this column has an expression
     */
    public boolean isComputed() {
      return hasExpression();
    }

    /**
     * Checks whether this column has a vector/array type.
     *
     * @return true if type starts with "array&lt;"
     */
    public boolean isVectorType() {
      return type != null && type.startsWith("array<");
    }

    /**
     * Returns the CSV column name if this column maps from a different CSV column name.
     * Used for CSV→Parquet conversions where the output column name differs from the CSV column name.
     *
     * @return CSV column name, or null if same as output column name
     */
    public String getCsvColumn() {
      return csvColumn;
    }
  }

  /**
   * Creates a PartitionedTableConfig from a map (JSON deserialization).
   */
  @SuppressWarnings("unchecked")
  public static PartitionedTableConfig fromMap(Map<String, Object> map) {
    String name = (String) map.get("name");
    String pattern = (String) map.get("pattern");
    String type = (String) map.get("type");
    String comment = (String) map.get("comment");
    Map<String, String> columnComments = parseColumnComments(map.get("column_comments"));
    List<TableColumn> columns = parseColumns(map.get("columns"));

    // Extract column comments from TableColumn objects if no separate column_comments was provided
    if ((columnComments == null || columnComments.isEmpty()) && columns != null) {
      columnComments = new java.util.LinkedHashMap<>();
      for (TableColumn col : columns) {
        if (col.getComment() != null) {
          columnComments.put(col.getName(), col.getComment());
        }
      }
      if (columnComments.isEmpty()) {
        columnComments = null;  // Keep consistent with parseColumnComments behavior
      }
    }

    PartitionConfig partitionConfig = null;
    Map<String, Object> partitionsMap = (Map<String, Object>) map.get("partitions");
    if (partitionsMap != null) {
      String style = (String) partitionsMap.get("style");

      // Handle column definitions - always use columnDefinitions format
      List<String> simpleColumns = null;
      List<ColumnDefinition> columnDefinitions = null;

      // Only support "columnDefinitions" format - no backward compatibility
      Object columnDefsObj = partitionsMap.get("columnDefinitions");
      if (columnDefsObj instanceof List) {
        List<?> defsList = (List<?>) columnDefsObj;
        if (!defsList.isEmpty()) {
          Object firstElem = defsList.get(0);
          if (firstElem instanceof String) {
            // Simple string array converted to ColumnDefinitions with default VARCHAR type
            simpleColumns = (List<String>) columnDefsObj;
            columnDefinitions = simpleColumns.stream()
                .map(colName -> new ColumnDefinition(colName, "VARCHAR"))
                .collect(java.util.stream.Collectors.toList());
          } else if (firstElem instanceof Map) {
            // Full definition with types: [{"name": "year", "type": "INTEGER"}, ...]
            columnDefinitions = ((List<Map<String, Object>>) defsList).stream()
                .map(
                    m -> new ColumnDefinition(
                    (String) m.get("name"),
                    (String) m.get("type")))
                .collect(java.util.stream.Collectors.toList());
            // Extract column names for APIs that need them
            simpleColumns = columnDefinitions.stream()
                .map(ColumnDefinition::getName)
                .collect(java.util.stream.Collectors.toList());
          }
        }
      }

      String regex = (String) partitionsMap.get("regex");

      List<ColumnMapping> columnMappings = null;
      List<Map<String, Object>> mappingsList =
          (List<Map<String, Object>>) partitionsMap.get("columnMappings");
      if (mappingsList != null) {
        columnMappings = mappingsList.stream()
            .map(
                m -> new ColumnMapping(
                (String) m.get("name"),
                ((Number) m.get("group")).intValue(),
                (String) m.get("type")))
            .collect(java.util.stream.Collectors.toList());
      }

      partitionConfig =
          new PartitionConfig(style, simpleColumns,
              columnDefinitions, regex, columnMappings);
    }

    // Parse alternate_partitions
    List<AlternatePartitionConfig> alternatePartitions = parseAlternatePartitions(
        map.get("alternate_partitions"));

    return new PartitionedTableConfig(name, pattern, type, partitionConfig, comment,
        columnComments, columns, alternatePartitions);
  }

  /**
   * Parses alternate_partitions from JSON format.
   * Expects: [{"name": "alt_name", "pattern": "path/pattern", "partition": {...}, "comment": "..."}, ...]
   */
  @SuppressWarnings("unchecked")
  private static List<AlternatePartitionConfig> parseAlternatePartitions(Object obj) {
    if (!(obj instanceof List)) {
      return null;
    }

    List<AlternatePartitionConfig> result = new java.util.ArrayList<>();
    List<?> list = (List<?>) obj;

    for (Object item : list) {
      if (!(item instanceof Map)) {
        continue;
      }

      Map<String, Object> m = (Map<String, Object>) item;
      String altName = (String) m.get("name");
      String altPattern = (String) m.get("pattern");
      String altComment = (String) m.get("comment");

      if (altName == null || altPattern == null) {
        continue;
      }

      // Parse partition config for alternate
      PartitionConfig altPartition = null;
      Map<String, Object> partitionMap = (Map<String, Object>) m.get("partition");
      if (partitionMap != null) {
        String style = (String) partitionMap.get("style");
        List<ColumnDefinition> columnDefs = null;
        List<String> simpleColumns = null;

        Object columnDefsObj = partitionMap.get("columnDefinitions");
        if (columnDefsObj instanceof List) {
          List<?> defsList = (List<?>) columnDefsObj;
          if (!defsList.isEmpty()) {
            Object firstElem = defsList.get(0);
            if (firstElem instanceof Map) {
              columnDefs = ((List<Map<String, Object>>) defsList).stream()
                  .map(cm -> new ColumnDefinition(
                      (String) cm.get("name"),
                      (String) cm.get("type")))
                  .collect(java.util.stream.Collectors.toList());
              simpleColumns = columnDefs.stream()
                  .map(ColumnDefinition::getName)
                  .collect(java.util.stream.Collectors.toList());
            }
          }
        }

        altPartition = new PartitionConfig(style, simpleColumns, columnDefs, null, null);
      }

      // Parse batch_partition_columns
      List<String> batchPartitionColumns = null;
      Object batchColsObj = m.get("batch_partition_columns");
      if (batchColsObj instanceof List) {
        batchPartitionColumns = new java.util.ArrayList<>();
        for (Object col : (List<?>) batchColsObj) {
          if (col instanceof String) {
            batchPartitionColumns.add((String) col);
          }
        }
      }

      // Parse column_mappings: {target_col: source_col, ...}
      Map<String, String> columnMappings = null;
      Object mappingsObj = m.get("column_mappings");
      if (mappingsObj instanceof Map) {
        columnMappings = new java.util.LinkedHashMap<>();
        Map<?, ?> mappingsMap = (Map<?, ?>) mappingsObj;
        for (Map.Entry<?, ?> entry : mappingsMap.entrySet()) {
          if (entry.getKey() instanceof String && entry.getValue() instanceof String) {
            columnMappings.put((String) entry.getKey(), (String) entry.getValue());
          }
        }
      }

      // Parse threads (optional, defaults to 2)
      int threads = 0;
      Object threadsObj = m.get("threads");
      if (threadsObj instanceof Number) {
        threads = ((Number) threadsObj).intValue();
      }

      // Parse incremental_keys (optional, enables incremental processing)
      List<String> incrementalKeys = null;
      Object incKeysObj = m.get("incremental_keys");
      if (incKeysObj instanceof List) {
        incrementalKeys = new java.util.ArrayList<>();
        for (Object key : (List<?>) incKeysObj) {
          if (key instanceof String) {
            incrementalKeys.add((String) key);
          }
        }
        if (incrementalKeys.isEmpty()) {
          incrementalKeys = null;
        }
      }

      result.add(new AlternatePartitionConfig(altName, altPattern, altPartition, altComment,
          batchPartitionColumns, columnMappings, threads, incrementalKeys));
    }

    return result.isEmpty() ? null : result;
  }

  /**
   * Parses column_comments from JSON format.
   * Expects: [{"name": "col1", "comment": "Comment 1"}, ...]
   */
  @SuppressWarnings("unchecked")
  private static Map<String, String> parseColumnComments(Object obj) {
    if (obj instanceof List) {
      Map<String, String> result = new java.util.LinkedHashMap<>();
      List<?> list = (List<?>) obj;
      for (Object item : list) {
        if (item instanceof Map) {
          Map<?, ?> m = (Map<?, ?>) item;
          String name = (String) m.get("name");
          String commentText = (String) m.get("comment");
          if (name != null && commentText != null) {
            result.put(name, commentText);
          }
        }
      }
      return result.isEmpty() ? null : result;
    }
    return null;
  }

  /**
   * Parses columns from JSON format.
   * Expects: [{"name": "col1", "type": "string", "nullable": false, "comment": "Comment 1",
   *            "expression": "EXTRACT(YEAR FROM date)"}, ...]
   */
  @SuppressWarnings("unchecked")
  private static List<TableColumn> parseColumns(Object obj) {
    if (obj instanceof List) {
      List<TableColumn> result = new java.util.ArrayList<>();
      List<?> list = (List<?>) obj;
      for (Object item : list) {
        if (item instanceof Map) {
          Map<?, ?> m = (Map<?, ?>) item;
          String name = (String) m.get("name");
          String type = (String) m.get("type");
          Boolean nullableObj = (Boolean) m.get("nullable");
          boolean nullable = nullableObj != null ? nullableObj : false;
          String comment = (String) m.get("comment");
          String expression = (String) m.get("expression");
          String csvColumn = (String) m.get("csvColumn");
          if (name != null) {
            result.add(new TableColumn(name, type, nullable, comment, expression, csvColumn));
          }
        }
      }
      return result.isEmpty() ? null : result;
    }
    return null;
  }
}
