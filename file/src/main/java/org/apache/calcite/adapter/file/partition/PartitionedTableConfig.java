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

  public PartitionedTableConfig(String name, String pattern, String type,
                                PartitionConfig partitions) {
    this(name, pattern, type, partitions, null, null, null);
  }

  public PartitionedTableConfig(String name, String pattern, String type,
                                PartitionConfig partitions, String comment,
                                Map<String, String> columnComments) {
    this(name, pattern, type, partitions, comment, columnComments, null);
  }

  public PartitionedTableConfig(String name, String pattern, String type,
                                PartitionConfig partitions, String comment,
                                Map<String, String> columnComments, List<TableColumn> columns) {
    this.name = name;
    this.pattern = pattern;
    this.type = type != null ? type : "partitioned";
    this.partitions = partitions;
    this.comment = comment;
    this.columnComments = columnComments;
    this.columns = columns;
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

    public TableColumn(String name, String type, boolean nullable, String comment) {
      this(name, type, nullable, comment, null);
    }

    public TableColumn(String name, String type, boolean nullable, String comment,
        String expression) {
      this.name = name;
      this.type = type;
      this.nullable = nullable;
      this.comment = comment;
      this.expression = expression;
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

    return new PartitionedTableConfig(name, pattern, type, partitionConfig, comment, columnComments, columns);
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
          if (name != null) {
            result.add(new TableColumn(name, type, nullable, comment, expression));
          }
        }
      }
      return result.isEmpty() ? null : result;
    }
    return null;
  }
}
