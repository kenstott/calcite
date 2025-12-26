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
 * Configuration for a column in a materialized table.
 *
 * <p>Columns can be direct mappings from source data or computed via SQL expressions:
 *
 * <h3>Direct Column Mapping</h3>
 * <pre>{@code
 * - name: region_code
 *   type: VARCHAR
 *   source: regionCode      # Maps from source field "regionCode"
 * }</pre>
 *
 * <h3>Computed Column</h3>
 * <pre>{@code
 * - name: quarter
 *   type: VARCHAR
 *   expression: "SUBSTR(period, 1, 2)"
 * }</pre>
 *
 * <h3>Value Normalization</h3>
 * <pre>{@code
 * - name: value
 *   type: DECIMAL(15,2)
 *   source: DataValue
 *   expression: "CASE WHEN DataValue IN ('(NA)', '(D)') THEN NULL ELSE CAST(DataValue AS DECIMAL(15,2)) END"
 * }</pre>
 */
public class ColumnConfig {

  private final String name;
  private final String type;
  private final String source;
  private final String expression;
  private final boolean required;

  private ColumnConfig(Builder builder) {
    this.name = builder.name;
    this.type = builder.type;
    this.source = builder.source;
    this.expression = builder.expression;
    this.required = builder.required != null ? builder.required : true;
  }

  /**
   * Returns the output column name.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the SQL type (e.g., VARCHAR, INTEGER, DECIMAL(15,2)).
   */
  public String getType() {
    return type;
  }

  /**
   * Returns the source field name in the input data.
   * May be null if this is a computed column.
   */
  public String getSource() {
    return source;
  }

  /**
   * Returns the SQL expression for computed columns.
   * The expression is evaluated by DuckDB during materialization.
   */
  public String getExpression() {
    return expression;
  }

  /**
   * Returns whether this column is required in the source data.
   * If true, materialization will fail if the source field is missing.
   */
  public boolean isRequired() {
    return required;
  }

  /**
   * Checks if this is a computed column (has an expression).
   */
  public boolean isComputed() {
    return expression != null && !expression.isEmpty();
  }

  /**
   * Returns the effective source field name.
   * If source is specified, returns source; otherwise returns name.
   */
  public String getEffectiveSource() {
    return source != null && !source.isEmpty() ? source : name;
  }

  /**
   * Builds the SELECT clause fragment for this column.
   * For computed columns, returns the expression with alias.
   * For direct columns, returns the source field (possibly renamed).
   */
  public String buildSelectExpression() {
    if (isComputed()) {
      return expression + " AS " + name;
    } else if (source != null && !source.equals(name)) {
      // Rename source to target name
      return "\"" + source + "\" AS " + name;
    } else {
      return name;
    }
  }

  /**
   * Builds the SELECT clause fragment for this column with a table alias prefix.
   * This is used to avoid DuckDB's column reference ambiguity when computed columns
   * reference source columns that are also being selected.
   *
   * <p>For source columns, returns: tableAlias."sourceName" AS targetName
   * <p>For computed columns with source column references, qualifies those references
   *
   * @param tableAlias The table alias to prefix source columns with (e.g., "src")
   * @param sourceColumns List of source column names that should be qualified in expressions
   * @return SELECT clause fragment
   */
  public String buildSelectExpression(String tableAlias, java.util.Set<String> sourceColumns) {
    return buildSelectExpression(tableAlias, sourceColumns, null);
  }

  /**
   * Builds the SELECT clause fragment for this column with a table alias prefix
   * and partition variable substitution.
   *
   * <p>For source columns, returns: tableAlias."sourceName" AS targetName
   * <p>For computed columns, qualifies column references and substitutes partition variables
   *
   * @param tableAlias The table alias to prefix source columns with (e.g., "src")
   * @param sourceColumns List of source column names that should be qualified in expressions
   * @param partitionVariables Map of partition variable names to values for substitution
   * @return SELECT clause fragment
   */
  public String buildSelectExpression(String tableAlias, java.util.Set<String> sourceColumns,
      Map<String, String> partitionVariables) {
    if (isComputed()) {
      String expr = expression;
      // First, substitute partition variables (e.g., {tablename} -> 'SAINC1')
      if (partitionVariables != null && !partitionVariables.isEmpty()) {
        expr = substitutePartitionVariables(expr, partitionVariables);
      }
      // Then qualify column references in the expression
      String qualifiedExpr = qualifyColumnReferences(expr, tableAlias, sourceColumns);
      return qualifiedExpr + " AS " + name;
    } else {
      // Qualify source column with table alias
      String sourceName = source != null && !source.isEmpty() ? source : name;

      // If source doesn't have this column but it's available as a partition variable,
      // use the partition variable value as a literal. This handles the case where
      // dimension values (e.g., year from query params) aren't echoed back in API responses.
      if (sourceColumns != null && !sourceColumns.contains(sourceName)
          && partitionVariables != null && partitionVariables.containsKey(name)) {
        String value = partitionVariables.get(name);
        // Escape single quotes in value
        value = value.replace("'", "''");
        return "'" + value + "' AS " + name;
      }

      return tableAlias + ".\"" + sourceName + "\" AS " + name;
    }
  }

  /**
   * Substitutes partition variable placeholders in an expression.
   * Placeholders are in the format {variableName} and are replaced with the literal value.
   *
   * @param expr The SQL expression containing placeholders
   * @param variables Map of variable names to values
   * @return Expression with placeholders replaced
   */
  private String substitutePartitionVariables(String expr, Map<String, String> variables) {
    String result = expr;
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      String placeholder = "{" + entry.getKey() + "}";
      result = result.replace(placeholder, entry.getValue());
    }
    return result;
  }

  /**
   * Qualifies column references in an expression by prefixing them with a table alias.
   * Uses word boundary matching to avoid replacing column names inside strings or identifiers.
   *
   * @param expr The SQL expression
   * @param tableAlias The table alias to prefix columns with
   * @param columnNames Set of column names to qualify
   * @return Expression with qualified column references
   */
  private String qualifyColumnReferences(String expr, String tableAlias,
      java.util.Set<String> columnNames) {
    if (columnNames == null || columnNames.isEmpty()) {
      return expr;
    }

    String result = expr;
    for (String colName : columnNames) {
      // Use word boundary regex to match column names not already qualified
      // This pattern matches the column name when it's not preceded by a dot or quote
      // and followed by word boundary (not alphanumeric or underscore)
      String pattern = "(?<![.\"a-zA-Z0-9_])" + java.util.regex.Pattern.quote(colName) + "(?![a-zA-Z0-9_])";
      result = result.replaceAll(pattern, tableAlias + ".\"" + colName + "\"");
    }
    return result;
  }

  /**
   * Creates a new builder for ColumnConfig.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a ColumnConfig from a YAML/JSON map.
   *
   * @param map Configuration map with keys: name, type, source, expression, required
   * @return ColumnConfig instance
   */
  public static ColumnConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    Builder builder = builder();
    builder.name((String) map.get("name"));
    builder.type((String) map.get("type"));
    builder.source((String) map.get("source"));
    builder.expression((String) map.get("expression"));

    Object requiredObj = map.get("required");
    if (requiredObj instanceof Boolean) {
      builder.required((Boolean) requiredObj);
    }

    return builder.build();
  }

  /**
   * Parses a list of column configurations from a YAML/JSON list.
   *
   * @param list List of column configuration maps
   * @return List of ColumnConfig instances
   */
  @SuppressWarnings("unchecked")
  public static List<ColumnConfig> fromList(List<?> list) {
    if (list == null || list.isEmpty()) {
      return Collections.emptyList();
    }

    List<ColumnConfig> result = new ArrayList<ColumnConfig>();
    for (Object item : list) {
      if (item instanceof Map) {
        ColumnConfig config = fromMap((Map<String, Object>) item);
        if (config != null && config.getName() != null) {
          result.add(config);
        }
      }
    }
    return result;
  }

  /**
   * Builder for ColumnConfig.
   */
  public static class Builder {
    private String name;
    private String type;
    private String source;
    private String expression;
    private Boolean required;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder type(String type) {
      this.type = type;
      return this;
    }

    public Builder source(String source) {
      this.source = source;
      return this;
    }

    public Builder expression(String expression) {
      this.expression = expression;
      return this;
    }

    public Builder required(boolean required) {
      this.required = required;
      return this;
    }

    public ColumnConfig build() {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Column name is required");
      }
      return new ColumnConfig(this);
    }
  }
}
