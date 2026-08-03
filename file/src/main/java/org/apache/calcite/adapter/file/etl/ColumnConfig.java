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
import java.util.Set;

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
  private final String dateFormat;
  private final String onCoercionFailure;
  private final boolean required;
  private final boolean replace;

  private ColumnConfig(Builder builder) {
    this.name = builder.name;
    this.type = builder.type;
    this.source = builder.source;
    this.dateFormat = builder.dateFormat;
    this.onCoercionFailure = builder.onCoercionFailure;
    this.required = builder.required != null ? builder.required : true;
    this.replace = builder.replace != null ? builder.replace : false;

    if (builder.onCoercionFailure != null
        && !("FAIL".equals(builder.onCoercionFailure)
            || "WARN".equals(builder.onCoercionFailure)
            || "DROP".equals(builder.onCoercionFailure))) {
      throw new IllegalArgumentException(
          "Column '" + builder.name + "': onCoercionFailure must be FAIL, WARN, or DROP, got '"
              + builder.onCoercionFailure + "'");
    }

    String expr = builder.expression;
    if ((expr == null || expr.isEmpty()) && builder.dateFormat != null) {
      String effectiveSource = builder.source != null && !builder.source.isEmpty()
          ? builder.source : builder.name;
      // CAST to VARCHAR: every DateParseFormat expression uses TRIM/LIKE/TRY_STRPTIME/LPAD on
      // this reference, all of which require a string argument. Without the cast, a source
      // field DuckDB infers as numeric (e.g. an unquoted 8-digit JSON date like 20260115) fails
      // to bind — "No function matches trim(BIGINT)" — found live during the fec DQ reingest.
      expr = DateParseFormat.valueOf(builder.dateFormat)
          .toExpression("CAST(src.\"" + effectiveSource + "\" AS VARCHAR)");
    }
    this.expression = expr;
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
   * The expression is evaluated by DuckDB during materialization. For a column declared with
   * {@code dateFormat:} instead of an explicit {@code expression:}, this is the
   * auto-synthesized {@link DateParseFormat} expression, not literal YAML text.
   */
  public String getExpression() {
    return expression;
  }

  /**
   * Returns the {@link DateParseFormat} name declared via {@code dateFormat:}, or {@code null}
   * if this column didn't use that shorthand. Retained separately from {@link #getExpression()}
   * so a Java-side (non-DuckDB) evaluation path can dispatch directly to
   * {@link DateParseFormat#parse(String)} instead of re-parsing the generated SQL text.
   */
  public String getDateFormat() {
    return dateFormat;
  }

  /**
   * Returns this column's {@code onCoercionFailure} policy ({@code FAIL}/{@code WARN}/
   * {@code DROP}), or {@code null} if unset — an unset column behaves as {@code WARN}
   * (log and write NULL), matching this writer's historical default behavior.
   */
  public String getOnCoercionFailure() {
    return onCoercionFailure;
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
   * Returns whether this column replaces the raw source column in-place.
   * When true, the source column is excluded from the wildcard SELECT and the
   * expression result appears under this column's name instead.
   */
  public boolean isReplace() {
    return replace;
  }

  /**
   * Returns the effective source field name.
   * If source is specified, returns source; otherwise returns name.
   */
  public String getEffectiveSource() {
    return source != null && !source.isEmpty() ? source : name;
  }

  /**
   * Resolves a field named elsewhere in the table config (e.g. {@code incremental.dateField},
   * {@code rowFilter.column}) to the key that field actually carries in a <b>fetched</b> row.
   *
   * <p>Fetched rows are keyed by the raw source field name — the rename declared by
   * {@code source:} is applied later, during materialization. So a config that names the
   * logical column ({@code last_update}) does not match a row keyed by the source field
   * ({@code Registration.LastUpdateDate}). This accepts either spelling and returns the key
   * to look up at fetch time, so the YAML means the same thing whichever the author wrote.
   *
   * @param columns Declared columns, or null/empty when the table declares none
   * @param fieldName Field name from the table config; may be null
   * @return the fetch-time key, {@code fieldName} unchanged when there are no declared
   *         columns to resolve against, or null when the name matches no declared column
   *         and no declared source (a configuration error the caller should report)
   */
  public static String resolveSourceKey(List<ColumnConfig> columns, String fieldName) {
    if (fieldName == null || columns == null || columns.isEmpty()) {
      return fieldName;
    }
    for (ColumnConfig column : columns) {
      if (fieldName.equals(column.getName())) {
        // A purely computed column exists only after materialization — it has no fetch-time key.
        return column.isComputed() && column.source == null ? null : column.getEffectiveSource();
      }
    }
    for (ColumnConfig column : columns) {
      if (fieldName.equals(column.getEffectiveSource())) {
        return fieldName;
      }
    }
    return null;
  }

  /**
   * Returns the names a field may be referred to by, for error messages: every declared
   * column name plus every distinct source field name.
   *
   * @param columns Declared columns; may be null
   * @return sorted, de-duplicated candidate names
   */
  public static List<String> resolvableNames(List<ColumnConfig> columns) {
    java.util.SortedSet<String> names = new java.util.TreeSet<String>();
    if (columns != null) {
      for (ColumnConfig column : columns) {
        if (column.getName() != null) {
          names.add(column.getName());
        }
        if (column.source != null && !column.source.isEmpty()) {
          names.add(column.source);
        }
      }
    }
    return new ArrayList<String>(names);
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
  public String buildSelectExpression(String tableAlias, Set<String> sourceColumns) {
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
  public String buildSelectExpression(String tableAlias, Set<String> sourceColumns,
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
      Set<String> columnNames) {
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
    builder.dateFormat((String) map.get("dateFormat"));
    builder.onCoercionFailure((String) map.get("onCoercionFailure"));

    Object requiredObj = map.get("required");
    if (requiredObj instanceof Boolean) {
      builder.required((Boolean) requiredObj);
    }

    Object replaceObj = map.get("replace");
    if (replaceObj instanceof Boolean) {
      builder.replace((Boolean) replaceObj);
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
      return new ArrayList<ColumnConfig>();
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
    private String dateFormat;
    private String onCoercionFailure;
    private Boolean required;
    private Boolean replace;

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

    public Builder dateFormat(String dateFormat) {
      this.dateFormat = dateFormat;
      return this;
    }

    public Builder onCoercionFailure(String onCoercionFailure) {
      this.onCoercionFailure = onCoercionFailure;
      return this;
    }

    public Builder required(boolean required) {
      this.required = required;
      return this;
    }

    public Builder replace(boolean replace) {
      this.replace = replace;
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
