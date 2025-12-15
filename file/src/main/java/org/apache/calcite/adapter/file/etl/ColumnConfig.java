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
