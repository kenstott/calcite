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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for a single dimension in batch processing.
 *
 * <p>Dimensions define the parameter space for ETL batch operations.
 * The {@link DimensionIterator} expands dimension configurations into
 * concrete value combinations.
 *
 * <h3>YAML Configuration Examples</h3>
 *
 * <p><b>Range dimension</b> - Numeric sequence:
 * <pre>{@code
 * year:
 *   type: range
 *   start: 2020
 *   end: 2024
 *   step: 1  # optional, defaults to 1
 * }</pre>
 *
 * <p><b>List dimension</b> - Explicit values:
 * <pre>{@code
 * frequency:
 *   type: list
 *   values: [A, M, Q]
 * }</pre>
 *
 * <p><b>Query dimension</b> - SQL-driven values:
 * <pre>{@code
 * region:
 *   type: query
 *   sql: "SELECT DISTINCT region FROM regions WHERE active = true"
 * }</pre>
 *
 * <p><b>Year range dimension</b> - With current year support:
 * <pre>{@code
 * year:
 *   type: yearRange
 *   start: 2020
 *   end: current
 * }</pre>
 *
 * @see DimensionType
 * @see DimensionIterator
 */
public class DimensionConfig {

  private final String name;
  private final DimensionType type;
  private final Integer start;
  private final Integer end;
  private final Integer step;
  private final Integer dataLag;
  private final List<String> values;
  private final String sql;
  private final String source;
  private final String path;

  private DimensionConfig(Builder builder) {
    this.name = builder.name;
    this.type = builder.type != null ? builder.type : DimensionType.LIST;
    this.start = builder.start;
    this.end = builder.end;
    this.step = builder.step != null ? builder.step : 1;
    this.dataLag = builder.dataLag != null ? builder.dataLag : 0;
    this.values = builder.values != null
        ? Collections.unmodifiableList(new ArrayList<String>(builder.values))
        : Collections.<String>emptyList();
    this.sql = builder.sql;
    this.source = builder.source;
    this.path = builder.path;
  }

  /**
   * Returns the dimension name (used as parameter key).
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the dimension type.
   */
  public DimensionType getType() {
    return type;
  }

  /**
   * Returns the start value for range dimensions.
   */
  public Integer getStart() {
    return start;
  }

  /**
   * Returns the end value for range dimensions.
   * For YEAR_RANGE type, null means current year.
   */
  public Integer getEnd() {
    return end;
  }

  /**
   * Returns the step value for range dimensions. Defaults to 1.
   */
  public Integer getStep() {
    return step;
  }

  /**
   * Returns the data lag in years for YEAR_RANGE dimensions.
   * When set, the end year is reduced by this amount (e.g., dataLag=1
   * means data ends at current year - 1). Defaults to 0.
   */
  public Integer getDataLag() {
    return dataLag;
  }

  /**
   * Returns the explicit value list for LIST type dimensions.
   */
  public List<String> getValues() {
    return values;
  }

  /**
   * Returns the SQL query for QUERY type dimensions.
   */
  public String getSql() {
    return sql;
  }

  /**
   * Returns the JSON resource file path for JSON_CATALOG type dimensions.
   * The path should be a classpath resource (e.g., "/worldbank/worldbank-countries.json").
   */
  public String getSource() {
    return source;
  }

  /**
   * Returns the JSONPath-like expression for extracting values from the JSON catalog.
   * Supports dot notation for nested objects and [*] for array iteration.
   *
   * <p>Examples:
   * <ul>
   *   <li>{@code countryGroups.G20.countries} - direct path to array</li>
   *   <li>{@code indicators[*].items[*].code} - iterate nested arrays</li>
   * </ul>
   */
  public String getPath() {
    return path;
  }

  /**
   * Creates a new builder for DimensionConfig.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a DimensionConfig from a YAML/JSON map.
   *
   * @param name Dimension name
   * @param map Configuration map
   * @return DimensionConfig instance
   */
  @SuppressWarnings("unchecked")
  public static DimensionConfig fromMap(String name, Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    Builder builder = builder().name(name);

    // Parse type
    Object typeObj = map.get("type");
    if (typeObj instanceof String) {
      builder.type(DimensionType.fromString((String) typeObj));
    }

    // Parse range parameters (supports numbers, env vars like {env:VAR}, or {env:VAR:default})
    Object startObj = map.get("start");
    if (startObj instanceof Number) {
      builder.start(((Number) startObj).intValue());
    } else if (startObj instanceof String) {
      Integer resolved = VariableResolver.resolveInteger((String) startObj);
      if (resolved != null) {
        builder.start(resolved);
      }
    }

    Object endObj = map.get("end");
    if (endObj instanceof Number) {
      builder.end(((Number) endObj).intValue());
    } else if (endObj instanceof String) {
      String endStr = (String) endObj;
      if ("current".equalsIgnoreCase(endStr)) {
        // For yearRange, "current" means resolve at runtime
        builder.end(null);
      } else {
        Integer resolved = VariableResolver.resolveInteger(endStr);
        if (resolved != null) {
          builder.end(resolved);
        }
      }
    }

    Object stepObj = map.get("step");
    if (stepObj instanceof Number) {
      builder.step(((Number) stepObj).intValue());
    } else if (stepObj instanceof String) {
      Integer resolved = VariableResolver.resolveInteger((String) stepObj);
      if (resolved != null) {
        builder.step(resolved);
      }
    }

    Object dataLagObj = map.get("dataLag");
    if (dataLagObj instanceof Number) {
      builder.dataLag(((Number) dataLagObj).intValue());
    } else if (dataLagObj instanceof String) {
      Integer resolved = VariableResolver.resolveInteger((String) dataLagObj);
      if (resolved != null) {
        builder.dataLag(resolved);
      }
    }

    // Parse list values
    Object valuesObj = map.get("values");
    if (valuesObj instanceof List) {
      List<String> stringValues = new ArrayList<String>();
      for (Object v : (List<?>) valuesObj) {
        stringValues.add(String.valueOf(v));
      }
      builder.values(stringValues);
    }

    // Parse SQL query
    Object sqlObj = map.get("sql");
    if (sqlObj instanceof String) {
      builder.sql((String) sqlObj);
    }

    // Parse JSON catalog source and path
    Object sourceObj = map.get("source");
    if (sourceObj instanceof String) {
      builder.source((String) sourceObj);
    }

    Object pathObj = map.get("path");
    if (pathObj instanceof String) {
      builder.path((String) pathObj);
    }

    return builder.build();
  }

  /**
   * Parses a map of dimension configurations.
   *
   * @param dimensionsMap Map of dimension name to configuration map
   * @return Map of dimension name to DimensionConfig
   */
  @SuppressWarnings("unchecked")
  public static Map<String, DimensionConfig> fromDimensionsMap(Map<String, Object> dimensionsMap) {
    if (dimensionsMap == null || dimensionsMap.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, DimensionConfig> result = new LinkedHashMap<String, DimensionConfig>();
    for (Map.Entry<String, Object> entry : dimensionsMap.entrySet()) {
      String name = entry.getKey();
      Object value = entry.getValue();

      DimensionConfig config = null;
      if (value instanceof Map) {
        // Standard map-based config (type, start, end, values, etc.)
        config = fromMap(name, (Map<String, Object>) value);
      } else if (value instanceof List) {
        // Shorthand: list values directly as dimension value (LIST type)
        List<String> stringValues = new ArrayList<String>();
        for (Object v : (List<?>) value) {
          stringValues.add(String.valueOf(v));
        }
        config = builder()
            .name(name)
            .type(DimensionType.LIST)
            .values(stringValues)
            .build();
      } else if (value instanceof String) {
        // Single value shorthand
        config = builder()
            .name(name)
            .type(DimensionType.LIST)
            .values(Collections.singletonList((String) value))
            .build();
      }

      if (config != null) {
        result.put(name, config);
      }
    }
    return result;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DimensionConfig{name='").append(name).append("'");
    sb.append(", type=").append(type);
    switch (type) {
      case RANGE:
      case YEAR_RANGE:
        sb.append(", start=").append(start);
        sb.append(", end=").append(end != null ? end : "current");
        sb.append(", step=").append(step);
        break;
      case LIST:
        sb.append(", values=").append(values);
        break;
      case QUERY:
        sb.append(", sql='").append(sql).append("'");
        break;
      case JSON_CATALOG:
        sb.append(", source='").append(source).append("'");
        sb.append(", path='").append(path).append("'");
        break;
      default:
        break;
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Builder for DimensionConfig.
   */
  public static class Builder {
    private String name;
    private DimensionType type;
    private Integer start;
    private Integer end;
    private Integer step;
    private Integer dataLag;
    private List<String> values;
    private String sql;
    private String source;
    private String path;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder type(DimensionType type) {
      this.type = type;
      return this;
    }

    public Builder start(Integer start) {
      this.start = start;
      return this;
    }

    public Builder end(Integer end) {
      this.end = end;
      return this;
    }

    public Builder step(Integer step) {
      this.step = step;
      return this;
    }

    public Builder dataLag(Integer dataLag) {
      this.dataLag = dataLag;
      return this;
    }

    public Builder values(List<String> values) {
      this.values = values;
      return this;
    }

    public Builder sql(String sql) {
      this.sql = sql;
      return this;
    }

    public Builder source(String source) {
      this.source = source;
      return this;
    }

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public DimensionConfig build() {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Dimension name is required");
      }
      return new DimensionConfig(this);
    }
  }
}
