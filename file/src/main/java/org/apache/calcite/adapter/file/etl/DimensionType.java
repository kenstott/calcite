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

/**
 * Types of dimension value expansion.
 *
 * <p>Dimensions define how batch processing iterates over parameter combinations.
 * Each type provides a different method of generating dimension values.
 *
 * @see DimensionConfig
 * @see DimensionIterator
 */
public enum DimensionType {
  /**
   * Numeric range with start, end, and optional step.
   *
   * <pre>{@code
   * year:
   *   type: range
   *   start: 2020
   *   end: 2024
   *   step: 1
   * }</pre>
   *
   * <p>Generates: [2020, 2021, 2022, 2023, 2024]
   */
  RANGE,

  /**
   * Explicit list of values.
   *
   * <pre>{@code
   * region:
   *   type: list
   *   values: [NORTH, SOUTH, EAST, WEST]
   * }</pre>
   */
  LIST,

  /**
   * SQL query that returns dimension values.
   *
   * <pre>{@code
   * region:
   *   type: query
   *   sql: "SELECT DISTINCT region FROM regions WHERE active = true"
   * }</pre>
   */
  QUERY,

  /**
   * Year range with current year support.
   *
   * <pre>{@code
   * year:
   *   type: yearRange
   *   start: 2020
   *   end: current  # resolves to current year
   * }</pre>
   */
  YEAR_RANGE,

  /**
   * Custom dimension resolved by a {@link DimensionResolver} implementation.
   *
   * <p>Use this type when dimension values need to be loaded from:
   * <ul>
   *   <li>External catalog APIs (e.g., FRED series catalog)</li>
   *   <li>Dynamic runtime computation</li>
   *   <li>Complex business rules</li>
   * </ul>
   *
   * <pre>{@code
   * series_id:
   *   type: custom
   *   catalog: fred_series
   *   filters:
   *     popularity: 100  # minimum popularity score
   * }</pre>
   *
   * <p>The hooks.dimensionResolver class is responsible for resolving these values.
   *
   * @see DimensionResolver
   */
  CUSTOM,

  /**
   * JSON catalog dimension - loads values from a JSON resource file.
   *
   * <p>Use this type when dimension values are defined in a JSON file
   * that can be loaded from the classpath. Supports JSONPath-like expressions
   * to extract specific values from the JSON structure.
   *
   * <pre>{@code
   * country:
   *   type: json_catalog
   *   source: "/worldbank/worldbank-countries.json"
   *   path: "countryGroups.G20.countries"
   * }</pre>
   *
   * <p>The path supports dot notation for nested objects and [*] for array iteration:
   * <ul>
   *   <li>{@code countryGroups.G20.countries} - direct path to array</li>
   *   <li>{@code indicators[*].items[*].code} - iterate nested arrays</li>
   * </ul>
   */
  JSON_CATALOG;

  /**
   * Parses a dimension type from a string value.
   *
   * @param value String representation (case-insensitive)
   * @return DimensionType, defaulting to LIST if not recognized
   */
  public static DimensionType fromString(String value) {
    if (value == null || value.isEmpty()) {
      return LIST;
    }
    switch (value.toLowerCase()) {
      case "range":
        return RANGE;
      case "list":
        return LIST;
      case "query":
        return QUERY;
      case "yearrange":
      case "year_range":
        return YEAR_RANGE;
      case "custom":
      case "catalog":
        return CUSTOM;
      case "json_catalog":
      case "jsoncatalog":
        return JSON_CATALOG;
      default:
        return LIST;
    }
  }
}
