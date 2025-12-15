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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Expands dimension definitions into concrete value combinations for batch processing.
 *
 * <p>DimensionIterator takes dimension configurations and produces a list of
 * parameter maps, where each map represents one combination of dimension values.
 * This is used by the ETL pipeline to iterate over batch combinations.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * // Define dimensions
 * Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
 * dimensions.put("year", DimensionConfig.builder()
 *     .name("year")
 *     .type(DimensionType.RANGE)
 *     .start(2020)
 *     .end(2024)
 *     .build());
 * dimensions.put("region", DimensionConfig.builder()
 *     .name("region")
 *     .type(DimensionType.LIST)
 *     .values(Arrays.asList("NORTH", "SOUTH", "EAST", "WEST"))
 *     .build());
 *
 * // Expand to combinations
 * DimensionIterator iterator = new DimensionIterator();
 * List<Map<String, String>> combinations = iterator.expand(dimensions);
 *
 * // Result: 20 combinations (5 years x 4 regions)
 * // [{year=2020, region=NORTH}, {year=2020, region=SOUTH}, ...]
 * }</pre>
 *
 * <h3>Dimension Types</h3>
 * <ul>
 *   <li>{@code RANGE} - Numeric sequence (start, end, step)</li>
 *   <li>{@code LIST} - Explicit value list</li>
 *   <li>{@code QUERY} - SQL query to fetch values</li>
 *   <li>{@code YEAR_RANGE} - Year range with current year support</li>
 * </ul>
 *
 * @see DimensionConfig
 * @see DimensionType
 * @see HiveParquetWriter
 */
public class DimensionIterator {

  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionIterator.class);

  private final Connection queryConnection;

  /**
   * Creates a DimensionIterator without SQL query support.
   * Query-type dimensions will throw an exception.
   */
  public DimensionIterator() {
    this.queryConnection = null;
  }

  /**
   * Creates a DimensionIterator with SQL query support.
   *
   * @param queryConnection JDBC connection for executing query-type dimensions
   */
  public DimensionIterator(Connection queryConnection) {
    this.queryConnection = queryConnection;
  }

  /**
   * Expands dimension configurations into all value combinations.
   *
   * <p>This method computes the Cartesian product of all dimension values.
   * For example, if dimension A has values [1, 2] and dimension B has values
   * [X, Y, Z], the result will be 6 combinations.
   *
   * @param dimensions Map of dimension name to configuration
   * @return List of parameter maps, one per combination
   */
  public List<Map<String, String>> expand(Map<String, DimensionConfig> dimensions) {
    if (dimensions == null || dimensions.isEmpty()) {
      LOGGER.debug("No dimensions to expand, returning single empty combination");
      return Collections.singletonList(Collections.<String, String>emptyMap());
    }

    // Resolve each dimension to its list of values
    List<String> dimensionNames = new ArrayList<String>();
    List<List<String>> dimensionValues = new ArrayList<List<String>>();

    for (Map.Entry<String, DimensionConfig> entry : dimensions.entrySet()) {
      String name = entry.getKey();
      DimensionConfig config = entry.getValue();

      List<String> values = resolveDimension(config);
      if (values.isEmpty()) {
        LOGGER.warn("Dimension '{}' resolved to empty values, skipping", name);
        continue;
      }

      dimensionNames.add(name);
      dimensionValues.add(values);
      LOGGER.debug("Dimension '{}' resolved to {} values: {}",
          name, values.size(), truncateForLog(values));
    }

    if (dimensionNames.isEmpty()) {
      LOGGER.debug("All dimensions resolved to empty, returning single empty combination");
      return Collections.singletonList(Collections.<String, String>emptyMap());
    }

    // Compute Cartesian product
    List<Map<String, String>> combinations = cartesianProduct(dimensionNames, dimensionValues);
    LOGGER.info("Expanded {} dimensions into {} combinations",
        dimensionNames.size(), combinations.size());

    return combinations;
  }

  /**
   * Expands a single dimension configuration to its list of values.
   *
   * @param config Dimension configuration
   * @return List of string values
   */
  public List<String> resolveDimension(DimensionConfig config) {
    switch (config.getType()) {
      case RANGE:
        return resolveRange(config);
      case LIST:
        return config.getValues();
      case QUERY:
        return resolveQuery(config);
      case YEAR_RANGE:
        return resolveYearRange(config);
      default:
        LOGGER.warn("Unknown dimension type '{}' for '{}', using empty list",
            config.getType(), config.getName());
        return Collections.emptyList();
    }
  }

  /**
   * Resolves a RANGE type dimension.
   */
  private List<String> resolveRange(DimensionConfig config) {
    Integer start = config.getStart();
    Integer end = config.getEnd();
    Integer step = config.getStep();

    if (start == null || end == null) {
      LOGGER.warn("Range dimension '{}' missing start or end", config.getName());
      return Collections.emptyList();
    }

    if (step == null || step == 0) {
      step = 1;
    }

    List<String> values = new ArrayList<String>();
    if (step > 0) {
      for (int i = start; i <= end; i += step) {
        values.add(String.valueOf(i));
      }
    } else {
      for (int i = start; i >= end; i += step) {
        values.add(String.valueOf(i));
      }
    }

    return values;
  }

  /**
   * Resolves a YEAR_RANGE type dimension.
   * Supports "current" as the end value, resolving to current year.
   */
  private List<String> resolveYearRange(DimensionConfig config) {
    Integer start = config.getStart();
    Integer end = config.getEnd();
    Integer step = config.getStep();

    if (start == null) {
      LOGGER.warn("Year range dimension '{}' missing start", config.getName());
      return Collections.emptyList();
    }

    // Resolve "current" year
    if (end == null) {
      end = Calendar.getInstance().get(Calendar.YEAR);
      LOGGER.debug("Year range dimension '{}' end resolved to current year: {}",
          config.getName(), end);
    }

    if (step == null || step == 0) {
      step = 1;
    }

    List<String> values = new ArrayList<String>();
    for (int year = start; year <= end; year += step) {
      values.add(String.valueOf(year));
    }

    return values;
  }

  /**
   * Resolves a QUERY type dimension by executing SQL.
   */
  private List<String> resolveQuery(DimensionConfig config) {
    if (queryConnection == null) {
      throw new IllegalStateException(
          "Query dimension '" + config.getName() + "' requires a database connection");
    }

    String sql = config.getSql();
    if (sql == null || sql.isEmpty()) {
      LOGGER.warn("Query dimension '{}' has no SQL", config.getName());
      return Collections.emptyList();
    }

    List<String> values = new ArrayList<String>();
    try (Statement stmt = queryConnection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String value = rs.getString(1);
        if (value != null) {
          values.add(value);
        }
      }
      LOGGER.debug("Query dimension '{}' returned {} values", config.getName(), values.size());
    } catch (SQLException e) {
      LOGGER.error("Failed to execute query dimension '{}': {}",
          config.getName(), e.getMessage());
      throw new RuntimeException(
          "Failed to resolve query dimension '" + config.getName() + "'", e);
    }

    return values;
  }

  /**
   * Computes the Cartesian product of dimension values.
   *
   * @param names Dimension names (for map keys)
   * @param values List of value lists (one per dimension)
   * @return List of maps representing all combinations
   */
  private List<Map<String, String>> cartesianProduct(
      List<String> names, List<List<String>> values) {

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();

    // Start with a single empty combination
    result.add(new LinkedHashMap<String, String>());

    // For each dimension, expand existing combinations
    for (int i = 0; i < names.size(); i++) {
      String name = names.get(i);
      List<String> dimValues = values.get(i);

      List<Map<String, String>> newResult = new ArrayList<Map<String, String>>();

      for (Map<String, String> existing : result) {
        for (String value : dimValues) {
          Map<String, String> newCombination = new LinkedHashMap<String, String>(existing);
          newCombination.put(name, value);
          newResult.add(newCombination);
        }
      }

      result = newResult;
    }

    return result;
  }

  /**
   * Truncates a list for logging purposes.
   */
  private String truncateForLog(List<String> values) {
    if (values.size() <= 5) {
      return values.toString();
    }
    List<String> preview = values.subList(0, 5);
    return preview.toString() + "... (" + values.size() + " total)";
  }

  /**
   * Creates a dimension iterator without query support.
   */
  public static DimensionIterator create() {
    return new DimensionIterator();
  }

  /**
   * Creates a dimension iterator with query support.
   *
   * @param queryConnection JDBC connection for SQL queries
   */
  public static DimensionIterator create(Connection queryConnection) {
    return new DimensionIterator(queryConnection);
  }
}
