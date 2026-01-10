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

import org.apache.calcite.adapter.file.storage.StorageProvider;

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
  private final DimensionResolver dimensionResolver;
  private final StorageProvider storageProvider;

  /**
   * Creates a DimensionIterator without SQL query or custom resolver support.
   * Query-type and custom-type dimensions will throw an exception.
   */
  public DimensionIterator() {
    this.queryConnection = null;
    this.dimensionResolver = null;
    this.storageProvider = null;
  }

  /**
   * Creates a DimensionIterator with SQL query support.
   *
   * @param queryConnection JDBC connection for executing query-type dimensions
   */
  public DimensionIterator(Connection queryConnection) {
    this.queryConnection = queryConnection;
    this.dimensionResolver = null;
    this.storageProvider = null;
  }

  /**
   * Creates a DimensionIterator with custom dimension resolver and storage provider.
   *
   * @param dimensionResolver Custom resolver for CUSTOM type dimensions
   * @param storageProvider Storage provider for file access (local or S3)
   */
  public DimensionIterator(DimensionResolver dimensionResolver, StorageProvider storageProvider) {
    this.queryConnection = null;
    this.dimensionResolver = dimensionResolver;
    this.storageProvider = storageProvider;
  }

  /**
   * Creates a DimensionIterator with SQL query, custom resolver, and storage provider.
   *
   * @param queryConnection JDBC connection for executing query-type dimensions
   * @param dimensionResolver Custom resolver for CUSTOM type dimensions
   * @param storageProvider Storage provider for file access (local or S3)
   */
  public DimensionIterator(Connection queryConnection, DimensionResolver dimensionResolver,
      StorageProvider storageProvider) {
    this.queryConnection = queryConnection;
    this.dimensionResolver = dimensionResolver;
    this.storageProvider = storageProvider;
  }

  /**
   * Expands dimension configurations into all value combinations.
   *
   * <p>This method computes the Cartesian product of all dimension values.
   * For example, if dimension A has values [1, 2] and dimension B has values
   * [X, Y, Z], the result will be 6 combinations.
   *
   * <p>For CUSTOM type dimensions, the resolver is called with context from
   * previously-resolved dimensions, enabling dependent dimension patterns.
   *
   * @param dimensions Map of dimension name to configuration
   * @return List of parameter maps, one per combination
   */
  public List<Map<String, String>> expand(Map<String, DimensionConfig> dimensions) {
    if (dimensions == null || dimensions.isEmpty()) {
      LOGGER.debug("No dimensions to expand, returning single empty combination");
      return Collections.singletonList(Collections.<String, String>emptyMap());
    }

    // Check if any dimensions are CUSTOM (context-dependent)
    boolean hasCustomDimensions = false;
    for (DimensionConfig config : dimensions.values()) {
      if (config.getType() == DimensionType.CUSTOM) {
        hasCustomDimensions = true;
        break;
      }
    }

    // Use context-aware expansion if there are CUSTOM dimensions
    if (hasCustomDimensions && dimensionResolver != null) {
      return expandWithContext(dimensions);
    }

    // Standard expansion for non-custom dimensions
    return expandStandard(dimensions);
  }

  /**
   * Standard expansion without context (original behavior).
   */
  private List<Map<String, String>> expandStandard(Map<String, DimensionConfig> dimensions) {
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
   * Context-aware expansion for CUSTOM dimensions.
   *
   * <p>Iteratively expands dimensions, passing context to the resolver for
   * CUSTOM dimensions so they can return values based on previously-selected
   * dimension values.
   */
  private List<Map<String, String>> expandWithContext(Map<String, DimensionConfig> dimensions) {
    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    result.add(new LinkedHashMap<String, String>());

    for (Map.Entry<String, DimensionConfig> entry : dimensions.entrySet()) {
      String name = entry.getKey();
      DimensionConfig config = entry.getValue();

      List<Map<String, String>> newResult = new ArrayList<Map<String, String>>();

      for (Map<String, String> existing : result) {
        // Resolve dimension values with context from existing combination
        List<String> values;
        if (config.getType() == DimensionType.CUSTOM) {
          values = resolveCustomWithContext(config, existing);
        } else {
          values = resolveDimension(config);
        }

        if (values.isEmpty()) {
          LOGGER.debug("Dimension '{}' resolved to empty for context {}, skipping",
              name, existing);
          continue;
        }

        // Expand this combination with each dimension value
        for (String value : values) {
          Map<String, String> newCombination = new LinkedHashMap<String, String>(existing);
          newCombination.put(name, value);
          newResult.add(newCombination);
        }
      }

      if (newResult.isEmpty()) {
        LOGGER.warn("Dimension '{}' resulted in no valid combinations", name);
        return Collections.emptyList();
      }

      result = newResult;
      LOGGER.debug("After dimension '{}': {} combinations", name, result.size());
    }

    LOGGER.info("Expanded {} dimensions into {} combinations (context-aware)",
        dimensions.size(), result.size());
    return result;
  }

  /**
   * Resolves a CUSTOM dimension with context from other dimensions.
   */
  private List<String> resolveCustomWithContext(DimensionConfig config,
      Map<String, String> context) {
    try {
      List<String> values = dimensionResolver.resolve(
          config.getName(), config, context, storageProvider);
      if (values == null) {
        LOGGER.warn("DimensionResolver returned null for '{}' with context {}, using empty list",
            config.getName(), context);
        return Collections.emptyList();
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Custom dimension '{}' resolved to {} values for context {}",
            config.getName(), values.size(), context);
      }
      return values;
    } catch (RuntimeException e) {
      LOGGER.error("DimensionResolver failed for '{}' with context {}: {}",
          config.getName(), context, e.getMessage());
      throw new RuntimeException(
          "Failed to resolve custom dimension '" + config.getName() + "'", e);
    }
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
      case CUSTOM:
        return resolveCustom(config);
      case JSON_CATALOG:
        return resolveJsonCatalog(config);
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
   * Supports dataLag to exclude recent years (e.g., dataLag=1 means data through current-1).
   *
   * <p>The dataLag is always computed from the current year, then the effective end
   * is the minimum of the lag-adjusted year and the configured end year.
   */
  private List<String> resolveYearRange(DimensionConfig config) {
    Integer start = config.getStart();
    Integer end = config.getEnd();
    Integer step = config.getStep();
    Integer dataLag = config.getDataLag();

    if (start == null) {
      LOGGER.warn("Year range dimension '{}' missing start", config.getName());
      return Collections.emptyList();
    }

    int currentYear = Calendar.getInstance().get(Calendar.YEAR);

    // Compute lag year from current year (always from current, not from configured end)
    int lagYear = currentYear;
    if (dataLag != null && dataLag > 0) {
      lagYear = currentYear - dataLag;
      LOGGER.debug("Year range dimension '{}' lag year computed as {} (currentYear={}, dataLag={})",
          config.getName(), lagYear, currentYear, dataLag);
    }

    // Apply release month adjustment to lag year
    // This handles cases where data for year Y is released mid-year in year Y+1
    Integer releaseMonth = config.getReleaseMonth();
    if (releaseMonth != null && releaseMonth >= 1 && releaseMonth <= 12) {
      int currentMonth = Calendar.getInstance().get(Calendar.MONTH) + 1; // 1-based
      if (currentMonth < releaseMonth) {
        lagYear = lagYear - 1;
        LOGGER.debug("Year range dimension '{}' adjusted lag year to {} (before releaseMonth={})",
            config.getName(), lagYear, releaseMonth);
      }
    }

    // Resolve configured end year (null means "current")
    int configuredEnd = (end != null) ? end : currentYear;
    LOGGER.debug("Year range dimension '{}' configured end: {}", config.getName(), configuredEnd);

    // Effective end is the minimum of lag year and configured end
    int effectiveEnd = Math.min(lagYear, configuredEnd);
    LOGGER.debug("Year range dimension '{}' effective end: {} (min of lagYear={}, configuredEnd={})",
        config.getName(), effectiveEnd, lagYear, configuredEnd);

    if (step == null || step == 0) {
      step = 1;
    }

    List<String> values = new ArrayList<String>();
    for (int year = start; year <= effectiveEnd; year += step) {
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
   * Resolves a CUSTOM type dimension by calling the DimensionResolver.
   *
   * <p>Custom dimensions allow adapters to provide dynamic dimension values
   * from external sources like catalog APIs, databases, or computed values.
   *
   * <p>Note: This method is called from standard expansion without context.
   * For context-aware resolution, use {@link #resolveCustomWithContext}.
   *
   * @param config Dimension configuration
   * @return List of resolved values
   */
  private List<String> resolveCustom(DimensionConfig config) {
    if (dimensionResolver == null) {
      throw new IllegalStateException(
          "Custom dimension '" + config.getName() + "' requires a DimensionResolver. "
          + "Configure hooks.dimensionResolver in the schema.");
    }

    // Call with empty context (no prior dimensions available)
    return resolveCustomWithContext(config, Collections.<String, String>emptyMap());
  }

  /**
   * Resolves a JSON_CATALOG type dimension by loading values from a JSON resource file.
   *
   * <p>Uses {@link JsonCatalogResolver} to load and extract values from the
   * JSON resource specified by the dimension's source and path properties.
   *
   * @param config Dimension configuration with source and path
   * @return List of resolved values
   */
  private List<String> resolveJsonCatalog(DimensionConfig config) {
    String source = config.getSource();
    String path = config.getPath();

    if (source == null || source.isEmpty()) {
      LOGGER.warn("JSON catalog dimension '{}' has no source", config.getName());
      return Collections.emptyList();
    }

    try {
      List<String> values = JsonCatalogResolver.resolve(
          DimensionIterator.class, source, path);
      LOGGER.info("JSON catalog dimension '{}' loaded {} values from {}",
          config.getName(), values.size(), source);
      return values;
    } catch (RuntimeException e) {
      LOGGER.error("Failed to resolve JSON catalog dimension '{}': {}",
          config.getName(), e.getMessage());
      throw new RuntimeException(
          "Failed to resolve JSON catalog dimension '" + config.getName() + "'", e);
    }
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
