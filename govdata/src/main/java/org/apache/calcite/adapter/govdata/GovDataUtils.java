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
package org.apache.calcite.adapter.govdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods for GovData schema factories.
 *
 * <p>Used by factories not yet migrated to FileSchemaBuilder (Census, SEC, Geo).
 */
public final class GovDataUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataUtils.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final Pattern ENV_VAR_PATTERN = Pattern.compile("\\$\\{([^}:]+)(?::([^}]*))?\\}");

  private GovDataUtils() {
  }

  /**
   * Get ObjectMapper for the schema file type.
   */
  public static ObjectMapper getMapperForSchema(String schemaResourceName) {
    return (schemaResourceName.endsWith(".yaml") || schemaResourceName.endsWith(".yml"))
        ? YAML_MAPPER : JSON_MAPPER;
  }

  /**
   * Get cache directory from operand.
   */
  public static String getCacheDir(Map<String, Object> operand) {
    if (operand != null) {
      String dir = (String) operand.get("cacheDirectory");
      if (dir != null && !dir.isEmpty()) {
        return resolveEnvVar(dir);
      }
    }
    return null;
  }

  /**
   * Get parquet/output directory from operand.
   */
  public static String getParquetDir(Map<String, Object> operand) {
    if (operand != null) {
      String dir = (String) operand.get("directory");
      if (dir != null && !dir.isEmpty()) {
        return resolveEnvVar(dir);
      }
    }
    return null;
  }

  /**
   * Get configured start year from operand.
   */
  public static Integer getStartYear(Map<String, Object> operand) {
    if (operand != null) {
      Object year = operand.get("startYear");
      if (year instanceof Integer) {
        return (Integer) year;
      }
    }
    String envYear = System.getenv("GOVDATA_START_YEAR");
    if (envYear != null) {
      try {
        return Integer.parseInt(envYear);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_START_YEAR: {}", envYear);
      }
    }
    return java.time.Year.now().getValue() - 5;
  }

  /**
   * Get configured end year from operand.
   */
  public static Integer getEndYear(Map<String, Object> operand) {
    if (operand != null) {
      Object year = operand.get("endYear");
      if (year instanceof Integer) {
        return (Integer) year;
      }
    }
    String envYear = System.getenv("GOVDATA_END_YEAR");
    if (envYear != null) {
      try {
        return Integer.parseInt(envYear);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_END_YEAR: {}", envYear);
      }
    }
    int currentYear = java.time.Year.now().getValue();
    int dataLag = 0;
    if (operand != null) {
      Object lagObj = operand.get("dataLagYears");
      if (lagObj instanceof Number) {
        dataLag = ((Number) lagObj).intValue();
      }
    }
    return currentYear - dataLag;
  }

  /**
   * Load table definitions from schema resource file.
   */
  public static List<Map<String, Object>> loadTableDefinitions(Class<?> clazz,
      String schemaResourceName) {
    try (InputStream is = clazz.getResourceAsStream(schemaResourceName)) {
      if (is == null) {
        throw new IllegalStateException("Could not find " + schemaResourceName);
      }
      com.fasterxml.jackson.databind.JsonNode schemaNode =
          YamlUtils.parseYamlOrJson(is, schemaResourceName);
      @SuppressWarnings("unchecked")
      Map<String, Object> schema = JSON_MAPPER.convertValue(schemaNode, Map.class);

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> partitionedTables =
          (List<Map<String, Object>>) schema.get("partitionedTables");
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> regularTables =
          (List<Map<String, Object>>) schema.get("tables");

      List<Map<String, Object>> allTables = new ArrayList<>();
      if (partitionedTables != null) {
        allTables.addAll(partitionedTables);
      }
      if (regularTables != null) {
        allTables.addAll(regularTables);
      }
      return allTables;
    } catch (IOException e) {
      throw new RuntimeException("Error loading " + schemaResourceName, e);
    }
  }

  /**
   * Load constraint definitions from schema resource file.
   */
  public static Map<String, Map<String, Object>> loadTableConstraints(Class<?> clazz,
      String schemaResourceName) {
    ObjectMapper mapper = getMapperForSchema(schemaResourceName);
    try (InputStream is = clazz.getResourceAsStream(schemaResourceName)) {
      if (is == null) {
        throw new IllegalStateException("Could not find " + schemaResourceName);
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> schema = mapper.readValue(is, Map.class);
      @SuppressWarnings("unchecked")
      Map<String, Map<String, Object>> constraints =
          (Map<String, Map<String, Object>>) schema.get("constraints");
      return constraints != null ? constraints : Collections.emptyMap();
    } catch (IOException e) {
      throw new RuntimeException("Error loading " + schemaResourceName, e);
    }
  }

  /**
   * Load schema comment from resource file.
   */
  public static String loadSchemaComment(Class<?> clazz, String schemaResourceName) {
    ObjectMapper mapper = getMapperForSchema(schemaResourceName);
    try (InputStream is = clazz.getResourceAsStream(schemaResourceName)) {
      if (is == null) {
        return null;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> schema = mapper.readValue(is, Map.class);
      return (String) schema.get("comment");
    } catch (IOException e) {
      LOGGER.warn("Error loading schema comment: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Load table comments from schema resource file.
   *
   * @param clazz Class to use for resource loading
   * @param schemaResourceName Schema file path (e.g., "/geo/geo-schema.json")
   * @return Map from table name to table comment
   */
  public static Map<String, String> loadTableComments(Class<?> clazz, String schemaResourceName) {
    List<Map<String, Object>> tables = loadTableDefinitions(clazz, schemaResourceName);
    Map<String, String> comments = new java.util.HashMap<>();
    for (Map<String, Object> table : tables) {
      String name = (String) table.get("name");
      String comment = (String) table.get("comment");
      if (name != null && comment != null) {
        comments.put(name, comment);
      }
    }
    return comments;
  }

  /**
   * Load column comments for a specific table from schema resource file.
   *
   * @param clazz Class to use for resource loading
   * @param schemaResourceName Schema file path (e.g., "/geo/geo-schema.json")
   * @param tableName Name of the table to get column comments for
   * @return Map from column name to column comment
   */
  @SuppressWarnings("unchecked")
  public static Map<String, String> loadColumnComments(Class<?> clazz,
      String schemaResourceName, String tableName) {
    List<Map<String, Object>> tables = loadTableDefinitions(clazz, schemaResourceName);
    for (Map<String, Object> table : tables) {
      String name = (String) table.get("name");
      if (tableName.equals(name)) {
        List<Map<String, Object>> columns = (List<Map<String, Object>>) table.get("columns");
        if (columns == null) {
          return Collections.emptyMap();
        }
        Map<String, String> comments = new java.util.HashMap<>();
        for (Map<String, Object> column : columns) {
          String colName = (String) column.get("name");
          String colComment = (String) column.get("comment");
          if (colName != null && colComment != null) {
            comments.put(colName, colComment);
          }
        }
        return comments;
      }
    }
    return Collections.emptyMap();
  }

  /**
   * Resolve environment variable placeholders (${VAR} or ${VAR:default}).
   */
  public static String resolveEnvVar(String value) {
    if (value == null || !value.contains("${")) {
      return value;
    }
    Matcher matcher = ENV_VAR_PATTERN.matcher(value);
    if (matcher.find()) {
      String varName = matcher.group(1);
      String defaultValue = matcher.group(2);
      String resolved = System.getenv(varName);
      if (resolved == null) {
        resolved = System.getProperty(varName);
      }
      if (resolved == null) {
        resolved = defaultValue;
      }
      return resolved;
    }
    return value;
  }
}
