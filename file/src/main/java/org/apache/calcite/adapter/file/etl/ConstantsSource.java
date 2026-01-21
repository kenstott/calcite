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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Data source that reads from YAML constants files.
 *
 * <p>This source type is used for static reference data that is defined
 * in YAML configuration files rather than fetched from APIs.
 *
 * <h3>Configuration</h3>
 * <pre>{@code
 * source:
 *   type: constants
 *   file: "/bls/bls-constants.yaml"   # Classpath resource
 *   path: "naicsSupersectors"          # Key path in YAML
 *   keyColumn: "supersector_code"      # Column name for map keys
 *   valueColumn: "supersector_name"    # Column name for map values
 * }</pre>
 *
 * <h3>Example YAML</h3>
 * <pre>{@code
 * naicsSupersectors:
 *   "00000000": Total Nonfarm
 *   "05000000": Total Private
 * }</pre>
 *
 * <p>This would produce rows:
 * <pre>
 * {supersector_code: "00000000", supersector_name: "Total Nonfarm"}
 * {supersector_code: "05000000", supersector_name: "Total Private"}
 * </pre>
 *
 * @see DataSource
 */
public class ConstantsSource implements DataSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConstantsSource.class);
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  private final ConstantsSourceConfig config;

  /**
   * Creates a ConstantsSource from configuration.
   *
   * @param config Source configuration
   */
  public ConstantsSource(ConstantsSourceConfig config) {
    this.config = config;
  }

  /**
   * Creates a ConstantsSource from a configuration map.
   *
   * @param configMap Configuration map from YAML
   * @return ConstantsSource instance
   */
  public static ConstantsSource fromMap(Map<String, Object> configMap) {
    return new ConstantsSource(ConstantsSourceConfig.fromMap(configMap));
  }

  @Override
  public Iterator<Map<String, Object>> fetch(Map<String, String> variables) throws IOException {
    String resourcePath = config.getFile();
    String keyPath = config.getPath();
    String keyColumn = config.getKeyColumn();
    String valueColumn = config.getValueColumn();

    LOGGER.info("Loading constants from resource: {}, path: {}", resourcePath, keyPath);

    // Load YAML from classpath
    InputStream inputStream = getClass().getResourceAsStream(resourcePath);
    if (inputStream == null) {
      throw new IOException("Resource not found: " + resourcePath);
    }

    try {
      // Parse YAML
      @SuppressWarnings("unchecked")
      Map<String, Object> yamlData = YAML_MAPPER.readValue(inputStream, Map.class);

      // Navigate to the specified path
      Object data = navigateToPath(yamlData, keyPath);
      if (data == null) {
        throw new IOException("Path not found in YAML: " + keyPath);
      }

      // Convert to list of rows
      List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();

      if (data instanceof Map) {
        // Map: each entry becomes a row with key/value columns
        @SuppressWarnings("unchecked")
        Map<String, Object> mapData = (Map<String, Object>) data;
        for (Map.Entry<String, Object> entry : mapData.entrySet()) {
          Map<String, Object> row = new HashMap<String, Object>();
          row.put(keyColumn, entry.getKey());
          row.put(valueColumn, String.valueOf(entry.getValue()));
          rows.add(row);
        }
        LOGGER.info("Loaded {} rows from constants map", rows.size());
      } else if (data instanceof List) {
        // List: each item is already a row (or map)
        @SuppressWarnings("unchecked")
        List<Object> listData = (List<Object>) data;
        for (Object item : listData) {
          if (item instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> rowData = (Map<String, Object>) item;
            Map<String, Object> row = new HashMap<String, Object>();
            row.putAll(rowData);
            rows.add(row);
          } else {
            // Scalar value in list - use as single column
            Map<String, Object> row = new HashMap<String, Object>();
            row.put(valueColumn, item);
            rows.add(row);
          }
        }
        LOGGER.info("Loaded {} rows from constants list", rows.size());
      } else {
        throw new IOException("Unsupported data type at path " + keyPath
            + ": " + data.getClass().getSimpleName());
      }

      return rows.iterator();
    } finally {
      inputStream.close();
    }
  }

  /**
   * Navigates to a nested path in a map.
   *
   * @param data Root map
   * @param path Dot-separated path (e.g., "foo.bar.baz")
   * @return Value at path, or null if not found
   */
  private Object navigateToPath(Map<String, Object> data, String path) {
    if (path == null || path.isEmpty()) {
      return data;
    }

    String[] parts = path.split("\\.");
    Object current = data;

    for (String part : parts) {
      if (current instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> currentMap = (Map<String, Object>) current;
        current = currentMap.get(part);
        if (current == null) {
          return null;
        }
      } else {
        return null;
      }
    }

    return current;
  }

  @Override
  public String getType() {
    return "constants";
  }

  @Override
  public long estimateRowCount() {
    // Could pre-load to estimate, but return -1 for simplicity
    return -1;
  }

  /**
   * Configuration for ConstantsSource.
   */
  public static class ConstantsSourceConfig {
    private final String file;
    private final String path;
    private final String keyColumn;
    private final String valueColumn;

    public ConstantsSourceConfig(String file, String path, String keyColumn, String valueColumn) {
      this.file = file;
      this.path = path;
      this.keyColumn = keyColumn != null ? keyColumn : "key";
      this.valueColumn = valueColumn != null ? valueColumn : "value";
    }

    public String getFile() {
      return file;
    }

    public String getPath() {
      return path;
    }

    public String getKeyColumn() {
      return keyColumn;
    }

    public String getValueColumn() {
      return valueColumn;
    }

    /**
     * Creates config from a YAML/JSON map.
     */
    public static ConstantsSourceConfig fromMap(Map<String, Object> map) {
      return new ConstantsSourceConfig(
          (String) map.get("file"),
          (String) map.get("path"),
          (String) map.get("keyColumn"),
          (String) map.get("valueColumn"));
    }
  }
}
