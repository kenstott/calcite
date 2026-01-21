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
package org.apache.calcite.adapter.file.format.json;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility to flatten nested JSON structures.
 * Objects are flattened using configurable separator, arrays are converted to delimited strings.
 */
public class JsonFlattener {
  private final String delimiter;
  private final int maxDepth;
  private final String nullValue;
  private final String separator;

  public JsonFlattener() {
    this(",", 3, "", "__");
  }

  public JsonFlattener(String delimiter, int maxDepth, String nullValue) {
    this(delimiter, maxDepth, nullValue, "__");
  }

  public JsonFlattener(String delimiter, int maxDepth, String nullValue, String separator) {
    this.delimiter = delimiter;
    this.maxDepth = maxDepth;
    this.nullValue = nullValue;
    this.separator = separator;
  }

  /**
   * Flattens a nested map structure.
   *
   * @param input The map to flatten
   * @return A new map with flattened keys
   */
  public Map<String, Object> flatten(Map<String, Object> input) {
    Map<String, Object> output = new LinkedHashMap<>();
    flattenObject("", input, output, 0);
    return output;
  }

  private void flattenObject(String prefix, Map<String, Object> obj,
                            Map<String, Object> output, int depth) {
    if (depth > maxDepth) {
      // If we've reached max depth, store the object as JSON string
      if (!prefix.isEmpty()) {
        output.put(prefix, obj.toString());
      }
      return;
    }

    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      String key = prefix.isEmpty() ? entry.getKey() : prefix + separator + entry.getKey();
      Object value = entry.getValue();

      if (value == null) {
        output.put(key, null);
      } else if (value instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> mapValue = (Map<String, Object>) value;
        if (mapValue.isEmpty()) {
          // Skip empty objects
          continue;
        }
        flattenObject(key, mapValue, output, depth + 1);
      } else if (value instanceof List) {
        String flattened = flattenArray((List<?>) value);
        if (flattened != null) {
          output.put(key, flattened);
        }
      } else {
        output.put(key, value);
      }
    }
  }

  private String flattenArray(List<?> array) {
    if (array.isEmpty()) {
      return "";
    }

    // Check if it's an array of objects - don't flatten those, but arrays of simple values should be flattened
    if (array.stream().anyMatch(item -> item instanceof Map)) {
      return null; // Signal to skip this field
    }

    return array.stream()
        .map(v -> v == null ? nullValue : escapeValue(v.toString()))
        .collect(Collectors.joining(delimiter));
  }

  private String escapeValue(String value) {
    // If the value contains our delimiter, wrap it in quotes
    if (value.contains(delimiter)) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }
}
