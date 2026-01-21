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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;

/**
 * Utility methods for reading YAML files with proper anchor/alias resolution.
 *
 * <p>Jackson's YAML parser does not properly resolve YAML anchors (*anchor_name)
 * and aliases (&anchor_name) for simple values and collections. This utility uses
 * SnakeYAML directly to parse YAML with full anchor support, then converts to
 * Jackson JsonNode for compatibility with existing code.
 */
public class YamlUtils {
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  /**
   * Parse YAML/JSON file and return JsonNode with resolved YAML anchors.
   *
   * <p>For YAML files, uses SnakeYAML directly to resolve all anchor references,
   * then converts to Jackson JsonNode. For JSON files, uses Jackson directly.
   *
   * @param stream InputStream containing YAML or JSON data
   * @param resourceName Name of resource (used to determine format by extension)
   * @return JsonNode with all YAML anchors/aliases resolved
   * @throws IOException if file cannot be read or parsed
   */
  public static JsonNode parseYamlOrJson(InputStream stream, String resourceName) throws IOException {
    // For YAML files, use SnakeYAML directly to resolve anchors/aliases
    // Jackson YAML doesn't resolve anchors for simple values/collections
    if (resourceName.endsWith(".yaml") || resourceName.endsWith(".yml")) {
      // Configure SnakeYAML to allow more aliases (default is 50)
      org.yaml.snakeyaml.LoaderOptions loaderOptions = new org.yaml.snakeyaml.LoaderOptions();
      loaderOptions.setMaxAliasesForCollections(500); // Increase limit for large schemas
      org.yaml.snakeyaml.Yaml yaml = new org.yaml.snakeyaml.Yaml(loaderOptions);
      Object parsedYaml = yaml.load(stream);
      // Convert SnakeYAML output (with resolved anchors) to Jackson JsonNode
      return JSON_MAPPER.convertValue(parsedYaml, JsonNode.class);
    } else {
      // For JSON files, use Jackson directly
      return JSON_MAPPER.readTree(stream);
    }
  }
}
