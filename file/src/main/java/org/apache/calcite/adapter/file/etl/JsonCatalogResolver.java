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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Resolves dimension values from JSON catalog files.
 *
 * <p>Supports loading JSON resources from the classpath and extracting
 * values using simple path expressions.
 *
 * <h3>Path Expression Syntax</h3>
 * <ul>
 *   <li>{@code field.subfield} - Dot notation for nested objects</li>
 *   <li>{@code array[*]} - Iterate all elements in an array</li>
 *   <li>{@code array[*].field} - Extract field from each array element</li>
 * </ul>
 *
 * <h3>Examples</h3>
 * <pre>{@code
 * // JSON: {"countries": ["USA", "CAN", "MEX"]}
 * resolve(json, "countries") -> ["USA", "CAN", "MEX"]
 *
 * // JSON: {"groups": {"G7": {"countries": ["USA", "GBR"]}}}
 * resolve(json, "groups.G7.countries") -> ["USA", "GBR"]
 *
 * // JSON: {"items": [{"code": "A"}, {"code": "B"}]}
 * resolve(json, "items[*].code") -> ["A", "B"]
 * }</pre>
 */
public final class JsonCatalogResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonCatalogResolver.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonCatalogResolver() {
    // Utility class - no instances
  }

  /**
   * Resolves dimension values from a JSON catalog file.
   *
   * @param resourceClass Class to use for loading the resource
   * @param resourcePath Classpath resource path (e.g., "/worldbank/countries.json")
   * @param path Path expression to extract values
   * @return List of string values extracted from the JSON
   * @throws RuntimeException if the resource cannot be loaded or parsed
   */
  public static List<String> resolve(Class<?> resourceClass, String resourcePath, String path) {
    LOGGER.debug("Resolving JSON catalog: resource={}, path={}", resourcePath, path);

    InputStream is = findResource(resourceClass, resourcePath);
    if (is == null) {
      throw new RuntimeException("JSON catalog resource not found: " + resourcePath);
    }

    try {
      JsonNode root = MAPPER.readTree(is);
      List<String> values = extractValues(root, path);

      LOGGER.debug("Resolved {} values from {} using path '{}'",
          values.size(), resourcePath, path);
      return values;

    } catch (IOException e) {
      throw new RuntimeException("Failed to load JSON catalog: " + resourcePath, e);
    } finally {
      try {
        is.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close resource stream: {}", e.getMessage());
      }
    }
  }

  /**
   * Finds a resource using multiple classloader strategies.
   *
   * <p>Attempts to find the resource using:
   * <ol>
   *   <li>The provided resource class</li>
   *   <li>The thread context classloader</li>
   *   <li>The system classloader</li>
   * </ol>
   *
   * @param resourceClass Class to try first
   * @param resourcePath Resource path
   * @return InputStream for the resource, or null if not found
   */
  private static InputStream findResource(Class<?> resourceClass, String resourcePath) {
    // Try the provided class first
    InputStream is = resourceClass.getResourceAsStream(resourcePath);
    if (is != null) {
      LOGGER.debug("Found resource via provided class: {}", resourcePath);
      return is;
    }

    // Try the thread context classloader
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    if (contextClassLoader != null) {
      String cleanPath = resourcePath.startsWith("/") ? resourcePath.substring(1) : resourcePath;
      is = contextClassLoader.getResourceAsStream(cleanPath);
      if (is != null) {
        LOGGER.debug("Found resource via context classloader: {}", resourcePath);
        return is;
      }
    }

    // Try the system classloader
    String cleanPath = resourcePath.startsWith("/") ? resourcePath.substring(1) : resourcePath;
    is = ClassLoader.getSystemResourceAsStream(cleanPath);
    if (is != null) {
      LOGGER.debug("Found resource via system classloader: {}", resourcePath);
      return is;
    }

    LOGGER.warn("Resource not found via any classloader: {}", resourcePath);
    return null;
  }

  /**
   * Extracts values from a JSON node using a path expression.
   *
   * @param root Root JSON node
   * @param path Path expression
   * @return List of extracted string values
   */
  private static List<String> extractValues(JsonNode root, String path) {
    List<String> results = new ArrayList<String>();

    if (path == null || path.isEmpty()) {
      // No path - extract values from root if it's an array
      if (root.isArray()) {
        collectValues(root, results);
      }
      return results;
    }

    // Parse and navigate the path
    String[] segments = path.split("\\.");
    List<JsonNode> currentNodes = new ArrayList<JsonNode>();
    currentNodes.add(root);

    for (String segment : segments) {
      List<JsonNode> nextNodes = new ArrayList<JsonNode>();

      for (JsonNode node : currentNodes) {
        if (segment.contains("[*]")) {
          // Array iteration: field[*]
          String fieldName = segment.replace("[*]", "");
          JsonNode arrayNode = fieldName.isEmpty() ? node : node.get(fieldName);

          if (arrayNode != null && arrayNode.isArray()) {
            for (JsonNode element : arrayNode) {
              nextNodes.add(element);
            }
          }
        } else {
          // Simple field access
          JsonNode child = node.get(segment);
          if (child != null) {
            nextNodes.add(child);
          }
        }
      }

      currentNodes = nextNodes;
    }

    // Collect values from final nodes
    for (JsonNode node : currentNodes) {
      collectValues(node, results);
    }

    return results;
  }

  /**
   * Collects string values from a JSON node.
   * Handles arrays, objects (extracts values), and scalar values.
   */
  private static void collectValues(JsonNode node, List<String> results) {
    if (node.isArray()) {
      for (JsonNode element : node) {
        collectValues(element, results);
      }
    } else if (node.isObject()) {
      // For objects, try common value fields: code, value, id
      JsonNode codeNode = node.get("code");
      if (codeNode != null && codeNode.isTextual()) {
        results.add(codeNode.asText());
      } else {
        // Fall back to all text values
        Iterator<JsonNode> values = node.elements();
        while (values.hasNext()) {
          JsonNode value = values.next();
          if (value.isTextual()) {
            results.add(value.asText());
          }
        }
      }
    } else if (node.isTextual()) {
      results.add(node.asText());
    } else if (node.isNumber()) {
      results.add(node.asText());
    }
  }
}
