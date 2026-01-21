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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.govdata.YamlUtils;

/**
 * Abstract base class for securities data downloaders.
 * Provides common infrastructure for metadata-driven schema generation.
 */
public abstract class AbstractSecDataDownloader {

  private static final String SCHEMA_RESOURCE = "/sec/sec-schema.yaml";

  /**
   * Loads column metadata for a table from sec-schema.yaml.
   *
   * <p>Uses YamlUtils to parse the YAML file with full anchor/alias resolution,
   * allowing column templates like {@code *cik_column} to be expanded.
   *
   * @param tableName The name of the table to load column metadata for
   * @return List of table column definitions
   * @throws IllegalArgumentException if the table is not found or schema is invalid
   */
  protected static java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
      loadTableColumns(String tableName) {
    try {
      // Load sec-schema.yaml from resources
      java.io.InputStream schemaStream =
          AbstractSecDataDownloader.class.getResourceAsStream(SCHEMA_RESOURCE);
      if (schemaStream == null) {
        throw new IllegalArgumentException(
            SCHEMA_RESOURCE + " not found in resources");
      }

      // Parse YAML with anchor/alias resolution via YamlUtils
      com.fasterxml.jackson.databind.JsonNode root =
          YamlUtils.parseYamlOrJson(schemaStream, SCHEMA_RESOURCE);

      // Find the table in the "partitionedTables" array
      if (!root.has("partitionedTables") || !root.get("partitionedTables").isArray()) {
        throw new IllegalArgumentException(
            "Invalid " + SCHEMA_RESOURCE + ": missing 'partitionedTables' array");
      }

      for (com.fasterxml.jackson.databind.JsonNode tableNode : root.get("partitionedTables")) {
        String name = tableNode.has("name") ? tableNode.get("name").asText() : null;
        if (tableName.equals(name)) {
          // Found the table - extract columns
          if (!tableNode.has("columns") || !tableNode.get("columns").isArray()) {
            throw new IllegalArgumentException(
                "Table '" + tableName + "' has no 'columns' array in " + SCHEMA_RESOURCE);
          }

          java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
              columns = new java.util.ArrayList<>();

          for (com.fasterxml.jackson.databind.JsonNode colNode : tableNode.get("columns")) {
            String colName = colNode.has("name") ? colNode.get("name").asText() : null;
            String colType = colNode.has("type") ? colNode.get("type").asText() : "string";
            boolean nullable = colNode.has("nullable") && colNode.get("nullable").asBoolean();
            String comment = colNode.has("comment") ? colNode.get("comment").asText() : "";
            String expression = colNode.has("expression") ? colNode.get("expression").asText() : null;

            if (colName != null) {
              columns.add(
                  new org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn(
                  colName, colType, nullable, comment, expression));
            }
          }

          return columns;
        }
      }

      // Table not found
      throw new IllegalArgumentException(
          "Table '" + tableName + "' not found in " + SCHEMA_RESOURCE);

    } catch (java.io.IOException e) {
      throw new IllegalArgumentException(
          "Failed to load column metadata for table '" + tableName + "': " + e.getMessage(), e);
    }
  }

}
