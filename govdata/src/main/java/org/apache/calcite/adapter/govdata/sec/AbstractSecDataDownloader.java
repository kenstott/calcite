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

            if (colName != null) {
              columns.add(
                  new org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn(
                  colName, colType, nullable, comment));
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
