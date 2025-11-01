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

/**
 * Abstract base class for securities data downloaders.
 * Provides common infrastructure for metadata-driven schema generation.
 */
public abstract class AbstractSecDataDownloader {

  protected static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
      new com.fasterxml.jackson.databind.ObjectMapper();

  /**
   * Loads column metadata for a table from sec-schema.json.
   *
   * @param tableName The name of the table to load column metadata for
   * @return List of table column definitions
   * @throws IllegalArgumentException if the table is not found or schema is invalid
   */
  protected static java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
      loadTableColumns(String tableName) {
    try {
      // Load sec-schema.json from resources
      java.io.InputStream schemaStream =
          AbstractSecDataDownloader.class.getResourceAsStream("/sec-schema.json");
      if (schemaStream == null) {
        throw new IllegalArgumentException(
            "sec-schema.json not found in resources");
      }

      // Parse JSON
      com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(schemaStream);

      // Find the table in the "partitionedTables" array
      if (!root.has("partitionedTables") || !root.get("partitionedTables").isArray()) {
        throw new IllegalArgumentException(
            "Invalid sec-schema.json: missing 'partitionedTables' array");
      }

      for (com.fasterxml.jackson.databind.JsonNode tableNode : root.get("partitionedTables")) {
        String name = tableNode.has("name") ? tableNode.get("name").asText() : null;
        if (tableName.equals(name)) {
          // Found the table - extract columns
          if (!tableNode.has("columns") || !tableNode.get("columns").isArray()) {
            throw new IllegalArgumentException(
                "Table '" + tableName + "' has no 'columns' array in sec-schema.json");
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
          "Table '" + tableName + "' not found in sec-schema.json");

    } catch (java.io.IOException e) {
      throw new IllegalArgumentException(
          "Failed to load column metadata for table '" + tableName + "': " + e.getMessage(), e);
    }
  }

}
