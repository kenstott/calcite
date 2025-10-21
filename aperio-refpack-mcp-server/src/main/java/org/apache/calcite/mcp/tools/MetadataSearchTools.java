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
package org.apache.calcite.mcp.tools;

import org.apache.calcite.mcp.CalciteConnection;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * MCP tools for metadata search.
 *
 * <p>Provides semantic search capabilities across database metadata
 * to help LLMs discover relevant tables and columns.
 */
public class MetadataSearchTools {
  private final CalciteConnection connection;
  private final DiscoveryTools discoveryTools;

  public MetadataSearchTools(CalciteConnection connection) {
    this.connection = connection;
    this.discoveryTools = new DiscoveryTools(connection);
  }

  /**
   * Search all metadata (schemas, tables, columns) and return as JSON.
   *
   * <p>This tool returns ALL database metadata in a structured format
   * that the LLM can semantically search. The LLM uses this to find
   * relevant tables/columns based on natural language queries.
   *
   * <p>Example: User asks "Find tables about accounts payable"
   * → LLM calls search_metadata(), receives all metadata
   * → LLM matches against table/column names and comments
   * → LLM returns relevant tables
   *
   * @param query User's search query (informational - not used for filtering)
   * @return JSON object with complete metadata catalog
   * @throws SQLException if metadata access fails
   */
  public JsonObject searchMetadata(String query) throws SQLException {
    JsonObject result = new JsonObject();
    result.addProperty("query", query);

    JsonArray schemas = new JsonArray();
    DatabaseMetaData meta = connection.getMetadata();

    // Get all schemas
    try (ResultSet schemaRs = meta.getSchemas()) {
      while (schemaRs.next()) {
        String schemaName = schemaRs.getString("TABLE_SCHEM");

        JsonObject schema = new JsonObject();
        schema.addProperty("name", schemaName);

        // Get tables in this schema
        JsonArray tables = new JsonArray();
        String[] types = {"TABLE", "VIEW"};
        try (ResultSet tableRs = meta.getTables(null, schemaName, null, types)) {
          while (tableRs.next()) {
            JsonObject table = new JsonObject();
            String tableName = tableRs.getString("TABLE_NAME");
            table.addProperty("name", tableName);
            table.addProperty("type", tableRs.getString("TABLE_TYPE"));

            String tableComment = tableRs.getString("REMARKS");
            if (tableComment != null && !tableComment.isEmpty()) {
              table.addProperty("comment", tableComment);
            }

            // Get columns for this table
            JsonArray columns = new JsonArray();
            try (ResultSet colRs = meta.getColumns(null, schemaName, tableName, null)) {
              while (colRs.next()) {
                JsonObject column = new JsonObject();
                column.addProperty("name", colRs.getString("COLUMN_NAME"));
                column.addProperty("type", colRs.getString("TYPE_NAME"));
                column.addProperty("nullable",
                    colRs.getInt("NULLABLE") == DatabaseMetaData.columnNullable);

                String colComment = colRs.getString("REMARKS");
                if (colComment != null && !colComment.isEmpty()) {
                  column.addProperty("comment", colComment);

                  // Include vector metadata flag if present
                  if (colComment.contains("[VECTOR ")) {
                    column.addProperty("has_vector_metadata", true);
                  }
                }

                columns.add(column);
              }
            }

            table.add("columns", columns);
            tables.add(table);
          }
        }

        schema.add("tables", tables);
        schemas.add(schema);
      }
    }

    result.add("schemas", schemas);
    return result;
  }
}
