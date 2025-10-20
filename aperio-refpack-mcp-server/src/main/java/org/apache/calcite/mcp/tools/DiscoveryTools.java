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
import org.apache.calcite.mcp.metadata.VectorMetadata;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * MCP tools for schema discovery.
 *
 * <p>Provides tools to explore database schemas, tables, and columns
 * through JDBC metadata.
 */
public class DiscoveryTools {
  private final CalciteConnection connection;

  public DiscoveryTools(CalciteConnection connection) {
    this.connection = connection;
  }

  /**
   * List all schemas in the database.
   *
   * @return JSON array of schema objects with name and comment
   * @throws SQLException if metadata access fails
   */
  public JsonArray listSchemas() throws SQLException {
    JsonArray schemas = new JsonArray();
    DatabaseMetaData meta = connection.getMetadata();

    try (ResultSet rs = meta.getSchemas()) {
      while (rs.next()) {
        JsonObject schema = new JsonObject();
        schema.addProperty("name", rs.getString("TABLE_SCHEM"));
        // JDBC doesn't provide schema comments directly
        schemas.add(schema);
      }
    }

    return schemas;
  }

  /**
   * List tables in a schema.
   *
   * @param schema Schema name (null for all schemas)
   * @param includeComments Whether to include table comments
   * @return JSON array of table objects
   * @throws SQLException if metadata access fails
   */
  public JsonArray listTables(String schema, boolean includeComments) throws SQLException {
    JsonArray tables = new JsonArray();
    DatabaseMetaData meta = connection.getMetadata();

    String[] types = {"TABLE", "VIEW"};
    try (ResultSet rs = meta.getTables(null, schema, null, types)) {
      while (rs.next()) {
        JsonObject table = new JsonObject();
        table.addProperty("schema", rs.getString("TABLE_SCHEM"));
        table.addProperty("name", rs.getString("TABLE_NAME"));
        table.addProperty("type", rs.getString("TABLE_TYPE"));

        if (includeComments) {
          String remarks = rs.getString("REMARKS");
          if (remarks != null && !remarks.isEmpty()) {
            table.addProperty("comment", remarks);
          }
        }

        tables.add(table);
      }
    }

    return tables;
  }

  /**
   * Describe table structure with column details.
   *
   * @param schema Schema name
   * @param table Table name
   * @param includeComments Whether to include column comments
   * @return JSON array of column objects
   * @throws SQLException if metadata access fails
   */
  public JsonArray describeTable(String schema, String table, boolean includeComments)
      throws SQLException {
    JsonArray columns = new JsonArray();
    DatabaseMetaData meta = connection.getMetadata();

    try (ResultSet rs = meta.getColumns(null, schema, table, null)) {
      while (rs.next()) {
        JsonObject column = new JsonObject();
        String columnName = rs.getString("COLUMN_NAME");
        column.addProperty("name", columnName);
        column.addProperty("type", rs.getString("TYPE_NAME"));
        column.addProperty("nullable", rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable);

        if (includeComments) {
          String remarks = rs.getString("REMARKS");
          if (remarks != null && !remarks.isEmpty()) {
            column.addProperty("comment", remarks);

            // Parse vector metadata if present
            VectorMetadata vm = VectorMetadata.parseFromComment(columnName, remarks);
            if (vm != null) {
              JsonObject vectorMeta = new JsonObject();
              vectorMeta.addProperty("dimension", vm.getDimension());
              vectorMeta.addProperty("provider", vm.getProvider());
              vectorMeta.addProperty("model", vm.getModel());

              if (vm.getSourceTableColumn() != null) {
                vectorMeta.addProperty("source_table_col", vm.getSourceTableColumn());
                vectorMeta.addProperty("source_id_col", vm.getSourceIdColumnName());
                vectorMeta.addProperty("pattern", "MULTI_SOURCE");
              } else if (vm.getSourceTable() != null) {
                vectorMeta.addProperty("source_table", vm.getSourceTable());
                vectorMeta.addProperty("source_id_col", vm.getSourceIdColumn());
                vectorMeta.addProperty("pattern", "FK_LOGICAL");
              }

              column.add("vector_metadata", vectorMeta);
            }
          }
        }

        columns.add(column);
      }
    }

    return columns;
  }
}
