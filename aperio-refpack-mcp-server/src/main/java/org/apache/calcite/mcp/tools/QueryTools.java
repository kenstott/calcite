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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * MCP tools for querying data.
 *
 * <p>Provides SQL query execution and table sampling capabilities.
 */
public class QueryTools {
  private final CalciteConnection connection;

  public QueryTools(CalciteConnection connection) {
    this.connection = connection;
  }

  /**
   * Execute a SQL query and return results.
   *
   * @param sql SQL query to execute
   * @param limit Maximum number of rows to return (default 100)
   * @return JSON object with columns and rows
   * @throws SQLException if query execution fails
   */
  public JsonObject queryData(String sql, int limit) throws SQLException {
    JsonObject result = new JsonObject();
    JsonArray columns = new JsonArray();
    JsonArray rows = new JsonArray();

    // Apply limit if not already present
    String query = sql;
    if (limit > 0 && !sql.toUpperCase().contains("LIMIT")) {
      query = sql + " LIMIT " + limit;
    }

    try (PreparedStatement stmt = connection.getConnection().prepareStatement(query);
         ResultSet rs = stmt.executeQuery()) {

      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();

      // Build column metadata
      for (int i = 1; i <= columnCount; i++) {
        JsonObject col = new JsonObject();
        col.addProperty("name", meta.getColumnName(i));
        col.addProperty("type", meta.getColumnTypeName(i));
        columns.add(col);
      }

      // Fetch rows
      int rowCount = 0;
      while (rs.next() && rowCount < limit) {
        JsonArray row = new JsonArray();
        for (int i = 1; i <= columnCount; i++) {
          Object value = rs.getObject(i);
          if (value == null) {
            row.add((String) null);
          } else {
            row.add(value.toString());
          }
        }
        rows.add(row);
        rowCount++;
      }
    }

    result.add("columns", columns);
    result.add("rows", rows);
    return result;
  }

  /**
   * Sample rows from a table.
   *
   * @param schema Schema name
   * @param table Table name
   * @param limit Number of rows to sample (default 10)
   * @return JSON object with columns and rows
   * @throws SQLException if query fails
   */
  public JsonObject sampleTable(String schema, String table, int limit) throws SQLException {
    String qualifiedTable = schema != null ? schema + "." + table : table;
    String sql = "SELECT * FROM " + qualifiedTable;
    return queryData(sql, limit);
  }
}
