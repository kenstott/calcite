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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * MCP tools for table profiling and statistics.
 *
 * <p>Provides statistical analysis of table data including row counts,
 * distinct values, min/max, null percentages, and more.
 */
public class ProfileTools {
  private final CalciteConnection connection;

  public ProfileTools(CalciteConnection connection) {
    this.connection = connection;
  }

  /**
   * Profile a table with aggregate statistics.
   *
   * @param schema Schema name
   * @param table Table name
   * @param columns List of columns to profile (empty = all columns)
   * @return JSON object with profiling statistics
   * @throws SQLException if profiling fails
   */
  public JsonObject profileTable(String schema, String table, List<String> columns)
      throws SQLException {

    String qualifiedTable = schema != null ? schema + "." + table : table;

    // If no columns specified, get all columns
    if (columns == null || columns.isEmpty()) {
      columns = getAllColumns(schema, table);
    }

    JsonObject profile = new JsonObject();
    profile.addProperty("schema", schema);
    profile.addProperty("table", table);

    // Build profile SQL
    StringBuilder sql = new StringBuilder("SELECT COUNT(*) as row_count");

    for (String column : columns) {
      sql.append(", COUNT(DISTINCT ").append(column).append(") as ")
          .append(column).append("_distinct");
      sql.append(", SUM(CASE WHEN ").append(column).append(" IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as ")
          .append(column).append("_null_pct");

      // Add min/max for numeric/date columns (best effort)
      sql.append(", CAST(MIN(").append(column).append(") AS VARCHAR) as ")
          .append(column).append("_min");
      sql.append(", CAST(MAX(").append(column).append(") AS VARCHAR) as ")
          .append(column).append("_max");
    }

    sql.append(" FROM ").append(qualifiedTable);

    // Execute profile query
    try (PreparedStatement stmt = connection.getConnection().prepareStatement(sql.toString());
         ResultSet rs = stmt.executeQuery()) {

      if (rs.next()) {
        profile.addProperty("row_count", rs.getLong("row_count"));

        JsonArray columnProfiles = new JsonArray();
        for (String column : columns) {
          JsonObject colProfile = new JsonObject();
          colProfile.addProperty("name", column);
          colProfile.addProperty("distinct_count", rs.getLong(column + "_distinct"));
          colProfile.addProperty("null_percentage", rs.getDouble(column + "_null_pct"));

          String minValue = rs.getString(column + "_min");
          String maxValue = rs.getString(column + "_max");
          if (minValue != null) {
            colProfile.addProperty("min", minValue);
          }
          if (maxValue != null) {
            colProfile.addProperty("max", maxValue);
          }

          columnProfiles.add(colProfile);
        }
        profile.add("columns", columnProfiles);
      }
    }

    return profile;
  }

  /**
   * Get all column names for a table.
   */
  private List<String> getAllColumns(String schema, String table) throws SQLException {
    List<String> columns = new ArrayList<>();
    DatabaseMetaData meta = connection.getMetadata();

    try (ResultSet rs = meta.getColumns(null, schema, table, null)) {
      while (rs.next()) {
        columns.add(rs.getString("COLUMN_NAME"));
      }
    }

    return columns;
  }
}
