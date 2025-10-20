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
import org.apache.calcite.mcp.metadata.PatternDetector;
import org.apache.calcite.mcp.metadata.VectorMetadata;
import org.apache.calcite.mcp.metadata.VectorPattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * MCP tools for vector similarity search.
 *
 * <p>Provides semantic search capabilities using vector embeddings
 * with pattern-aware query generation.
 */
public class VectorSearchTools {
  private final CalciteConnection connection;
  private final DiscoveryTools discoveryTools;

  public VectorSearchTools(CalciteConnection connection) {
    this.connection = connection;
    this.discoveryTools = new DiscoveryTools(connection);
  }

  /**
   * Perform semantic search using vector similarity.
   *
   * @param schema Schema name
   * @param table Table name containing vector column
   * @param queryText Text to search for
   * @param limit Maximum number of results (default 10)
   * @param threshold Similarity threshold 0-1 (default 0.7)
   * @param sourceTableFilter Filter by source table (Pattern 3 only)
   * @param includeSource Join to source table (Pattern 2a/2b only)
   * @return JSON array of matching rows with similarity scores
   * @throws SQLException if search fails
   */
  public JsonArray semanticSearch(String schema, String table, String queryText,
                                   int limit, double threshold,
                                   String sourceTableFilter, boolean includeSource)
      throws SQLException {

    // Discover vector column
    VectorMetadata vm = findVectorColumn(schema, table);
    if (vm == null) {
      throw new SQLException("No vector column found in table: " + table);
    }

    // Detect pattern
    VectorPattern pattern = PatternDetector.detectPattern(
        connection.getConnection(), schema, table, vm);

    // Generate pattern-aware SQL
    String sql = generateSearchSql(schema, table, vm, pattern,
        queryText, limit, threshold, sourceTableFilter, includeSource);

    // Execute query
    return executeVectorSearch(sql, vm);
  }

  /**
   * List available source tables for multi-source vector tables (Pattern 3).
   *
   * @param schema Schema name
   * @param table Table name
   * @return JSON array of source tables with counts
   * @throws SQLException if query fails
   */
  public JsonArray listVectorSources(String schema, String table) throws SQLException {
    VectorMetadata vm = findVectorColumn(schema, table);
    if (vm == null || vm.getSourceTableColumn() == null) {
      throw new SQLException("Table is not a multi-source vector table: " + table);
    }

    String qualifiedTable = schema != null ? schema + "." + table : table;
    String sql = String.format(
        "SELECT %s as source_table, COUNT(*) as vector_count " +
        "FROM %s " +
        "GROUP BY %s " +
        "ORDER BY vector_count DESC",
        vm.getSourceTableColumn(), qualifiedTable, vm.getSourceTableColumn());

    JsonArray sources = new JsonArray();
    try (PreparedStatement stmt = connection.getConnection().prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      while (rs.next()) {
        JsonObject source = new JsonObject();
        source.addProperty("source_table", rs.getString("source_table"));
        source.addProperty("vector_count", rs.getInt("vector_count"));
        sources.add(source);
      }
    }

    return sources;
  }

  /**
   * Find vector column in table by parsing column comments.
   */
  private VectorMetadata findVectorColumn(String schema, String table) throws SQLException {
    JsonArray columns = discoveryTools.describeTable(schema, table, true);

    for (int i = 0; i < columns.size(); i++) {
      JsonObject col = columns.get(i).getAsJsonObject();
      if (col.has("vector_metadata")) {
        String columnName = col.get("name").getAsString();
        String comment = col.get("comment").getAsString();
        return VectorMetadata.parseFromComment(columnName, comment);
      }
    }

    return null;
  }

  /**
   * Generate pattern-aware vector search SQL.
   */
  private String generateSearchSql(String schema, String table, VectorMetadata vm,
                                    VectorPattern pattern, String queryText,
                                    int limit, double threshold,
                                    String sourceTableFilter, boolean includeSource) {
    String qualifiedTable = schema != null ? schema + "." + table : table;
    String vectorCol = vm.getColumnName();

    // Build EMBED() call with proper parameters
    String embedCall = String.format("EMBED(?, %d, '%s', '%s')",
        vm.getDimension(), vm.getProvider(), vm.getModel());

    // Build similarity expression
    String similarityExpr = String.format("COSINE_SIMILARITY(t.%s, %s)",
        vectorCol, embedCall);

    StringBuilder sql = new StringBuilder();

    // SELECT clause
    sql.append("SELECT t.*");
    sql.append(", ").append(similarityExpr).append(" as similarity");

    // Add source table columns if requested
    if (includeSource && (pattern == VectorPattern.FK_FORMAL || pattern == VectorPattern.FK_LOGICAL)) {
      sql.append(", src.*");
    }

    // FROM clause
    sql.append(" FROM ").append(qualifiedTable).append(" t");

    // JOIN for Pattern 2a/2b
    if (includeSource) {
      if (pattern == VectorPattern.FK_FORMAL) {
        sql.append(" LEFT JOIN ").append(vm.getDiscoveredTargetTable()).append(" src");
        sql.append(" ON t.").append(vm.getDiscoveredFkColumn());
        sql.append(" = src.").append(vm.getDiscoveredTargetColumn());
      } else if (pattern == VectorPattern.FK_LOGICAL) {
        String sourceTable = vm.getSourceTable();
        if (schema != null && !sourceTable.contains(".")) {
          sourceTable = schema + "." + sourceTable;
        }
        sql.append(" LEFT JOIN ").append(sourceTable).append(" src");
        sql.append(" ON t.").append(vm.getSourceIdColumn());
        sql.append(" = src.id");  // Assumes 'id' column in source table
      }
    }

    // WHERE clause
    sql.append(" WHERE ").append(similarityExpr).append(" > ").append(threshold);

    // Pattern 3: Filter by source table
    if (pattern == VectorPattern.MULTI_SOURCE && sourceTableFilter != null) {
      sql.append(" AND t.").append(vm.getSourceTableColumn());
      sql.append(" = '").append(sourceTableFilter).append("'");
    }

    // ORDER and LIMIT
    sql.append(" ORDER BY similarity DESC");
    sql.append(" LIMIT ").append(limit);

    return sql.toString();
  }

  /**
   * Execute vector search query and return results.
   */
  private JsonArray executeVectorSearch(String sql, VectorMetadata vm) throws SQLException {
    JsonArray results = new JsonArray();

    try (PreparedStatement stmt = connection.getConnection().prepareStatement(sql)) {
      // Bind query text parameter (used in EMBED function)
      stmt.setString(1, "query text placeholder");  // TODO: Fix parameter binding

      try (ResultSet rs = stmt.executeQuery()) {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        while (rs.next()) {
          JsonObject row = new JsonObject();
          for (int i = 1; i <= columnCount; i++) {
            String colName = meta.getColumnName(i);
            Object value = rs.getObject(i);
            if (value != null) {
              row.addProperty(colName, value.toString());
            }
          }
          results.add(row);
        }
      }
    }

    return results;
  }
}
