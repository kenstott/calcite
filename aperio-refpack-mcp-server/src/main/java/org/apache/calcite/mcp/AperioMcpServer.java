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
package org.apache.calcite.mcp;

import org.apache.calcite.mcp.protocol.McpRequest;
import org.apache.calcite.mcp.protocol.McpResponse;
import org.apache.calcite.mcp.tools.DiscoveryTools;
import org.apache.calcite.mcp.tools.QueryTools;
import org.apache.calcite.mcp.tools.VectorSearchTools;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

/**
 * Aperio Refpack MCP Server - Model Context Protocol server for Apache Calcite.
 *
 * <p>Enables Claude Code and other MCP clients to discover schemas, query data,
 * and perform semantic search on vector embeddings through a JSON-RPC stdio interface.
 *
 * <p>Usage:
 * <pre>
 * java -jar aperio-refpack-mcp-server.jar --jdbc-url "jdbc:calcite:model=model.json"
 * </pre>
 */
public class AperioMcpServer {
  private static final Logger logger = LoggerFactory.getLogger(AperioMcpServer.class);
  private static final Gson gson = new Gson();

  private final CalciteConnection connection;
  private final DiscoveryTools discoveryTools;
  private final QueryTools queryTools;
  private final VectorSearchTools vectorSearchTools;

  public static void main(String[] args) {
    try {
      String jdbcUrl = parseJdbcUrl(args);
      if (jdbcUrl == null) {
        System.err.println("Usage: java -jar aperio-refpack-mcp-server.jar --jdbc-url <url>");
        System.exit(1);
      }

      AperioMcpServer server = new AperioMcpServer(jdbcUrl);
      server.start();

    } catch (Exception e) {
      logger.error("Failed to start MCP server", e);
      System.exit(1);
    }
  }

  public AperioMcpServer(String jdbcUrl) throws SQLException {
    this.connection = new CalciteConnection(jdbcUrl);
    this.discoveryTools = new DiscoveryTools(connection);
    this.queryTools = new QueryTools(connection);
    this.vectorSearchTools = new VectorSearchTools(connection);
  }

  /**
   * Start JSON-RPC stdio protocol loop.
   */
  public void start() {
    logger.info("Aperio MCP Server starting...");

    try (BufferedReader in = new BufferedReader(
             new InputStreamReader(System.in, StandardCharsets.UTF_8));
         PrintWriter out = new PrintWriter(System.out, true, StandardCharsets.UTF_8)) {

      String line;
      while ((line = in.readLine()) != null) {
        try {
          McpRequest request = gson.fromJson(line, McpRequest.class);
          McpResponse response = handleRequest(request);
          out.println(gson.toJson(response));
        } catch (Exception e) {
          logger.error("Error processing request", e);
          McpResponse error = McpResponse.error(null, -32603, "Internal error: " + e.getMessage());
          out.println(gson.toJson(error));
        }
      }

    } catch (Exception e) {
      logger.error("Fatal error in stdio loop", e);
    } finally {
      try {
        connection.close();
      } catch (SQLException e) {
        logger.error("Error closing connection", e);
      }
    }
  }

  /**
   * Handle MCP request and return response.
   */
  private McpResponse handleRequest(McpRequest request) {
    String method = request.getMethod();
    JsonObject params = request.getParams();

    try {
      JsonElement result;

      switch (method) {
        // Discovery tools
        case "list_schemas":
          result = discoveryTools.listSchemas();
          break;

        case "list_tables":
          String schema = getParam(params, "schema", null);
          boolean includeComments = getParam(params, "include_comments", true);
          result = discoveryTools.listTables(schema, includeComments);
          break;

        case "describe_table":
          schema = getParam(params, "schema", null);
          String table = getParam(params, "table", null);
          includeComments = getParam(params, "include_comments", true);
          result = discoveryTools.describeTable(schema, table, includeComments);
          break;

        // Query tools
        case "query_data":
          String sql = getParam(params, "sql", null);
          int limit = getParam(params, "limit", 100);
          result = queryTools.queryData(sql, limit);
          break;

        case "sample_table":
          schema = getParam(params, "schema", null);
          table = getParam(params, "table", null);
          limit = getParam(params, "limit", 10);
          result = queryTools.sampleTable(schema, table, limit);
          break;

        // Vector search tools
        case "semantic_search":
          schema = getParam(params, "schema", null);
          table = getParam(params, "table", null);
          String queryText = getParam(params, "query_text", null);
          limit = getParam(params, "limit", 10);
          double threshold = getParam(params, "threshold", 0.7);
          String sourceTableFilter = getParam(params, "source_table_filter", null);
          boolean includeSource = getParam(params, "include_source", false);
          result = vectorSearchTools.semanticSearch(schema, table, queryText,
              limit, threshold, sourceTableFilter, includeSource);
          break;

        case "list_vector_sources":
          schema = getParam(params, "schema", null);
          table = getParam(params, "table", null);
          result = vectorSearchTools.listVectorSources(schema, table);
          break;

        default:
          return McpResponse.error(request.getId(), -32601,
              "Method not found: " + method);
      }

      return McpResponse.success(request.getId(), result);

    } catch (SQLException e) {
      logger.error("SQL error handling request: " + method, e);
      return McpResponse.error(request.getId(), -32000,
          "SQL error: " + e.getMessage());
    } catch (Exception e) {
      logger.error("Error handling request: " + method, e);
      return McpResponse.error(request.getId(), -32603,
          "Internal error: " + e.getMessage());
    }
  }

  /**
   * Parse JDBC URL from command-line arguments.
   */
  private static String parseJdbcUrl(String[] args) {
    for (int i = 0; i < args.length - 1; i++) {
      if ("--jdbc-url".equals(args[i])) {
        return args[i + 1];
      }
    }
    return null;
  }

  /**
   * Get string parameter from JSON object.
   */
  private String getParam(JsonObject params, String name, String defaultValue) {
    if (params == null || !params.has(name)) {
      return defaultValue;
    }
    return params.get(name).getAsString();
  }

  /**
   * Get int parameter from JSON object.
   */
  private int getParam(JsonObject params, String name, int defaultValue) {
    if (params == null || !params.has(name)) {
      return defaultValue;
    }
    return params.get(name).getAsInt();
  }

  /**
   * Get double parameter from JSON object.
   */
  private double getParam(JsonObject params, String name, double defaultValue) {
    if (params == null || !params.has(name)) {
      return defaultValue;
    }
    return params.get(name).getAsDouble();
  }

  /**
   * Get boolean parameter from JSON object.
   */
  private boolean getParam(JsonObject params, String name, boolean defaultValue) {
    if (params == null || !params.has(name)) {
      return defaultValue;
    }
    return params.get(name).getAsBoolean();
  }
}
