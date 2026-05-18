/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.calcite.adapter.govdata.GovDataDriver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * AskAmerica MCP server — implements the Model Context Protocol over stdio.
 *
 * Each line on stdin is a JSON-RPC 2.0 request; each response is a single
 * JSON-RPC 2.0 object written to stdout. Logging goes to stderr so stdout
 * stays clean for the MCP client.
 *
 * Tools exposed:
 *   list_schemas    — available government data schemas
 *   list_tables     — tables in a schema
 *   describe_table  — column names/types for a table
 *   query           — execute SQL, returns rows as JSON array
 *
 * Environment variables:
 *   ASKAMERICA_SCHEMAS  — comma-separated source list (default: all 15)
 *   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL_S3
 *                       — R2 credentials for data access
 */
public class McpServer {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_LIMIT = 500;
    private static final int MAX_LIMIT = 5000;

    private static final String DEFAULT_SCHEMAS =
        "sec,geo,econ,census,crime,weather,ref,fec,"
        + "fedregister,cyber_vuln,cyber_threat,energy,health,edu,econ_reference";

    private static Connection conn;
    private static PrintStream log;

    public static void main(String[] args) throws Exception {
        boolean mcpMode = false;
        for (String arg : args) {
            if ("--mcp".equals(arg)) {
                mcpMode = true;
                break;
            }
        }

        if (!mcpMode) {
            // Launched interactively (double-clicked from OS) — show setup wizard.
            new SetupWindow().show();
            return;
        }

        // Redirect all logging away from stdout — MCP requires clean JSON on stdout only.
        log = System.err;
        suppressFrameworkLogging();

        log.println("[askamerica-mcp] Starting...");
        initConnection();
        log.println("[askamerica-mcp] Connected. Listening for MCP requests.");

        BufferedReader in = new BufferedReader(
            new InputStreamReader(System.in, "UTF-8"));
        PrintStream out = new PrintStream(System.out, true, "UTF-8");

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            try {
                JsonNode req = MAPPER.readTree(line);
                String method = req.path("method").asText("");

                // Notifications have no id — fire and forget, no response.
                if (!req.has("id")) {
                    handleNotification(method);
                    continue;
                }

                ObjectNode resp = dispatch(req, method);
                out.println(MAPPER.writeValueAsString(resp));
            } catch (Exception e) {
                log.println("[askamerica-mcp] Error: " + e.getMessage());
                ObjectNode err = errorResponse(null, -32700, "Parse error: " + e.getMessage());
                out.println(MAPPER.writeValueAsString(err));
            }
        }
    }

    // ── Dispatcher ────────────────────────────────────────────────────────────

    private static ObjectNode dispatch(JsonNode req, String method) {
        JsonNode id = req.get("id");
        JsonNode params = req.path("params");

        try {
            switch (method) {
                case "initialize":       return handleInitialize(id);
                case "tools/list":       return handleToolsList(id);
                case "tools/call":       return handleToolsCall(id, params);
                case "ping":             return result(id, MAPPER.createObjectNode());
                default:
                    return errorResponse(id, -32601, "Method not found: " + method);
            }
        } catch (Exception e) {
            log.println("[askamerica-mcp] Handler error: " + e.getMessage());
            return errorResponse(id, -32603, e.getMessage());
        }
    }

    private static void handleNotification(String method) {
        log.println("[askamerica-mcp] Notification: " + method);
    }

    // ── MCP handlers ─────────────────────────────────────────────────────────

    private static ObjectNode handleInitialize(JsonNode id) {
        ObjectNode serverInfo = MAPPER.createObjectNode();
        serverInfo.put("name", "AskAmerica");
        serverInfo.put("version", "1.0.0");

        ObjectNode capabilities = MAPPER.createObjectNode();
        capabilities.set("tools", MAPPER.createObjectNode());

        ObjectNode body = MAPPER.createObjectNode();
        body.put("protocolVersion", "2024-11-05");
        body.set("capabilities", capabilities);
        body.set("serverInfo", serverInfo);
        body.put("instructions",
            "Query US government data using SQL. "
            + "Schemas: sec (SEC filings/XBRL), geo (TIGER/FIPS), "
            + "econ (BLS/BEA), census (ACS), crime (FBI UCR), "
            + "weather (NOAA GHCND), ref (NAICS/SIC), fec (campaign finance), "
            + "fedregister, cyber_vuln (NVD CVEs), cyber_threat (CISA KEV), "
            + "energy (EIA), health (CDC/CMS), edu (NCES), econ_reference. "
            + "Call list_tables(schema) before querying. "
            + "Use FETCH FIRST N ROWS ONLY to limit results.");
        return result(id, body);
    }

    private static ObjectNode handleToolsList(JsonNode id) {
        ArrayNode tools = MAPPER.createArrayNode();

        tools.add(tool("list_schemas",
            "List all available US government data schemas.",
            MAPPER.createObjectNode()
                .put("type", "object")
                .<ObjectNode>set("properties", MAPPER.createObjectNode())
        ));

        ObjectNode listTablesProps = MAPPER.createObjectNode();
        listTablesProps.set("schema", prop("string",
            "Schema name, e.g. 'sec', 'geo', 'census'. Case-insensitive."));
        tools.add(tool("list_tables",
            "List all tables and views in a schema.",
            schema(listTablesProps, new String[]{"schema"})
        ));

        ObjectNode describeProps = MAPPER.createObjectNode();
        describeProps.set("schema", prop("string", "Schema name, e.g. 'sec'."));
        describeProps.set("table", prop("string", "Table name, e.g. 'filing_metadata'."));
        tools.add(tool("describe_table",
            "Get column names, types, and nullability for a table.",
            schema(describeProps, new String[]{"schema", "table"})
        ));

        ObjectNode queryProps = MAPPER.createObjectNode();
        queryProps.set("sql", prop("string",
            "Calcite SQL. Reference tables as schema.table "
            + "(e.g. sec.filing_metadata). Always include FETCH FIRST N ROWS ONLY."));
        queryProps.set("limit", prop("integer",
            "Max rows to return (default 500, max 5000)."));
        tools.add(tool("query",
            "Execute SQL against US government data. Returns a JSON array of row objects.",
            schema(queryProps, new String[]{"sql"})
        ));

        ObjectNode body = MAPPER.createObjectNode();
        body.set("tools", tools);
        return result(id, body);
    }

    private static ObjectNode handleToolsCall(JsonNode id, JsonNode params) throws Exception {
        String name = params.path("name").asText();
        JsonNode args = params.path("arguments");

        String text;
        switch (name) {
            case "list_schemas":
                text = listSchemas();
                break;
            case "list_tables":
                text = listTables(args.path("schema").asText());
                break;
            case "describe_table":
                text = describeTable(
                    args.path("schema").asText(),
                    args.path("table").asText());
                break;
            case "query":
                int limit = args.has("limit")
                    ? Math.min(Math.max(1, args.get("limit").asInt()), MAX_LIMIT)
                    : DEFAULT_LIMIT;
                text = query(args.path("sql").asText(), limit);
                break;
            default:
                return errorResponse(id, -32602, "Unknown tool: " + name);
        }

        ArrayNode content = MAPPER.createArrayNode();
        ObjectNode textBlock = MAPPER.createObjectNode();
        textBlock.put("type", "text");
        textBlock.put("text", text);
        content.add(textBlock);

        ObjectNode body = MAPPER.createObjectNode();
        body.set("content", content);
        body.put("isError", false);
        return result(id, body);
    }

    // ── Tool implementations ──────────────────────────────────────────────────

    private static String listSchemas() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getSchemas();
        ArrayNode arr = MAPPER.createArrayNode();
        while (rs.next()) {
            arr.add(rs.getString("TABLE_SCHEM"));
        }
        rs.close();
        return arr.toString();
    }

    private static String listTables(String schema) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getTables(null, schema.toUpperCase(), "%", null);
        ArrayNode arr = MAPPER.createArrayNode();
        while (rs.next()) {
            ObjectNode row = MAPPER.createObjectNode();
            row.put("table", rs.getString("TABLE_NAME"));
            row.put("type", rs.getString("TABLE_TYPE"));
            arr.add(row);
        }
        rs.close();
        return arr.toString();
    }

    private static String describeTable(String schema, String table) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getColumns(null, schema.toUpperCase(), table.toUpperCase(), "%");
        ArrayNode arr = MAPPER.createArrayNode();
        while (rs.next()) {
            ObjectNode col = MAPPER.createObjectNode();
            col.put("name", rs.getString("COLUMN_NAME"));
            col.put("type", rs.getString("TYPE_NAME"));
            col.put("nullable", rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
            arr.add(col);
        }
        rs.close();
        return arr.toString();
    }

    private static String query(String sql, int limit) throws SQLException {
        String effective = sql;
        String lower = sql.toLowerCase();
        if (!lower.contains("fetch first") && !lower.contains(" limit ")) {
            effective = sql.replaceAll(";\\s*$", "")
                + " FETCH FIRST " + limit + " ROWS ONLY";
        }
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(effective);
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            String[] names = new String[cols];
            for (int i = 0; i < cols; i++) {
                names[i] = meta.getColumnName(i + 1);
            }
            ArrayNode arr = MAPPER.createArrayNode();
            while (rs.next()) {
                ObjectNode row = MAPPER.createObjectNode();
                for (int i = 0; i < cols; i++) {
                    Object val = rs.getObject(i + 1);
                    if (rs.wasNull() || val == null) {
                        row.putNull(names[i]);
                    } else {
                        row.put(names[i], val.toString());
                    }
                }
                arr.add(row);
            }
            rs.close();
            return arr.toString();
        } finally {
            stmt.close();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void initConnection() throws Exception {
        String schemas = System.getenv("ASKAMERICA_SCHEMAS");
        if (schemas == null || schemas.trim().isEmpty()) {
            schemas = DEFAULT_SCHEMAS;
        }
        GovDataDriver driver = new GovDataDriver();
        Properties props = new Properties();
        conn = driver.connect("jdbc:govdata:source=" + schemas, props);
        if (conn == null) {
            throw new IllegalStateException(
                "GovDataDriver returned null — check credentials and schema names.");
        }
    }

    private static void suppressFrameworkLogging() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        System.setProperty("log4j.rootLogger", "ERROR");
    }

    private static ObjectNode result(JsonNode id, ObjectNode body) {
        ObjectNode resp = MAPPER.createObjectNode();
        resp.put("jsonrpc", "2.0");
        resp.set("id", id);
        resp.set("result", body);
        return resp;
    }

    private static ObjectNode errorResponse(JsonNode id, int code, String message) {
        ObjectNode resp = MAPPER.createObjectNode();
        resp.put("jsonrpc", "2.0");
        resp.set("id", id);
        ObjectNode err = MAPPER.createObjectNode();
        err.put("code", code);
        err.put("message", message);
        resp.set("error", err);
        return resp;
    }

    private static ObjectNode tool(String name, String description, ObjectNode inputSchema) {
        ObjectNode t = MAPPER.createObjectNode();
        t.put("name", name);
        t.put("description", description);
        t.set("inputSchema", inputSchema);
        return t;
    }

    private static ObjectNode prop(String type, String description) {
        ObjectNode p = MAPPER.createObjectNode();
        p.put("type", type);
        p.put("description", description);
        return p;
    }

    private static ObjectNode schema(ObjectNode properties, String[] required) {
        ObjectNode s = MAPPER.createObjectNode();
        s.put("type", "object");
        s.set("properties", properties);
        ArrayNode req = MAPPER.createArrayNode();
        for (String r : required) {
            req.add(r);
        }
        s.set("required", req);
        return s;
    }
}
