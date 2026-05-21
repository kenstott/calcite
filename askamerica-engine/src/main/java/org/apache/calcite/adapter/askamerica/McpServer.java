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
package org.apache.calcite.adapter.askamerica;

import org.apache.calcite.adapter.govdata.GovDataDriver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    private static volatile Connection conn;
    private static volatile Exception connError;
    private static final CountDownLatch connReady = new CountDownLatch(1);
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

        // Capture the real stdout before any framework can write to it, then
        // replace System.out with stderr so all logging goes there instead.
        // MCP JSON is written exclusively to the saved mcpOut stream.
        PrintStream mcpOut = System.out;
        System.setOut(System.err);

        log = System.err;
        suppressFrameworkLogging();

        log.println("[askamerica-mcp] Starting...");

        // Initialize the DB connection in the background so the MCP handshake
        // (initialize / tools/list) can complete immediately without timing out.
        Thread connThread = new Thread(() -> {
            try {
                initConnection();
                log.println("[askamerica-mcp] Connected.");
            } catch (Exception e) {
                connError = e;
                log.println("[askamerica-mcp] Connection failed: " + e.getMessage());
            } finally {
                connReady.countDown();
            }
        }, "conn-init");
        connThread.setDaemon(true);
        connThread.start();

        log.println("[askamerica-mcp] Listening for MCP requests.");

        BufferedReader in =
            new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
        PrintStream out = mcpOut;

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
            Throwable cause = e.getCause();
            while (cause != null) {
                log.println("[askamerica-mcp]   caused by: " + cause.getMessage());
                cause = cause.getCause();
            }
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
            "Query US government data using PostgreSQL-compatible SQL. "
            + "Schemas: sec (SEC filings/XBRL), geo (TIGER/FIPS), "
            + "econ (BLS/BEA), census (ACS), crime (FBI UCR), "
            + "weather (NOAA GHCND), ref (NAICS/SIC), fec (campaign finance), "
            + "fedregister, cyber_vuln (NVD CVEs), cyber_threat (CISA KEV), "
            + "energy (EIA), health (CDC/CMS), edu (NCES), econ_reference. "
            + "Call list_tables(schema) before querying. "
            + "When exploring or testing a query add FETCH FIRST N ROWS ONLY. "
            + "For analytical or aggregation queries omit the row limit so all "
            + "matching rows are processed. The limit parameter caps the rows "
            + "returned to the client (default 500, max 5000).");
        return result(id, body);
    }

    private static ObjectNode handleToolsList(JsonNode id) {
        ArrayNode tools = MAPPER.createArrayNode();

        tools.add(
            tool("list_schemas",
            "List all available US government data schemas.",
            MAPPER.createObjectNode()
                .put("type", "object")
                .<ObjectNode>set("properties", MAPPER.createObjectNode())));

        ObjectNode listTablesProps = MAPPER.createObjectNode();
        listTablesProps.set(
            "schema", prop("string",
            "Schema name, e.g. 'sec', 'geo', 'census'. Case-insensitive."));
        tools.add(
            tool("list_tables",
            "List all tables and views in a schema.",
            schema(listTablesProps, new String[]{"schema"})));

        ObjectNode describeProps = MAPPER.createObjectNode();
        describeProps.set("schema", prop("string", "Schema name, e.g. 'sec'."));
        describeProps.set("table", prop("string", "Table name, e.g. 'filing_metadata'."));
        tools.add(
            tool("describe_table",
            "Get column names, types, and nullability for a table.",
            schema(describeProps, new String[]{"schema", "table"})));

        ObjectNode queryProps = MAPPER.createObjectNode();
        queryProps.set(
            "sql", prop("string",
            "PostgreSQL-compatible SQL. Reference tables as schema.table "
            + "(e.g. sec.filing_metadata). Add FETCH FIRST N ROWS ONLY when "
            + "exploring; omit for analytical or aggregation queries."));
        queryProps.set(
            "limit", prop("integer",
            "Max rows to return (default 500, max 5000)."));
        tools.add(
            tool("query",
            "Execute SQL against US government data. Returns a JSON array of row objects.",
            schema(queryProps, new String[]{"sql"})));

        ObjectNode body = MAPPER.createObjectNode();
        body.set("tools", tools);
        return result(id, body);
    }

    private static void awaitConnection() throws Exception {
        if (!connReady.await(600, TimeUnit.SECONDS)) {
            throw new RuntimeException(
                "Database initialization is taking longer than expected "
                + "(first launch can take several minutes). Please retry.");
        }
        if (connError != null) {
            throw new RuntimeException("Connection failed: " + connError.getMessage(), connError);
        }
    }

    private static ObjectNode handleToolsCall(JsonNode id, JsonNode params) throws Exception {
        awaitConnection();
        String name = params.path("name").asText();
        JsonNode args = params.path("arguments");

        long t0 = System.currentTimeMillis();
        String text;
        switch (name) {
            case "list_schemas":
                log.println("[askamerica-mcp] tool=list_schemas");
                text = listSchemas();
                break;
            case "list_tables": {
                String schema = args.path("schema").asText();
                log.println("[askamerica-mcp] tool=list_tables schema=" + schema);
                text = listTables(schema);
                break;
            }
            case "describe_table": {
                String schema = args.path("schema").asText();
                String table  = args.path("table").asText();
                log.println("[askamerica-mcp] tool=describe_table schema=" + schema
                    + " table=" + table);
                text = describeTable(schema, table);
                break;
            }
            case "query": {
                int limit = args.has("limit")
                    ? Math.min(Math.max(1, args.get("limit").asInt()), MAX_LIMIT)
                    : DEFAULT_LIMIT;
                String sql = args.path("sql").asText();
                log.println("[askamerica-mcp] tool=query sql=" + sql);
                text = query(sql, limit);
                break;
            }
            default:
                return errorResponse(id, -32602, "Unknown tool: " + name);
        }

        long ms = System.currentTimeMillis() - t0;
        // text is a JSON array string; count commas at depth-1 to approximate rows
        int rows = text.startsWith("[{") ? countRows(text) : -1;
        if (rows >= 0) {
            log.println("[askamerica-mcp] tool=" + name + " rows=" + rows + " ms=" + ms);
        } else {
            log.println("[askamerica-mcp] tool=" + name + " ms=" + ms);
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

    private static int countRows(String json) {
        try {
            return MAPPER.readTree(json).size();
        } catch (Exception e) {
            return -1;
        }
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

        // Logback ignores the above properties — configure it via reflection.
        // Must run before initConnection() to suppress Hadoop/Calcite WARN spam
        // that would otherwise contaminate stdout (the MCP JSON channel).
        try {
            Class<?> contextClass = Class.forName("ch.qos.logback.classic.LoggerContext");
            Class<?> levelClass   = Class.forName("ch.qos.logback.classic.Level");
            Object context = org.slf4j.LoggerFactory.getILoggerFactory();
            if (!contextClass.isInstance(context)) {
                return;
            }
            Object errorLevel = levelClass.getField("ERROR").get(null);

            // Set root logger to ERROR.
            Object rootLogger = contextClass.getMethod("getLogger", String.class)
                .invoke(context, "ROOT");
            rootLogger.getClass().getMethod("setLevel", levelClass)
                .invoke(rootLogger, errorLevel);

            // Re-point every ConsoleAppender to System.err.
            java.util.List<?> loggers = (java.util.List<?>)
                contextClass.getMethod("getLoggerList").invoke(context);
            Class<?> consoleAppenderClass =
                Class.forName("ch.qos.logback.core.ConsoleAppender");
            for (Object logger : loggers) {
                java.util.Iterator<?> it = (java.util.Iterator<?>)
                    logger.getClass().getMethod("iteratorForAppenders").invoke(logger);
                while (it != null && it.hasNext()) {
                    Object appender = it.next();
                    if (consoleAppenderClass.isInstance(appender)) {
                        appender.getClass().getMethod("setTarget", String.class)
                            .invoke(appender, "System.err");
                    }
                }
            }
        } catch (Exception ignored) {
            // Logback not on classpath or reflection failed — nothing to do.
        }
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
