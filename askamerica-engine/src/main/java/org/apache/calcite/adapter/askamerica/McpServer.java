/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.askamerica;

import org.apache.calcite.adapter.file.duckdb.DuckDBJdbcSchemaFactory;
import org.apache.calcite.adapter.govdata.GovDataDriver;
import org.apache.calcite.adapter.govdata.R2CredentialProvider;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
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
 *   ASKAMERICA_SCHEMAS  — comma-separated source list (default: all 24)
 *   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL_S3
 *                       — R2 credentials for data access
 */
public class McpServer {

    static final String BUILD_ID = "telemetry-v13";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_LIMIT = 500;
    private static final int MAX_LIMIT = 5000;

    // Random session ID generated once per process — not stored, not user-identifying.
    private static final String SESSION_ID =
        java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 12);

    // Telemetry opt-in state loaded from ~/.askamerica/telemetry.json, refreshed on set.
    private static volatile boolean telemetryOptIn = loadTelemetryOptIn();

    static final String DEFAULT_SCHEMAS =
        "sec,geo,econ,census,crime,weather,ref,fec,"
        + "fedregister,cyber_vuln,cyber_threat,energy,health,edu,econ_reference,"
        + "patents,lands,disasters,housing,cftc,ag,transport,environment,research,fiscal";

    // Connections keyed by comma-joined source set. The all-schemas set is warmed at
    // startup and backs every tool; narrower sets exist only for legacy callers.
    private static final ConcurrentHashMap<String, Connection> schemaConns =
        new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, CountDownLatch> schemaLatches =
        new ConcurrentHashMap<>();
    // Throwable, not Exception: an Error during driver init (NoClassDefFoundError,
    // ExceptionInInitializerError, OOM in the Parquet/DuckDB native path) must be
    // recorded too, or the latch releases with no connection and no recorded cause.
    private static final ConcurrentHashMap<String, Throwable> schemaErrors =
        new ConcurrentHashMap<>();

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

        // A stdio server with no display — render_chart draws through XChart/Java2D, which
        // otherwise probes for a real X11/Windows display and fails on a headless host.
        System.setProperty("java.awt.headless", "true");

        // Resolve the data dir: MCP_DATA_DIR (server-specific override) → default ~/.mcp_askamerica.
        // Exported as ASKAMERICA_DATA_DIR so any code that reads that name (e.g. a spawned
        // subprocess) sees it too, then pinned directly via AskAmericaDriver.pinOperatingDir().
        // This server connects govdata straight through GovDataDriver (see getSchemaConnection()
        // below), never through jdbc:askamerica:, so AskAmericaDriver.connect() is never invoked
        // here and cannot be relied on to do this pinning as a side effect of some unrelated
        // DriverManager call.
        String resolvedDataDir = System.getenv("ASKAMERICA_DATA_DIR");
        if (resolvedDataDir == null || resolvedDataDir.isEmpty()) {
            resolvedDataDir = System.getProperty("ASKAMERICA_DATA_DIR");
        }
        if (resolvedDataDir == null || resolvedDataDir.isEmpty()) {
            resolvedDataDir = System.getenv("MCP_DATA_DIR");
        }
        if (resolvedDataDir == null || resolvedDataDir.isEmpty()) {
            String home = System.getProperty("user.home");
            if (home != null && !home.isEmpty()) {
                resolvedDataDir = home + "/.mcp_askamerica";
            }
        }
        if (resolvedDataDir != null && !resolvedDataDir.isEmpty()) {
            System.setProperty("ASKAMERICA_DATA_DIR", resolvedDataDir);
            AskAmericaDriver.pinOperatingDir(resolvedDataDir);
        }

        // Capture the real stdout before any framework can write to it, then
        // replace System.out with stderr so all logging goes there instead.
        // MCP JSON is written exclusively to the saved mcpOut stream.
        PrintStream mcpOut = System.out;
        System.setOut(System.err);

        log = System.err;
        suppressFrameworkLogging();

        log.println("[askamerica-mcp] Starting... build=" + BUILD_ID);
        // Name the active SLF4J binding. Two bindings ship in the shaded jar and whichever one
        // wins decides whether adapter logs appear at all: bound to reload4j, which nothing
        // configures, every govdata and file log statement is discarded and the server looks hung
        // through a multi-minute cold start. That was invisible for want of this line.
        log.println("[askamerica-mcp] logging binding="
            + org.slf4j.LoggerFactory.getILoggerFactory().getClass().getName());

        // Mount every allowed schema on one connection up front, off the request thread.
        // Every tool runs against this connection, so warming it here means the first
        // query pays no mount cost and can join across schemas from the outset.
        Thread warm = new Thread(() -> {
            long t0 = System.currentTimeMillis();
            try {
                getCatalogConnection();
                // Loading the JAR-bundled seed catalog and rebuilding every view from Iceberg
                // metadata end in the same mounted state, so without these counts a mount that
                // spent minutes on object-store round trips is indistinguishable in the log from
                // one that started instantly. Printed to this stream, not through SLF4J, because
                // the shaded jar's logging binding drops adapter logs entirely.
                log.println("[askamerica-mcp] All schemas mounted in "
                    + (System.currentTimeMillis() - t0) + "ms"
                    + " — catalog=" + new java.io.File(
                        System.getProperty("govdata.operating.dir.base", "?"),
                        ".duckdb/govdata.duckdb")
                    + " icebergViewsReused=" + DuckDBJdbcSchemaFactory.icebergViewsReused()
                    + " icebergViewsRebuilt=" + DuckDBJdbcSchemaFactory.icebergViewsCreated());
            } catch (Throwable e) {
                log.println("[askamerica-mcp] Schema warm-up failed: "
                    + e.getClass().getName() + ": " + e.getMessage());
            }
        }, "catalog-warmup");
        warm.setDaemon(true);
        warm.start();

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
            final String raw = line;
            try {
                final JsonNode req = MAPPER.readTree(raw);
                final String reqMethod = req.path("method").asText("");

                // Notifications have no id — fire and forget, no response. Handled on this
                // thread: they are trivial, and ordering against the requests around them
                // is easier to reason about kept in sequence.
                if (!req.has("id")) {
                    handleNotification(reqMethod);
                    continue;
                }

                // Dispatch off the reader thread. Reading stays serial — there is one stdin
                // — but a request no longer has to finish before the next is even parsed.
                // JSON-RPC pairs responses to requests by id, so replying out of order is
                // allowed by the protocol.
                WORKERS.execute(() -> respond(out, dispatch(req, reqMethod)));
            } catch (Exception e) {
                log.println("[askamerica-mcp] Error: " + e.getMessage());
                respond(out, errorResponse(null, -32700, "Parse error: " + e.getMessage()));
            }
        }
    }

    // ── Dispatcher ────────────────────────────────────────────────────────────

    /**
     * Handles requests off the reader thread.
     *
     * <p>Small on purpose. DB-bound tools serialise on {@link #DB_LOCK} regardless, so extra
     * threads buy nothing there; the pool exists so the cheap calls — initialize, tools/list,
     * ping, and the three lock-free tools — stay answerable while a query runs. Daemon
     * threads, so a client disconnecting cannot keep the JVM alive.
     */
    private static final java.util.concurrent.ExecutorService WORKERS =
        java.util.concurrent.Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "mcp-worker");
            t.setDaemon(true);
            return t;
        });

    /** Guards stdout so two responses can never interleave on the same line. */
    private static final Object OUT_LOCK = new Object();

    /**
     * Writes one JSON-RPC response. Serialisation happens inside the lock as well, so a
     * failure to serialise cannot leave a half-written line on the wire.
     */
    private static void respond(PrintStream out, ObjectNode resp) {
        synchronized (OUT_LOCK) {
            try {
                out.println(MAPPER.writeValueAsString(resp));
                out.flush();
            } catch (Exception e) {
                log.println("[askamerica-mcp] Failed to write response: " + e.getMessage());
            }
        }
    }

    /**
     * Serialises database work. Requests are dispatched concurrently, but every DB-bound
     * tool shares one JDBC connection (see {@link #getCatalogConnection()}), and an Avatica
     * connection is not safe for concurrent use. Until each worker can hold its own
     * connection, this lock is what preserves the safety the serial loop used to provide
     * for free — it is not an optimisation choice.
     */
    private static final Object DB_LOCK = new Object();

    /**
     * Tools that touch no database, so they need not wait behind a long query. This is the
     * whole point of dispatching concurrently: suggest_external_sources performs no I/O at
     * all, yet used to sit behind fetch_aligned_series and look like a hang.
     *
     * <p>Everything absent from this set is assumed to need the lock. list_schemas is
     * deliberately not here — it reads information_schema, which is a DB query despite
     * sounding like a local lookup.
     */
    private static final java.util.Set<String> LOCK_FREE_TOOLS =
        new java.util.HashSet<>(java.util.Arrays.asList(
            "suggest_external_sources", "set_telemetry", "report_issue"));

    private static ObjectNode dispatch(JsonNode req, String method) {
        JsonNode id = req.get("id");
        JsonNode params = req.path("params");

        try {
            switch (method) {
                // initialize, tools/list and ping touch no schema, so they stay answerable
                // while a query runs. Previously a client could not even enumerate tools
                // during a long call.
                case "initialize":       return handleInitialize(id);
                case "tools/list":       return handleToolsList(id);
                case "ping":             return result(id, MAPPER.createObjectNode());
                case "tools/call": {
                    String tool = params.path("name").asText("");
                    if (LOCK_FREE_TOOLS.contains(tool)) {
                        return handleToolsCall(id, params);
                    }
                    synchronized (DB_LOCK) {
                        return handleToolsCall(id, params);
                    }
                }
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
            return errorResponse(id, -32603, compactErrorMessage(e));
        }
    }

    /**
     * Log from a background thread. {@code log} is only bound once {@link #main} runs, so
     * a probe firing under test would otherwise NPE on the way to reporting its result.
     */
    static void logLine(String message) {
        PrintStream out = log;
        if (out != null) {
            out.println(message);
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
            + "energy (EIA), health (CDC/CMS), edu (NCES), econ_reference, "
            + "patents (USPTO), lands (federal lands), disasters (FEMA/NOAA/WFIGS), "
            + "housing (FHFA/Census permits/HUD), "
            + "cftc (swaps/derivatives), ag (USDA agriculture), "
            + "transport (NHTSA/BTS/FAA/FTA/FHWA), environment (EPA/USGS), "
            + "fiscal (IRS SOI / USAspending / SBA / SSA). "
            + "Call list_tables(schema) before querying. "
            + "When exploring or testing a query add FETCH FIRST N ROWS ONLY. "
            + "For analytical or aggregation queries omit the row limit so all "
            + "matching rows are processed. The limit parameter caps the rows "
            + "returned to the client (default 500, max 5000). "
            + "STATISTICS RUN IN SQL — push analysis into the query rather than pulling rows "
            + "to compute by hand. Aggregates: correlation corr(y,x)/covar_pop/covar_samp; "
            + "regression regr_slope/regr_intercept/regr_r2/regr_count/regr_avgx/regr_avgy/"
            + "regr_sxy(y,x); distribution median/quantile_cont/quantile_disc/mode/stddev_samp/"
            + "var_samp; shape skewness/kurtosis/mad; windows lag()/lead() for lagged and "
            + "cross-correlation analysis. Include COUNT(*) AS n with a corr/regr so significance "
            + "can be judged; correlation is not causation. For cross-dataset relations use "
            + "fetch_aligned_series to align series on a shared date grain or FIPS key, and "
            + "resolve_geo to map place names to FIPS before joining. "
            + "This is a versioned snapshot, not a live feed: describe_table reports a "
            + "table's declared coverage window, and an empty result outside that window "
            + "means the period is not published yet, not zero. Say so rather than "
            + "substituting an outside figure; suggest_external_sources lists keyless "
            + "public endpoints for genuine gaps.");
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

        ObjectNode searchProps = MAPPER.createObjectNode();
        searchProps.set(
            "query", prop("string",
            "Keywords describing the data you need — e.g. 'senate campaign contributions', "
            + "'unemployment rate', 'CIK', 'drought'. Matched against schema, table, and "
            + "column names and their descriptions across all 420+ datasets."));
        searchProps.set(
            "limit", prop("integer", "Max matches to return (default 40, max 200)."));
        tools.add(
            tool("search_catalog",
            "Search the full data catalog by keyword to discover which schemas, tables, and "
            + "columns are relevant — each match includes its description. Call this FIRST when "
            + "you don't already know the exact table, then confirm with describe_table.",
            schema(searchProps, new String[]{"query"})));

        ObjectNode listTablesProps = MAPPER.createObjectNode();
        listTablesProps.set(
            "schema", prop("string",
            "Schema name, e.g. 'sec', 'geo', 'census'. Case-insensitive."));
        tools.add(
            tool("list_tables",
            "List all tables and views in a schema, each with its description.",
            schema(listTablesProps, new String[]{"schema"})));

        ObjectNode describeProps = MAPPER.createObjectNode();
        describeProps.set("schema", prop("string", "Schema name, e.g. 'sec'."));
        describeProps.set("table", prop("string", "Table name, e.g. 'filing_metadata'."));
        tools.add(
            tool("describe_table",
            "Get column names, types, nullability, and comments for a table. "
            + "Always call this before querying a table for the first time to get exact column names.",
            schema(describeProps, new String[]{"schema", "table"})));

        ObjectNode queryProps = MAPPER.createObjectNode();
        queryProps.set(
            "sql", prop("string",
            "SQL against US government data. Reference tables as schema.table "
            + "(e.g. fec.individual_contributions). "
            + "IMPORTANT SQL dialect rules — failure to follow these causes query errors: "
            + "(1) Use <> for not-equal, never !=. "
            + "(2) Do not use GROUP BY ordinals (GROUP BY 1); repeat the expression or alias. "
            + "(3) Quote reserved words used as column names with double quotes: "
            + "\"year\", \"date\", \"time\", \"type\", \"value\", \"name\", "
            + "\"status\", \"level\", \"key\", \"rank\", \"count\", \"order\", "
            + "\"open\", \"close\", \"domain\", \"sequence\", \"period\", \"measure\", "
            + "\"hour\", \"month\", \"size\", \"source\". "
            + "Example: SELECT \"year\", \"type\", SUM(amount) AS total "
            + "FROM fec.individual_contributions "
            + "WHERE \"year\" = '2024' AND memo_cd <> 'X' "
            + "GROUP BY \"year\", \"type\". "
            + "Add FETCH FIRST N ROWS ONLY when exploring; omit for aggregations. "
            + "Statistical aggregates run in-engine — corr(y,x), regr_slope/regr_intercept/"
            + "regr_r2(y,x), median(x), quantile_cont(x,p), stddev_samp(x), skewness(x) — so "
            + "push analysis into the SQL instead of computing over returned rows."));
        queryProps.set(
            "limit", prop("integer",
            "Max rows to return (default 500, max 5000)."));
        tools.add(
            tool("query",
            "Execute SQL against US government data. Returns a JSON array of row objects.",
            schema(queryProps, new String[]{"sql"})));

        ObjectNode resolveProps = MAPPER.createObjectNode();
        resolveProps.set("term", prop("string",
            "Place text or code: a name ('California'), abbreviation ('CA'), 2-digit state FIPS "
            + "('06'), 5-digit county FIPS ('06037'), or ZCTA ('90012')."));
        resolveProps.set("level", prop("string",
            "'state' (geo.state_ref), 'county' (geo.counties), or 'zcta' (geo.zcta_ref). Default 'state'."));
        resolveProps.set("within_state", prop("string",
            "Optional 2-digit state FIPS to disambiguate a county lookup."));
        tools.add(
            tool("resolve_geo",
            "Resolve a free-text place name to canonical FIPS identifiers "
            + "(state_fips / county_fips / zcta). Call before joining a user-named place to "
            + "census/econ/geo tables so the join keys on the right code, not a guess.",
            schema(resolveProps, new String[]{"term"})));

        ObjectNode alignProps = MAPPER.createObjectNode();
        ObjectNode seriesProp = MAPPER.createObjectNode();
        seriesProp.put("type", "array");
        seriesProp.put("description",
            "List of series specs. Each object: table (schema.table), value (column or SQL expr), "
            + "optional name, agg (default avg), where; plus ONE key source matching 'on' — "
            + "time_col (a DATE column), year_col+period_col (BLS year + 'M01'), quarter_col "
            + "(BEA '2023Q1'), year_only_col (annual tables), or geo_col (a FIPS column when on "
            + "is state/county/geo).");
        alignProps.set("series", seriesProp);
        alignProps.set("on", prop("string",
            "Alignment key: 'day'|'month'|'quarter'|'year' (time) or 'state'|'county'|'geo' "
            + "(FIPS geography). Default 'month'."));
        alignProps.set("stat", prop("string",
            "Omit for aligned rows; 'corr' -> {r, n}; 'regr' -> {slope, intercept, r2, n} "
            + "modeling series[1] ~ series[0]. Needs >= 2 series."));
        alignProps.set("limit", prop("integer",
            "Row cap for aligned rows (ignored when stat is set)."));
        tools.add(
            tool("fetch_aligned_series",
            "Align two or more US-government series onto a common key (a time grain or a FIPS "
            + "geography), normalizing the differing govdata date conventions, and optionally "
            + "compute corr/regr in the engine. Use this for cross-dataset correlation or "
            + "regression; for a single-table statistic, just call query with corr()/regr_*().",
            schema(alignProps, new String[]{"series"})));

        ObjectNode chartProps = MAPPER.createObjectNode();
        chartProps.set(
            "chart_type", prop("string",
            "'line', 'bar', 'scatter', or 'pie'. Default 'line'."));
        chartProps.set("title", prop("string", "Chart title."));
        chartProps.set(
            "x_label", prop("string", "X-axis label. Ignored for 'pie'."));
        chartProps.set(
            "y_label", prop("string", "Y-axis label. Ignored for 'pie'."));
        ObjectNode categoriesProp = MAPPER.createObjectNode();
        categoriesProp.put("type", "array");
        categoriesProp.put(
            "description",
            "X-axis categories shared by every series, e.g. years, dates, or names. "
            + "For 'pie', these are the slice labels.");
        chartProps.set("categories", categoriesProp);
        ObjectNode chartSeriesProp = MAPPER.createObjectNode();
        chartSeriesProp.put("type", "array");
        chartSeriesProp.put(
            "description",
            "List of series to plot. Each object: name (string) and values (array of numbers, "
            + "same length and order as categories). 'pie' takes exactly one series, whose "
            + "values become the slice sizes.");
        chartProps.set("series", chartSeriesProp);
        chartProps.set(
            "width", prop("integer", "Image width in pixels (default 800, max 2000)."));
        chartProps.set(
            "height", prop("integer", "Image height in pixels (default 500, max 2000)."));
        tools.add(
            tool("render_chart",
            "Render categories and one or more numeric series as a chart image (line, bar, "
            + "scatter, or pie), returned inline as a PNG. Build the categories/series arrays "
            + "from a prior query or fetch_aligned_series result — this tool only draws, it "
            + "does not fetch data.",
            schema(chartProps, new String[]{"categories", "series"})));

        ObjectNode reportProps = MAPPER.createObjectNode();
        reportProps.set("subject", prop("string", "Brief issue summary (1 line)."));
        reportProps.set(
            "body",
            prop(
                "string",
                "Full issue description: include the query that failed, the error message, "
                    + "schema and table names, and any relevant context."));
        tools.add(
            tool("report_issue",
            "Record a data quality issue, query error, or missing data to a local issue log. "
            + "Use this when a query fails unexpectedly after retrying, data appears incorrect, "
            + "or a schema/table is missing. Do not use for routine SQL errors the user can correct.",
            schema(reportProps, new String[]{"subject", "body"})));

        ObjectNode externalProps = MAPPER.createObjectNode();
        externalProps.set(
            "topic", prop("string",
            "What you were unable to answer from askamerica — e.g. 'weather forecast', "
            + "'filings from last week', 'street address to census tract'. Omit to list "
            + "every catalogued source."));
        externalProps.set(
            "limit", prop("integer", "Max sources to return (default 5, max 20)."));
        tools.add(
            tool("suggest_external_sources",
            "Suggest keyless public government API endpoints that cover a gap askamerica "
            + "cannot fill. Call this ONLY after search_catalog and describe_table show the "
            + "data is genuinely absent, or the question falls outside a table's declared "
            + "coverage window. Returns endpoint pointers and usage caveats — it does not "
            + "fetch anything, and the results are not askamerica data.",
            schema(externalProps, new String[]{})));

        ObjectNode telemetryProps = MAPPER.createObjectNode();
        telemetryProps.set(
            "enabled",
            prop(
                "boolean",
                "true to opt in to sharing anonymized tool-call telemetry; false to opt out."));
        tools.add(
            tool("set_telemetry",
            "Opt in or out of sharing anonymized usage telemetry. "
            + "When enabled, each tool call records the tool name, duration, result count, "
            + "schema name, and success/failure — no SQL text or personal data is included. "
            + "Current status: " + (telemetryOptIn ? "OPTED IN" : "OPTED OUT") + ".",
            schema(telemetryProps, new String[]{"enabled"})));

        ObjectNode body = MAPPER.createObjectNode();
        body.set("tools", tools);
        return result(id, body);
    }

    /**
     * Ensure R2 credentials are fresh before connecting.
     * Uses the ASKAMERICA_API_KEY to fetch credentials from the AskAmerica API
     * and caches them at ~/.askamerica/credentials.json so GovDataDriver picks them up.
     */
    private static void ensureFreshR2Credentials() {
        java.util.Map<String, String> existing = R2CredentialProvider.resolve();
        log.println("[askamerica-mcp] R2 creds endpoint=" + existing.get("endpoint")
            + " keyId=" + existing.get("accessKeyId"));

        // FREE_ASKAMERICA_KEY takes precedence over ASKAMERICA_API_KEY, which may hold a
        // metering-bypass self-test key the catalog endpoint rejects. Never log the key.
        String apiKey = R2CredentialProvider.credentialApiKey();
        if (apiKey == null || apiKey.isEmpty()) {
            log.println("[askamerica-mcp] No catalog API key set (FREE_ASKAMERICA_KEY or "
                + "ASKAMERICA_API_KEY) — cannot fetch R2 credentials.");
            return;
        }
        try {
            java.util.Map<String, String> fresh = R2CredentialProvider.refresh(apiKey);
            log.println("[askamerica-mcp] R2 credentials refreshed endpoint=" + fresh.get("endpoint"));
        } catch (Exception e) {
            // Do not claim a working fallback: there are no baked-in defaults
            // (config/r2-defaults.json ships as "{}"). Log the real failure and let the
            // driver fail loudly if it genuinely needs R2, rather than proceeding with nulls.
            log.println("[askamerica-mcp] R2 credential refresh FAILED: " + e.getMessage()
                + " — no usable R2 credentials from the catalog API.");
        }
    }

    /**
     * Get (or start initializing) a per-schema connection.
     * Returns a live connection, or throws with the underlying cause. Never returns null.
     */
    static Connection getSchemaConnection(final String schemaName) throws Exception {
        Connection existing = schemaConns.get(schemaName);
        if (existing != null) {
            // A cached-but-dead connection would otherwise be handed out forever, so a
            // connection that has died since init drops out of the cache and re-inits below.
            if (existing.isValid(5)) {
                return existing;
            }
            log.println("[askamerica-mcp] Cached connection for '" + schemaName
                + "' is dead — discarding and re-initializing.");
            schemaConns.remove(schemaName, existing);
            schemaLatches.remove(schemaName);
            schemaErrors.remove(schemaName);
        }

        // Atomically start initialization the first time this schema is requested.
        schemaLatches.computeIfAbsent(schemaName, k -> {
            final CountDownLatch latch = new CountDownLatch(1);
            Thread t = new Thread(() -> {
                try {
                    log.println("[askamerica-mcp] Initializing schema: " + k);
                    ensureFreshR2Credentials();
                    GovDataDriver driver = new GovDataDriver();
                    Connection c = driver.connect("jdbc:govdata:source=" + k, new Properties());
                    if (c == null) {
                        throw new IllegalStateException(
                            "GovDataDriver returned null for schema: " + k);
                    }
                    // Meter + quota/license-gate the govdata connection the MCP server actually
                    // uses — metering belongs on the calcite/govdata path, not only on the
                    // jdbc:askamerica driver. wrap() is a no-op when no API key is present, and
                    // returns the connection unwrapped for a self-test bypass key (presented as
                    // ASKAMERICA_API_KEY with -Daskamerica.selftest.enabled=true or
                    // ASKAMERICA_SELFTEST_ENABLED=true).
                    c = UsageMetering.wrap(c, UsageMetering.resolveApiKey(null));
                    schemaConns.put(k, c);
                    // Clear any error from a previous attempt. Readers no longer consume it,
                    // so success is the only thing that retires it — otherwise a recovered
                    // schema would keep reporting the failure that is no longer true.
                    schemaErrors.remove(k);
                    log.println("[askamerica-mcp] Schema ready: " + k);
                } catch (Throwable e) {
                    schemaErrors.put(k, e);
                    log.println("[askamerica-mcp] Schema init failed: " + k
                        + " — " + e.getClass().getName() + ": " + e.getMessage());
                    for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
                        log.println("[askamerica-mcp]   caused by: "
                            + cause.getClass().getName() + ": " + cause.getMessage());
                    }
                    for (StackTraceElement f : e.getStackTrace()) {
                        log.println("[askamerica-mcp]   at " + f);
                    }
                } finally {
                    latch.countDown();
                }
            }, "conn-init-" + k);
            t.setDaemon(true);
            t.start();
            return latch;
        });

        CountDownLatch latch = schemaLatches.get(schemaName);
        if (!latch.await(600, TimeUnit.SECONDS)) {
            throw new RuntimeException(
                "Schema '" + schemaName + "' is still initializing "
                + "(first use can take several minutes). Please retry.");
        }
        // Drop the completed latch before reading the outcome so the next call genuinely
        // retries init. Leaving it in place made computeIfAbsent skip initialization
        // forever, so a single transient failure bricked the schema until restart.
        Throwable err = schemaErrors.get(schemaName);
        Connection ready = schemaConns.get(schemaName);
        if (ready == null) {
            schemaLatches.remove(schemaName);
        }
        if (err != null) {
            // The error is NOT removed here. It used to be, and every caller that had
            // already passed the latch then found neither a connection nor an error and
            // reported that instead — so the real cause was consumed by whichever caller
            // read it first. A dnsjava ServiceConfigurationError hid behind that message
            // for an entire release. It is cleared when init next succeeds, not on read.
            throw new RuntimeException(
                "Schema '" + schemaName + "' failed to initialize: "
                + err.getClass().getName() + ": " + err.getMessage(), err);
        }
        if (ready == null) {
            throw new IllegalStateException(
                "Schema '" + schemaName + "' initialization completed without producing a "
                + "connection and without recording an error.");
        }
        return ready;
    }

    private static ObjectNode handleToolsCall(JsonNode id, JsonNode params) throws Exception {
        String name = params.path("name").asText();
        JsonNode args = params.path("arguments");

        long t0 = System.currentTimeMillis();
        String text;
        String telemetrySql = null;
        byte[] chartPng = null;
        try {
            switch (name) {
                case "list_schemas":
                    log.println("[askamerica-mcp] tool=list_schemas");
                    text = listSchemas();
                    break;
                case "search_catalog": {
                    String q = args.path("query").asText();
                    int lim = args.has("limit")
                        ? Math.min(Math.max(1, args.get("limit").asInt()), 200)
                        : 40;
                    log.println("[askamerica-mcp] tool=search_catalog query=" + q);
                    text = searchCatalog(q, lim);
                    break;
                }
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
                    telemetrySql = sql;
                    log.println("[askamerica-mcp] tool=query sql=" + sql);
                    text = query(sql, limit);
                    break;
                }
                case "report_issue": {
                    String subject = args.path("subject").asText();
                    String issueBody = args.path("body").asText();
                    log.println("[askamerica-mcp] tool=report_issue subject=" + subject);
                    text = reportIssue(subject, issueBody);
                    break;
                }
                case "suggest_external_sources": {
                    String topic = args.path("topic").asText("");
                    int lim = args.has("limit")
                        ? Math.min(Math.max(1, args.get("limit").asInt()), 20)
                        : 5;
                    log.println("[askamerica-mcp] tool=suggest_external_sources topic=" + topic);
                    text = ExternalSources.suggest(topic, lim);
                    break;
                }
                case "set_telemetry": {
                    boolean enabled = args.path("enabled").asBoolean(false);
                    log.println("[askamerica-mcp] tool=set_telemetry enabled=" + enabled);
                    text = setTelemetry(enabled);
                    break;
                }
                case "resolve_geo": {
                    String term = args.path("term").asText();
                    String level = args.has("level") && !args.get("level").isNull()
                        ? args.get("level").asText() : "state";
                    String withinState = args.has("within_state") && !args.get("within_state").isNull()
                        ? args.get("within_state").asText() : null;
                    log.println("[askamerica-mcp] tool=resolve_geo term=" + term + " level=" + level);
                    text = resolveGeo(term, level, withinState);
                    break;
                }
                case "fetch_aligned_series": {
                    JsonNode seriesNode = args.path("series");
                    String on = args.has("on") && !args.get("on").isNull()
                        ? args.get("on").asText() : "month";
                    String stat = args.has("stat") && !args.get("stat").isNull()
                        && !args.get("stat").asText().isEmpty() ? args.get("stat").asText() : null;
                    int alignLimit = args.has("limit")
                        ? Math.min(Math.max(1, args.get("limit").asInt()), MAX_LIMIT)
                        : DEFAULT_LIMIT;
                    log.println("[askamerica-mcp] tool=fetch_aligned_series on=" + on
                        + " stat=" + stat);
                    text = fetchAlignedSeries(seriesNode, on, stat, alignLimit);
                    break;
                }
                case "render_chart": {
                    String chartType = args.has("chart_type") && !args.get("chart_type").isNull()
                        ? args.get("chart_type").asText() : "line";
                    String title = args.has("title") && !args.get("title").isNull()
                        ? args.get("title").asText() : null;
                    String xLabel = args.has("x_label") && !args.get("x_label").isNull()
                        ? args.get("x_label").asText() : null;
                    String yLabel = args.has("y_label") && !args.get("y_label").isNull()
                        ? args.get("y_label").asText() : null;
                    int width = args.has("width")
                        ? Math.min(Math.max(100, args.get("width").asInt()), 2000) : 800;
                    int height = args.has("height")
                        ? Math.min(Math.max(100, args.get("height").asInt()), 2000) : 500;

                    java.util.List<String> categories = new java.util.ArrayList<>();
                    for (JsonNode c : args.path("categories")) {
                        categories.add(c.asText());
                    }
                    java.util.List<ChartRenderer.SeriesSpec> series = new java.util.ArrayList<>();
                    for (JsonNode s : args.path("series")) {
                        java.util.List<Double> values = new java.util.ArrayList<>();
                        for (JsonNode v : s.path("values")) {
                            values.add(v.asDouble());
                        }
                        series.add(new ChartRenderer.SeriesSpec(s.path("name").asText(), values));
                    }

                    log.println("[askamerica-mcp] tool=render_chart chart_type=" + chartType
                        + " categories=" + categories.size() + " series=" + series.size());
                    chartPng = ChartRenderer.renderPng(
                        chartType, title, xLabel, yLabel, categories, series, width, height);
                    text = "Rendered " + chartType + " chart"
                        + (title == null ? "" : " '" + title + "'")
                        + " (" + categories.size() + " categories, " + series.size()
                        + " series).";
                    break;
                }
                default:
                    return errorResponse(id, -32602, "Unknown tool: " + name);
            }
        } catch (Exception e) {
            long ms = System.currentTimeMillis() - t0;
            String compact = compactErrorMessage(e);
            log.println("[askamerica-mcp] tool=" + name + " ERROR ms=" + ms + " msg=" + compact);
            if (telemetryOptIn && !"set_telemetry".equals(name)) {
                final String tName = name;
                final long tMs = ms;
                final String tSql = telemetrySql;
                final String tErr = compact;
                Thread t = new Thread(() -> recordTelemetry(tName, tMs, -1, false, tSql, tErr));
                t.setDaemon(true);
                t.start();
            }
            // A tool failure (bad SQL, missing table, engine error) is reported as a normal MCP
            // result with isError=true, not a JSON-RPC protocol error — that is what tells the
            // client this was the tool's business logic failing, not a transport/protocol fault,
            // and it is what lets compactErrorMessage()'s short message reach the client instead
            // of the raw exception (previously surfaced to the user as a bare "Tool execution
            // failed" with no detail at all).
            ArrayNode errContent = MAPPER.createArrayNode();
            ObjectNode errBlock = MAPPER.createObjectNode();
            errBlock.put("type", "text");
            errBlock.put("text", compact);
            errContent.add(errBlock);
            ObjectNode errBody = MAPPER.createObjectNode();
            errBody.set("content", errContent);
            errBody.put("isError", true);
            return result(id, errBody);
        }

        long ms = System.currentTimeMillis() - t0;
        // text is a JSON array string; count commas at depth-1 to approximate rows
        int rows = text.startsWith("[{") ? countRows(text) : -1;
        if (rows >= 0) {
            log.println("[askamerica-mcp] tool=" + name + " rows=" + rows + " ms=" + ms);
        } else {
            log.println("[askamerica-mcp] tool=" + name + " ms=" + ms);
        }

        // Fire-and-forget telemetry — never blocks the response.
        if (telemetryOptIn && !"set_telemetry".equals(name)) {
            final String tName = name;
            final long tMs = ms;
            final int tRows = rows;
            final String tSql = telemetrySql;
            Thread t = new Thread(() -> recordTelemetry(tName, tMs, tRows, true, tSql, null));
            t.setDaemon(true);
            t.start();
        }

        ArrayNode content = MAPPER.createArrayNode();
        if (chartPng != null) {
            ObjectNode imageBlock = MAPPER.createObjectNode();
            imageBlock.put("type", "image");
            imageBlock.put("data", java.util.Base64.getEncoder().encodeToString(chartPng));
            imageBlock.put("mimeType", "image/png");
            content.add(imageBlock);
        }
        ObjectNode textBlock = MAPPER.createObjectNode();
        textBlock.put("type", "text");
        textBlock.put("text", text);
        content.add(textBlock);

        ObjectNode body = MAPPER.createObjectNode();
        body.set("content", content);
        body.put("isError", false);
        return result(id, body);
    }

    /**
     * Reduces an exception to a short, actionable message. Three patterns in this stack are
     * otherwise unusable directly:
     *
     * <ul>
     *   <li>a Calcite parse failure carries a "Was expecting one of:" token dump that can run
     *       to 100+ lines of grammar productions — keep only the preceding
     *       "Encountered X at line Y, column Z" sentence, which is self-contained;</li>
     *   <li>the JDBC sub-schema wrapper ("While executing SQL [...] on JDBC sub-schema", and
     *       Avatica's own "Error while executing SQL \"...\":" atop it) carries no information
     *       of its own — the engine's real error (e.g. DuckDB's) is on the deepest cause;</li>
     *   <li>a stats aggregate (corr, regr_*, median, skewness, kurtosis, mad, quantile_cont,
     *       quantile_disc — see {@code DuckDBStatsFunctions}) that fails to push down to
     *       DuckDB is meant to fail with a clean {@code UnsupportedOperationException} from
     *       its {@code result()} stub, but when a second aggregate (e.g. {@code COUNT(*)})
     *       shares the same {@code EnumerableAggregate}, Calcite's generated code instead
     *       fails to *compile*, before that stub ever runs — surfacing a raw Janino
     *       "No applicable constructor/method" error naming the stub class/method instead.
     *       Both are the same underlying limitation (the aggregate's inputs span more than
     *       one govdata schema, so the join can't be pushed to a single DuckDB catalog);
     *       recognize the compile-failure shape too so it gets the same actionable message.</li>
     * </ul>
     */
    static String compactErrorMessage(Throwable e) {
        for (Throwable t = e; t != null; t = safeCause(t)) {
            String msg = t.getMessage();
            if (msg != null && msg.contains("No applicable constructor/method found")
                && msg.contains("DuckDBStatsFunctions$")) {
                return "A statistical aggregate (corr, regr_*, median, skewness, kurtosis, "
                    + "mad, quantile_cont, or quantile_disc) failed to push down to the "
                    + "DuckDB engine, which is the only place these run — likely because its "
                    + "inputs come from a join across two different schemas (each schema is "
                    + "its own DuckDB catalog, so the join can't be pushed down as one query). "
                    + "Use fetch_aligned_series to align the series first and compute the "
                    + "statistic there, or keep the corr()/regr_*() call within a single "
                    + "schema.";
            }
        }
        for (Throwable t = e; t != null; t = safeCause(t)) {
            String msg = t.getMessage();
            if (msg != null) {
                int expecting = msg.indexOf("Was expecting one of:");
                if (expecting >= 0) {
                    return truncateMessage(msg.substring(0, expecting).trim());
                }
            }
        }
        Throwable deepest = e;
        for (Throwable t = e; t != null; t = safeCause(t)) {
            if (t.getMessage() != null && !t.getMessage().isEmpty()) {
                deepest = t;
            }
        }
        String msg = deepest.getMessage();
        if (msg == null || msg.isEmpty()) {
            msg = e.getClass().getSimpleName();
        }
        return truncateMessage(msg);
    }

    /** {@code getCause()} on some exceptions returns itself; guards the cause-chain walk. */
    private static Throwable safeCause(Throwable t) {
        Throwable cause = t.getCause();
        return cause == t ? null : cause;
    }

    private static String truncateMessage(String msg) {
        return msg.length() > 600 ? msg.substring(0, 600) + "..." : msg;
    }

    private static int countRows(String json) {
        try {
            return MAPPER.readTree(json).size();
        } catch (Exception e) {
            return -1;
        }
    }

    // ── Tool implementations ──────────────────────────────────────────────────

    /** The effective set of schema names (env override, else the built-in default set). */
    private static java.util.Set<String> allowedSchemas() {
        String env = System.getenv("ASKAMERICA_SCHEMAS");
        String src = (env == null || env.trim().isEmpty()) ? DEFAULT_SCHEMAS : env;
        java.util.LinkedHashSet<String> out = new java.util.LinkedHashSet<>();
        for (String s : src.split(",")) {
            String t = s.trim().toLowerCase();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    /**
     * One connection scoped to all selected sources, for metadata (information_schema)
     * queries. Views only materialize when every schema they reference is selected, so a
     * single all-sources connection yields the complete, runtime-accurate catalog.
     */
    static Connection getCatalogConnection() throws Exception {
        return getSchemaConnection(String.join(",", allowedSchemas()));
    }

    /** Reduce a caller-supplied identifier to a safe [a-z0-9_] literal for meta queries. */
    private static String safeIdent(String s) {
        return s == null ? "" : s.replaceAll("[^A-Za-z0-9_]", "").toLowerCase();
    }

    private static String listSchemas() throws Exception {
        java.util.Set<String> allowed = allowedSchemas();
        ArrayNode arr = MAPPER.createArrayNode();
        java.util.Set<String> seen = new java.util.HashSet<>();
        try {
            Connection c = getCatalogConnection();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT schema_name, remarks FROM information_schema.schemata "
                     + "ORDER BY schema_name")) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (name == null) {
                        continue;
                    }
                    String lower = name.toLowerCase();
                    if (!allowed.contains(lower)) {
                        continue;
                    }
                    seen.add(lower);
                    arr.add(schemaEntry(lower, rs.getString(2)));
                }
            }
        } catch (Exception e) {
            // Do not fall through to the allow-list. Swallowing this reported a healthy
            // 25-schema catalog off a connection that had failed to open, so discovery
            // looked fine while every query, list_tables and resolve_geo call failed —
            // the static list is indistinguishable from a live one to the caller.
            log.println("[askamerica-mcp] list_schemas failed: " + e.getMessage());
            throw e;
        }
        // Guarantee every allowed schema appears even if information_schema didn't list it.
        for (String s : allowed) {
            if (!seen.contains(s)) {
                arr.add(schemaEntry(s, null));
            }
        }
        return arr.toString();
    }

    private static ObjectNode schemaEntry(String schema, String remarks) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("schema", schema);
        // Prefer the full authored description; information_schema truncates it to 80 chars.
        String desc = Catalog.schemaDescription(schema);
        if (desc == null || desc.isEmpty()) {
            desc = remarks;
        }
        if (desc != null && !desc.isEmpty()) {
            o.put("description", desc);
        }
        return o;
    }

    private static String searchCatalog(String query, int limit) {
        if (query == null || query.trim().isEmpty() || !Catalog.available()) {
            return "[]";
        }
        return Catalog.search(query.trim(), limit).toString();
    }

    private static String listTables(String schema) throws Exception {
        String s = safeIdent(schema);
        Connection c = getCatalogConnection();
        ArrayNode arr = MAPPER.createArrayNode();
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT table_name, table_type, remarks FROM information_schema.tables "
                 + "WHERE lower(table_schema) = '" + s + "' ORDER BY table_name")) {
            while (rs.next()) {
                String tname = rs.getString(1);
                ObjectNode o = MAPPER.createObjectNode();
                o.put("table", tname);
                o.put("type", rs.getString(2));
                // Prefer authored description; fill from REMARKS (covers runtime-only tables).
                String desc = Catalog.tableDescription(s, tname);
                if (desc == null || desc.isEmpty()) {
                    desc = rs.getString(3);
                }
                if (desc != null && !desc.isEmpty()) {
                    o.put("description", desc);
                }
                arr.add(o);
            }
        }
        return arr.toString();
    }

    private static String describeTable(String schema, String table) throws Exception {
        String s = safeIdent(schema);
        String t = safeIdent(table);
        Connection c = getCatalogConnection();

        ObjectNode out = MAPPER.createObjectNode();
        out.put("schema", s);
        out.put("table", t);

        // Table type + description (information_schema resolves both base tables and views).
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT table_type, remarks FROM information_schema.tables "
                 + "WHERE lower(table_schema) = '" + s + "' AND lower(table_name) = '" + t + "'")) {
            if (rs.next()) {
                out.put("type", rs.getString(1));
                String tdesc = Catalog.tableDescription(s, t);
                if (tdesc == null || tdesc.isEmpty()) {
                    tdesc = rs.getString(2);
                }
                if (tdesc != null && !tdesc.isEmpty()) {
                    out.put("description", tdesc);
                }
            }
        }

        // Columns — resolved by Calcite, so view row types come through correctly.
        ArrayNode cols = MAPPER.createArrayNode();
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT column_name, data_type, is_nullable, remarks "
                 + "FROM information_schema.columns "
                 + "WHERE lower(table_schema) = '" + s + "' AND lower(table_name) = '" + t + "' "
                 + "ORDER BY ordinal_position")) {
            while (rs.next()) {
                String cname = rs.getString(1);
                ObjectNode col = MAPPER.createObjectNode();
                col.put("name", cname);
                col.put("type", rs.getString(2));
                col.put("nullable", "YES".equalsIgnoreCase(rs.getString(3)));
                String cdesc = Catalog.columnDescription(s, t, cname);
                if (cdesc == null || cdesc.isEmpty()) {
                    cdesc = rs.getString(4);
                }
                if (cdesc != null && !cdesc.isEmpty()) {
                    col.put("description", cdesc);
                }
                cols.add(col);
            }
        }
        out.set("columns", cols);

        // Declared primary key. Without it a caller cannot tell a table's grain from its columns,
        // and several govdata tables are vintage-partitioned: geo.counties keys on
        // (county_fips, year) and holds one full copy of every county per TIGER vintage, so a join
        // on county_fips alone silently multiplies row counts by the number of years with no error
        // to notice. The schemas already declare these keys; reporting them here is what makes the
        // grain visible at the point a caller decides how to join.
        ArrayNode pk = MAPPER.createArrayNode();
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT k.column_name FROM information_schema.key_column_usage k "
                 + "JOIN information_schema.table_constraints tc "
                 + "  ON k.constraint_name = tc.constraint_name "
                 + " AND k.table_schema = tc.table_schema "
                 + " AND k.table_name = tc.table_name "
                 + "WHERE lower(k.table_schema) = '" + s + "' "
                 + "  AND lower(k.table_name) = '" + t + "' "
                 // Restated for tc, not redundant: each metadata table prunes from its own
                 // predicates, and without these the constraints scan walks every table in every
                 // schema to answer a question about one — the whole-catalog resolution that made
                 // describe_table both slow and breakable by an unrelated table.
                 + "  AND lower(tc.table_schema) = '" + s + "' "
                 + "  AND lower(tc.table_name) = '" + t + "' "
                 + "  AND tc.constraint_type = 'PRIMARY KEY' "
                 + "ORDER BY k.ordinal_position")) {
            while (rs.next()) {
                pk.add(rs.getString(1));
            }
        }
        if (pk.size() > 0) {
            out.set("primaryKey", pk);
        }

        // Declared year window, so an empty result outside coverage isn't read as a zero.
        // The observed window is measured out of band and attached once it lands, since
        // the declared range can run ahead of an in-progress backfill.
        ObjectNode cov = Catalog.coverage(s, t);
        if (cov == null) {
            // Views declare no dimensions, so nothing above knows their range — but the
            // resolved row type does, and a view over partitioned data still exposes the
            // year. Measuring is the only way to state coverage for these, so say only
            // what the probe finds and never imply a declared window exists.
            String yearCol = resolvedYearColumn(cols);
            if (yearCol != null) {
                cov = MAPPER.createObjectNode();
                cov.put("column", yearCol);
                cov.put("basis", "observed-only");
                cov.put("note",
                    "No declared coverage window — this table's range is not stated in the "
                    + "schema, so it can only be measured. Use the 'observed' block when "
                    + "present; status 'measuring' means the scan has not finished yet.");
            }
        }
        if (cov != null) {
            ObjectNode obs = IngestedYears.observed(s, t, cov.path("column").asText("year"));
            if (obs != null) {
                cov.set("observed", obs);
            }
            out.set("coverage", cov);
        }

        log.println("[askamerica-mcp] describe_table " + s + "." + t
            + " columns=" + cols.size() + " coverage=" + (cov != null));
        return out.toString();
    }

    /**
     * The year-like column in a resolved row type, or null. Restricted to integral types so
     * a probe never runs MIN/MAX over a date or over a string that merely happens to be
     * named "year".
     */
    private static String resolvedYearColumn(ArrayNode cols) {
        for (JsonNode c : cols) {
            String name = c.path("name").asText("");
            if (!"year".equalsIgnoreCase(name)) {
                continue;
            }
            String type = c.path("type").asText("").toUpperCase(java.util.Locale.ROOT);
            // A hive partition column surfaces as VARCHAR on most of these tables, so a
            // string year is as legitimate as an integer one. Anything else named "year"
            // (a DATE, a TIMESTAMP) is not a year bound and is left alone.
            if (type.contains("INT") || type.contains("DECIMAL") || type.contains("NUMERIC")
                    || type.contains("CHAR")) {
                return name;
            }
        }
        return null;
    }

    /**
     * Words Calcite's Oracle lex reserves that are also real schema, table, or column names
     * in this catalog. Used only to decide whether a token <em>in identifier position</em>
     * needs quoting — never to rewrite a keyword doing its grammatical job.
     */
    private static final java.util.Set<String> RESERVED_IDENTIFIERS =
        new java.util.HashSet<>(
            java.util.Arrays.asList(
            "ref", "year", "date", "time", "timestamp", "type", "value", "name", "status",
            "level", "key", "rank", "count", "order", "open", "close", "domain", "sequence",
            "start", "end", "position", "language", "size", "path", "source", "system",
            "user", "day", "month", "hour", "minute", "second", "range", "period", "measure"));

    /**
     * Quote reserved words used as identifiers, leaving reserved words used as keywords alone.
     *
     * <p>Only dot-adjacent tokens are rewritten, because those are the positions where a token
     * is unambiguously an identifier: the token before a {@code .} is a schema or table
     * qualifier, the token after a {@code .} is a table or column name. A bare {@code YEAR} is
     * left untouched — it may be {@code EXTRACT(YEAR FROM d)} or {@code ORDER BY}, and quoting
     * those breaks a query that would otherwise parse. String literals, quoted identifiers and
     * comments are skipped: the previous regex rewrote inside them, so
     * {@code WHERE note = 'see ref.table'} came out malformed.
     */
    static String quoteReservedIdentifiers(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }
        StringBuilder out = new StringBuilder(sql.length() + 32);
        int n = sql.length();
        int i = 0;
        boolean afterDot = false;
        while (i < n) {
            char ch = sql.charAt(i);
            if (ch == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int nl = sql.indexOf('\n', i);
                int stop = nl < 0 ? n : nl;
                out.append(sql, i, stop);
                i = stop;
                continue;
            }
            if (ch == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int close = sql.indexOf("*/", i + 2);
                int stop = close < 0 ? n : close + 2;
                out.append(sql, i, stop);
                i = stop;
                continue;
            }
            if (ch == '\'' || ch == '"') {
                int j = i + 1;
                while (j < n) {
                    if (sql.charAt(j) == ch) {
                        if (j + 1 < n && sql.charAt(j + 1) == ch) {
                            j += 2;      // doubled quote is an escape, keep scanning
                            continue;
                        }
                        j++;
                        break;
                    }
                    j++;
                }
                out.append(sql, i, Math.min(j, n));
                i = Math.min(j, n);
                afterDot = false;
                continue;
            }
            if (Character.isLetter(ch) || ch == '_') {
                int j = i;
                while (j < n
                        && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) {
                    j++;
                }
                String word = sql.substring(i, j);
                int k = j;
                while (k < n && Character.isWhitespace(sql.charAt(k))) {
                    k++;
                }
                boolean beforeDot = k < n && sql.charAt(k) == '.';
                boolean isCall = k < n && sql.charAt(k) == '(';
                if (!isCall && (afterDot || beforeDot)
                        && RESERVED_IDENTIFIERS.contains(
                            word.toLowerCase(java.util.Locale.ROOT))) {
                    out.append('"').append(word).append('"');
                } else {
                    out.append(word);
                }
                i = j;
                afterDot = false;
                continue;
            }
            out.append(ch);
            if (!Character.isWhitespace(ch)) {
                // A '.' between identifiers marks the next token as an identifier. Guard
                // against decimals: a '.' preceded by a digit belongs to a number literal.
                afterDot = ch == '.' && !(i > 0 && Character.isDigit(sql.charAt(i - 1)));
            }
            i++;
        }
        return out.toString();
    }

    // Extract the first govdata schema name from a SQL query (e.g. "FROM sec.filings" → "sec").
    private static final java.util.regex.Pattern SQL_SCHEMA_PAT =
        java.util.regex.Pattern.compile(
            "(?i)\\b(?:FROM|JOIN)\\s+(?:\")?([a-zA-Z_][a-zA-Z0-9_]*)(?:\")?\\.");
    private static final java.util.Set<String> META_SCHEMAS =
        new java.util.HashSet<>(
            java.util.Arrays.asList(
            "information_schema", "pg_catalog", "metadata"));

    /** First non-meta schema qualifier in the SQL, or null when it names none. Used only
     *  to reject unqualified SQL with a useful message — never to scope the connection. */
    private static String extractSchema(String sql) {
        java.util.regex.Matcher m = SQL_SCHEMA_PAT.matcher(sql);
        while (m.find()) {
            String s = m.group(1).toLowerCase();
            if (!META_SCHEMAS.contains(s)) {
                return s;
            }
        }
        return null;
    }

    private static String query(String sql, int limit) throws Exception {
        // No pre-emptive schema check. extractSchema() plays no part in choosing the
        // connection — runSqlOn always uses the all-schemas catalog connection — so
        // rejecting up front only refused SQL that would have run:
        //   SELECT 1+1                              no FROM at all
        //   SELECT ... FROM information_schema.columns   meta schemas are excluded by design
        //   DESCRIBE geo.state_ref                  no FROM/JOIN for the pattern to match
        // All three work on that connection; the guard was the only thing stopping them.
        //
        // The advice it carried is still worth giving, so it is attached to the failure
        // instead — where it is a hint about a real error rather than a refusal to try.
        try {
            return runSqlOn(sql, limit);
        } catch (Exception e) {
            String msg = e.getMessage();
            if (extractSchema(sql) == null && msg != null && looksLikeUnresolvedObject(msg)) {
                throw new RuntimeException(msg
                    + " — tables must be referenced as schema.table, e.g. "
                    + "SELECT * FROM sec.filing_metadata. Call list_schemas to see the "
                    + "available schemas.", e);
            }
            throw e;
        }
    }

    /** True when a SQL error reads like an unqualified or unknown table/column reference. */
    private static boolean looksLikeUnresolvedObject(String msg) {
        String m = msg.toLowerCase();
        return m.contains("not found")
            || m.contains("object '")
            || m.contains("table '")
            || m.contains("unknown");
    }

    /** Default seconds a single query may run before it is aborted. */
    private static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 180;

    /**
     * Per-query time bound, overridable with ASKAMERICA_QUERY_TIMEOUT_SECONDS.
     *
     * <p>Deliberately generous. A legitimate cross-schema aggregate over several years of
     * data can take minutes, and this is a ceiling on runaway work, not a latency target —
     * cutting it to something like 30s would start failing real analytical queries. Zero or
     * negative disables the bound, for a caller who genuinely wants to wait.
     */
    private static int queryTimeoutSeconds() {
        String raw = System.getProperty("askamerica.query.timeout.seconds");
        if (raw == null || raw.isEmpty()) {
            raw = System.getenv("ASKAMERICA_QUERY_TIMEOUT_SECONDS");
        }
        if (raw != null && !raw.isEmpty()) {
            try {
                int v = Integer.parseInt(raw.trim());
                return Math.max(0, v);
            } catch (NumberFormatException e) {
                log.println("[askamerica-mcp] bad query timeout '" + raw + "', using "
                    + DEFAULT_QUERY_TIMEOUT_SECONDS + "s");
            }
        }
        return DEFAULT_QUERY_TIMEOUT_SECONDS;
    }

    /** Execute SQL on the single all-schemas connection, applying the same reserved-word
     *  quoting and default row-limit as query(). Every tool runs here: a narrower source
     *  set would mount a second connection and re-open all of that schema's Iceberg
     *  metadata, so resolve_geo and fetch_aligned_series share this one too. */
    private static String runSqlOn(String sql, int limit) throws Exception {
        String effective = quoteReservedIdentifiers(sql);
        String lower = effective.toLowerCase();
        if (!lower.contains("fetch first") && !lower.contains(" limit ")) {
            effective = effective.replaceAll(";\\s*$", "")
                + " FETCH FIRST " + limit + " ROWS ONLY";
        }
        Connection c = getCatalogConnection();
        Statement stmt = c.createStatement();
        // Without this a query runs unbounded, and because the stdio loop is strictly
        // serial that freezes the whole server, not just the one call: a tool doing no I/O
        // at all appears to hang because it is queued behind the query that is still
        // running. Verified enforced on this path -- a 5s limit aborted a three-way cross
        // join at 5996ms -- so it is a real bound rather than an advisory one.
        stmt.setQueryTimeout(queryTimeoutSeconds());
        try {
            ResultSet rs = stmt.executeQuery(effective);
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            String[] names = new String[cols];
            for (int i = 0; i < cols; i++) {
                names[i] = meta.getColumnName(i + 1);
            }
            int[] sqlTypes = new int[cols];
            for (int i = 0; i < cols; i++) {
                sqlTypes[i] = meta.getColumnType(i + 1);
            }
            ArrayNode arr = MAPPER.createArrayNode();
            while (rs.next()) {
                ObjectNode row = MAPPER.createObjectNode();
                for (int i = 0; i < cols; i++) {
                    int t = sqlTypes[i];
                    if (t == java.sql.Types.INTEGER || t == java.sql.Types.SMALLINT
                            || t == java.sql.Types.TINYINT) {
                        int v = rs.getInt(i + 1);
                        if (rs.wasNull()) row.putNull(names[i]); else row.put(names[i], v);
                    } else if (t == java.sql.Types.BIGINT) {
                        long v = rs.getLong(i + 1);
                        if (rs.wasNull()) row.putNull(names[i]); else row.put(names[i], v);
                    } else if (t == java.sql.Types.FLOAT || t == java.sql.Types.REAL) {
                        float v = rs.getFloat(i + 1);
                        if (rs.wasNull()) row.putNull(names[i]); else row.put(names[i], v);
                    } else if (t == java.sql.Types.DOUBLE || t == java.sql.Types.NUMERIC
                            || t == java.sql.Types.DECIMAL) {
                        double v = rs.getDouble(i + 1);
                        if (rs.wasNull()) row.putNull(names[i]); else row.put(names[i], v);
                    } else if (t == java.sql.Types.BOOLEAN || t == java.sql.Types.BIT) {
                        boolean v = rs.getBoolean(i + 1);
                        if (rs.wasNull()) row.putNull(names[i]); else row.put(names[i], v);
                    } else {
                        Object val = rs.getObject(i + 1);
                        if (rs.wasNull() || val == null) row.putNull(names[i]);
                        else row.put(names[i], val.toString());
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

    // ── resolve_geo ──────────────────────────────────────────────────────────

    private static String sqlStr(String v) {
        return "'" + v.replace("'", "''") + "'";
    }

    private static String buildResolveSql(String term, String level, String withinState, int limit) {
        String t = term == null ? "" : term.trim();
        if (t.isEmpty()) {
            throw new IllegalArgumentException("term must be non-empty");
        }
        String lit = sqlStr(t);
        String like = sqlStr("%" + t.toLowerCase() + "%");
        int cap = Math.min(Math.max(1, limit), 500);
        String lvl = (level == null || level.isEmpty()) ? "state" : level;
        switch (lvl) {
            case "state":
                // geo.state_ref is the canonical state reference (state_fips, state_abbr,
                // state_name) — the FK target for state joins across all govdata schemas.
                //
                // DISTINCT, matching the county branch. state_ref declares state_abbr as
                // its primary key and state_fips/state_name as unique, so in a healthy
                // table this changes nothing. It matters when the table is not healthy:
                // a duplicated load once made term="NC" return the same row seventeen
                // times, and the county branch's DISTINCT is why the same fault never
                // showed there.
                return "SELECT DISTINCT state_fips, state_abbr, state_name FROM geo.state_ref "
                    + "WHERE lower(state_abbr) = lower(" + lit + ") "
                    + "OR state_fips = " + lit + " "
                    + "OR lower(state_name) LIKE " + like + " "
                    + "ORDER BY state_fips FETCH FIRST " + cap + " ROWS ONLY";
            case "county": {
                // geo.counties carries the bare name in county_name ("Mecklenburg") and the
                // fuller form in county_code ("Mecklenburg County"), so matching only
                // county_name meant the name a caller is most likely to type — the one that
                // appears on a map or in a citation — found nothing while the bare word
                // worked. Both are matched now.
                String where = "lower(county_name) LIKE " + like
                    + " OR lower(county_code) LIKE " + like
                    + " OR county_fips = " + lit;
                if (withinState != null && !withinState.isEmpty()) {
                    where = "(" + where + ") AND state_fips = " + sqlStr(withinState);
                }
                return "SELECT DISTINCT county_fips, state_fips, county_name FROM geo.counties "
                    + "WHERE " + where + " ORDER BY state_fips, county_name "
                    + "FETCH FIRST " + cap + " ROWS ONLY";
            }
            case "zcta":
                return "SELECT zcta, latitude, longitude FROM geo.zcta_ref "
                    + "WHERE zcta = " + lit + " FETCH FIRST " + cap + " ROWS ONLY";
            default:
                throw new IllegalArgumentException(
                    "level must be 'state', 'county', or 'zcta'; got " + level);
        }
    }

    private static String resolveGeo(String term, String level, String withinState) throws Exception {
        return runSqlOn(buildResolveSql(term, level, withinState, 50), 50);
    }

    // ── fetch_aligned_series ─────────────────────────────────────────────────

    private static final java.util.Set<String> TIME_GRAINS =
        new java.util.HashSet<>(java.util.Arrays.asList("day", "month", "quarter", "year"));
    private static final java.util.Set<String> GEO_LEVELS =
        new java.util.HashSet<>(java.util.Arrays.asList("state", "county", "geo"));
    private static final java.util.Set<String> ALLOWED_AGG =
        new java.util.HashSet<>(java.util.Arrays.asList(
            "avg", "sum", "min", "max", "count", "median", "last", "first"));

    private static String specText(JsonNode spec, String field) {
        JsonNode v = spec.get(field);
        return (v != null && !v.isNull()) ? v.asText() : null;
    }

    private static String checkIdent(String name, String what) {
        if (name == null || name.isEmpty() || name.length() > 63) {
            throw new IllegalArgumentException(what + " must be a simple identifier");
        }
        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (!(Character.isLetterOrDigit(ch) || ch == '_')) {
                throw new IllegalArgumentException(
                    what + " must be letters/digits/underscore; got " + name);
            }
        }
        return name;
    }

    /** Validates {@code col} is a simple identifier, then double-quotes it. Every key column
     *  this method feeds (time_col, year_col, period_col, quarter_col, year_only_col, geo_col)
     *  is documented as a plain column reference, so quoting unconditionally — rather than only
     *  when it happens to collide with {@link #RESERVED_IDENTIFIERS} — needs no reserved-word
     *  detection and is a no-op for an already-safe name. Lower-cased first so a quoted reference
     *  resolves the same column an unquoted one would under this connection's TO_LOWER casing. */
    private static String quoteCol(String col, String what) {
        return "\"" + checkIdent(col, what).toLowerCase(java.util.Locale.ROOT) + "\"";
    }

    static String keyExpr(JsonNode spec, String on, String label) {
        if (TIME_GRAINS.contains(on)) {
            // FLOOR(x TO <unit>), not date_trunc/make_date: both are DuckDB-native, not Calcite
            // operators, and Calcite's JdbcProjectRule unconditionally refuses to push a Project
            // down to a JDBC source at all if it contains a user-defined function — so even
            // registering date_trunc/make_date as schema UDFs (matching the corr/regr_* pattern)
            // can never push down here, since this key expression always sits inside a Project
            // (the GROUP BY key), never inside an Aggregate the way corr/regr_* do. FLOOR(...TO
            // unit), CAST, ||, CASE and ANSI SUBSTRING are all genuine Calcite-standard
            // operators — never flagged user-defined — so they push through JdbcProjectRule, and
            // Calcite's own dialect-aware unparse for FLOOR expands it into DuckDB's native
            // date_trunc call at SQL-generation time.
            String unit = on.toUpperCase(java.util.Locale.ROOT);
            String timeCol = specText(spec, "time_col");
            if (timeCol != null) {
                timeCol = quoteCol(timeCol, "series " + label + ".time_col");
                // CAST, not the raw column: most date-bearing columns in this warehouse are
                // VARCHAR holding ISO-8601 text, and FLOOR on a VARCHAR fails outright — so
                // every time-grain alignment failed while the geo path, which passes its key
                // column through untouched, worked. Casting covers both typings without the tool
                // having to inspect types: DATE -> DATE is a no-op, and ISO text parses.
                return "FLOOR(CAST(" + timeCol + " AS DATE) TO " + unit + ")";
            }
            String yearCol = specText(spec, "year_col");
            String periodCol = specText(spec, "period_col");
            if (yearCol != null && periodCol != null) {
                yearCol = quoteCol(yearCol, "series " + label + ".year_col");
                periodCol = quoteCol(periodCol, "series " + label + ".period_col");
                // year_col is just as often VARCHAR as INTEGER; period_col is BLS-style
                // "M01".."M12", so its month digits (SUBSTRING from position 2) are already
                // zero-padded to two characters. ANSI SUBSTRING(x FROM n), not substr(x, n): the
                // comma-call substr(...) form isn't registered under this connection's fun
                // libraries (standard,postgresql,spatial) at all, under any arity.
                return "FLOOR(CAST(CAST(CAST(" + yearCol + " AS INTEGER) AS VARCHAR) || '-'"
                    + " || SUBSTRING(" + periodCol + " FROM 2) || '-01' AS DATE) TO " + unit + ")";
            }
            String quarterCol = specText(spec, "quarter_col");
            if (quarterCol != null) {
                quarterCol = quoteCol(quarterCol, "series " + label + ".quarter_col");
                // quarter_col is BEA-style "2023Q1"; map the quarter digit to its first month
                // directly rather than computing then re-padding an arithmetic result.
                String monthOfQuarter = "CASE SUBSTRING(" + quarterCol + " FROM 6 FOR 1) "
                    + "WHEN '1' THEN '01' WHEN '2' THEN '04' "
                    + "WHEN '3' THEN '07' WHEN '4' THEN '10' END";
                return "FLOOR(CAST(SUBSTRING(" + quarterCol + " FROM 1 FOR 4) || '-' || "
                    + monthOfQuarter + " || '-01' AS DATE) TO " + unit + ")";
            }
            String yearOnly = specText(spec, "year_only_col");
            if (yearOnly != null) {
                yearOnly = quoteCol(yearOnly, "series " + label + ".year_only_col");
                return "FLOOR(CAST(CAST(CAST(" + yearOnly + " AS INTEGER) AS VARCHAR)"
                    + " || '-01-01' AS DATE) TO " + unit + ")";
            }
            throw new IllegalArgumentException("series " + label + ": for on=" + on
                + " give time_col | (year_col & period_col) | quarter_col | year_only_col");
        }
        if (GEO_LEVELS.contains(on)) {
            String geoCol = specText(spec, "geo_col");
            if (geoCol == null) {
                throw new IllegalArgumentException(
                    "series " + label + ": for on=" + on + " give geo_col");
            }
            String col = quoteCol(geoCol, "series " + label + ".geo_col");
            if (on.equals("state")) {
                // Normalize to canonical state_fips via a LEFT JOIN against geo.state_ref (added
                // to this series' FROM clause by stateGeoJoin below): two series whose geo_col
                // happen to use different conventions (one a 2-digit FIPS code, the other a USPS
                // abbreviation like "CA") otherwise produce a disjoint FULL OUTER JOIN, where
                // every row is half-NULL because the raw key values never compare equal. Both
                // forms resolve to the same state_fips here, so the join actually aligns.
                // COALESCE falls back to the raw (cast) value so a code state_ref doesn't
                // recognize (e.g. a territory) still gets a join key instead of being dropped —
                // matching this method's existing behavior of trusting the caller's data.
                return "COALESCE(gsr.state_fips, CAST(" + col + " AS VARCHAR))";
            }
            return col;
        }
        throw new IllegalArgumentException("on must be a time grain or geo level; got " + on);
    }

    /**
     * The {@code LEFT JOIN geo.state_ref ...} fragment {@link #keyExpr} needs already spliced
     * into a series' {@code FROM} clause when {@code on} is {@code state} — a correlated
     * subquery can't be used instead because Calcite's validator does not resolve a subquery's
     * correlation back to the outer FROM columns when that subquery is repeated in GROUP BY
     * (the same key expression is deliberately repeated in both SELECT and GROUP BY; see
     * {@link #buildAlignedSql}). Returns {@code null} for any other grain.
     */
    static String stateGeoJoin(JsonNode spec, String on, String label) {
        if (!on.equals("state")) {
            return null;
        }
        String geoCol = specText(spec, "geo_col");
        if (geoCol == null) {
            throw new IllegalArgumentException(
                "series " + label + ": for on=" + on + " give geo_col");
        }
        String col = quoteCol(geoCol, "series " + label + ".geo_col");
        String asVarchar = "CAST(" + col + " AS VARCHAR)";
        return " LEFT JOIN geo.state_ref gsr ON gsr.state_fips = " + asVarchar
            + " OR gsr.state_abbr = UPPER(" + asVarchar + ")";
    }

    private static String buildAlignedSql(JsonNode series, String on, String stat) {
        if (series == null || !series.isArray() || series.size() < 1) {
            throw new IllegalArgumentException("series must be a non-empty array of spec objects");
        }
        if (stat != null && !stat.equals("corr") && !stat.equals("regr")) {
            throw new IllegalArgumentException("stat must be 'corr', 'regr', or null; got " + stat);
        }
        if (stat != null && series.size() < 2) {
            throw new IllegalArgumentException("stat=" + stat + " needs at least two series");
        }
        java.util.List<String> ctes = new java.util.ArrayList<>();
        java.util.List<String> cols = new java.util.ArrayList<>();
        for (int i = 0; i < series.size(); i++) {
            JsonNode spec = series.get(i);
            String table = specText(spec, "table");
            String value = specText(spec, "value");
            if (table == null || value == null) {
                throw new IllegalArgumentException("series[" + i + "] needs 'table' and 'value'");
            }
            String name = specText(spec, "name");
            name = checkIdent(name != null ? name : ("s" + i), "series[" + i + "].name");
            String agg = specText(spec, "agg");
            if (agg == null) {
                agg = "avg";
            }
            if (!ALLOWED_AGG.contains(agg)) {
                throw new IllegalArgumentException("series[" + i + "].agg " + agg + " not allowed");
            }
            String key = keyExpr(spec, on, name);
            String geoJoin = stateGeoJoin(spec, on, name);
            String tableFrom = table + (geoJoin != null ? geoJoin : "");
            String where = specText(spec, "where");
            String whereClause = (where != null && !where.isEmpty()) ? (" WHERE " + where) : "";
            // Repeat the key expression in GROUP BY — this Calcite dialect rejects
            // ordinal GROUP BY (GROUP BY 1).
            ctes.add("s" + i + " AS (SELECT " + key + " AS k, " + agg + "(" + value + ") AS " + name
                + " FROM " + tableFrom + whereClause + " GROUP BY " + key + ")");
            cols.add(name);
        }
        StringBuilder from = new StringBuilder("s0");
        java.util.List<String> seen = new java.util.ArrayList<>();
        seen.add("s0.k");
        for (int i = 1; i < series.size(); i++) {
            String left = seen.size() == 1 ? seen.get(0)
                : ("COALESCE(" + String.join(", ", seen) + ")");
            from.append(" FULL OUTER JOIN s").append(i).append(" ON ")
                .append(left).append(" = s").append(i).append(".k");
            seen.add("s" + i + ".k");
        }
        String keySel = (seen.size() == 1 ? seen.get(0)
            : ("COALESCE(" + String.join(", ", seen) + ")")) + " AS key";
        String withClause = "WITH " + String.join(", ", ctes);
        if (stat != null) {
            String a = cols.get(0);
            String b = cols.get(1);
            String expr = stat.equals("corr")
                ? ("corr(" + a + ", " + b + ") AS r, regr_count(" + a + ", " + b + ") AS n")
                : ("regr_slope(" + b + ", " + a + ") AS slope, regr_intercept(" + b + ", " + a
                    + ") AS intercept, regr_r2(" + b + ", " + a + ") AS r2, regr_count("
                    + b + ", " + a + ") AS n");
            return withClause + ", aligned AS (SELECT " + keySel + ", " + String.join(", ", cols)
                + " FROM " + from + ") SELECT " + expr + " FROM aligned";
        }
        // no stat: aligned frame (runSqlOn appends the row limit)
        return withClause + " SELECT " + keySel + ", " + String.join(", ", cols)
            + " FROM " + from + " ORDER BY key";
    }

    /** Comma-joined, canonical (sorted) union of the schemas referenced by the series. */
    private static String schemasOf(JsonNode series) {
        java.util.TreeSet<String> set = new java.util.TreeSet<>();
        for (int i = 0; i < series.size(); i++) {
            String table = specText(series.get(i), "table");
            if (table != null) {
                int dot = table.indexOf('.');
                if (dot > 0) {
                    set.add(table.substring(0, dot).toLowerCase());
                }
            }
        }
        if (set.isEmpty()) {
            throw new IllegalArgumentException("no schema found in series tables");
        }
        return String.join(",", set);
    }

    private static String fetchAlignedSeries(JsonNode series, String on, String stat, int limit)
            throws Exception {
        String sql = buildAlignedSql(series, on, stat);
        // Validates that every series names a schema-qualified table; the result is not used
        // to scope the connection, which is always the all-schemas one.
        schemasOf(series);
        // stat returns a single scalar row; a frame gets the caller's limit.
        return runSqlOn(sql, stat != null ? 5 : limit);
    }


    /** Base for the AskAmerica API — system property, then env, then production. */
    private static String apiBase() {
        String p = System.getProperty("askamerica.api.url");
        if (p != null && !p.isEmpty()) {
            return p;
        }
        String e = System.getenv("ASKAMERICA_API_URL");
        return (e != null && !e.isEmpty()) ? e : "https://api.askamerica.ai";
    }

    /**
     * Shared stamp the issues endpoint expects. A filter, not a secret — the endpoint is
     * public so a customer whose key is the broken thing can still report it.
     */
    private static final String ISSUE_STAMP = "askamerica-mcp";

    /**
     * Files a customer issue via POST /v1/issues, which records it in D1.
     *
     * <p>This used to write a parquet file straight into s3://govdata-parquet-v1/issues/
     * with the caller's own credentials. Those are read-only on the data bucket, so it
     * always returned 403 — and making it succeed would have meant giving every MCP client
     * write access to the production data bucket in order to file a bug report.
     *
     * <p>Unlike metering, the result is awaited and surfaced. The user has just written up
     * a report; telling them it was filed when it was not is worse than telling them to
     * retry.
     */
    private static String reportIssue(String subject, String body) {
        HttpURLConnection c = null;
        try {
            String payload = "{"
                + "\"stamp\":" + jsonStr(ISSUE_STAMP) + ","
                + "\"build\":" + jsonStr(BUILD_ID) + ","
                + "\"session_id\":" + jsonStr(SESSION_ID) + ","
                + "\"reported_at\":" + jsonStr(java.time.Instant.now().toString()) + ","
                + "\"subject\":" + jsonStr(subject) + ","
                + "\"body\":" + jsonStr(body)
                + "}";

            URL url = java.net.URI.create(apiBase() + "/v1/issues").toURL();
            c = (HttpURLConnection) url.openConnection();
            c.setRequestMethod("POST");
            c.setConnectTimeout(10000);
            c.setReadTimeout(15000);
            c.setDoOutput(true);
            c.setRequestProperty("Content-Type", "application/json");
            // Optional: lets the server attribute the report. A missing or rejected key
            // must never block filing, so this is only ever added when present.
            String apiKey = UsageMetering.resolveApiKey(null);
            if (apiKey != null && !apiKey.isEmpty()) {
                c.setRequestProperty("X-API-Key", apiKey);
            }
            OutputStream os = c.getOutputStream();
            try {
                os.write(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } finally {
                os.close();
            }

            int code = c.getResponseCode();
            if (code >= 200 && code < 300) {
                log.println("[askamerica-mcp] report_issue filed (HTTP " + code + ")");
                return "Issue recorded. Subject: " + subject;
            }
            log.println("[askamerica-mcp] report_issue rejected: HTTP " + code);
            return "Could not record the issue (HTTP " + code
                + "). Nothing was filed — please retry, or report it at askamerica.ai.";
        } catch (Exception e) {
            log.println("[askamerica-mcp] report_issue error: " + e.getMessage());
            return "Could not record the issue: " + e.getMessage()
                + ". Nothing was filed — please retry, or report it at askamerica.ai.";
        } finally {
            if (c != null) {
                c.disconnect();
            }
        }
    }

    /** Minimal JSON string encoder — the launcher classpath has no JSON library. */
    private static String jsonStr(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(s.length() + 16).append('"');
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (ch < 0x20) {
                        sb.append(String.format("\\u%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
            }
        }
        return sb.append('"').toString();
    }

    private static String setTelemetry(boolean enabled) {
        telemetryOptIn = enabled;
        try {
            java.io.File dir =
                new java.io.File(System.getProperty("user.home"), ".askamerica");
            if (!dir.exists()) {
                dir.mkdirs();
            }
            java.io.File f = new java.io.File(dir, "telemetry.json");
            MAPPER.writeValue(f, java.util.Collections.singletonMap("optIn", enabled));
        } catch (Exception e) {
            log.println("[askamerica-mcp] set_telemetry persist failed: " + e.getMessage());
        }
        return enabled
            ? "Telemetry enabled. Anonymous tool-call metrics will be shared."
            : "Telemetry disabled. No data will be shared.";
    }

    private static boolean loadTelemetryOptIn() {
        try {
            java.io.File f =
                new java.io.File(System.getProperty("user.home"), ".askamerica/telemetry.json");
            if (!f.exists()) {
                return true;
            }
            JsonNode node = MAPPER.readTree(f);
            return node.path("optIn").asBoolean(true);
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * Runaway guard. Telemetry is already opt-in, and a normal session sends tens of
     * events, so this is not a volume control — it is a ceiling so a client stuck in a
     * loop cannot turn one session into unbounded requests. Failures are exempt: an
     * error is the event worth having, and a session that has gone wrong is exactly when
     * the cap would otherwise start discarding the useful records.
     */
    private static final int TELEMETRY_SESSION_CAP = 2000;
    private static final java.util.concurrent.atomic.AtomicInteger TELEMETRY_SENT =
        new java.util.concurrent.atomic.AtomicInteger();

    /**
     * Records one tool call, both to the local log and to POST /v1/telemetry.
     *
     * <p>This used to write a parquet file into s3://govdata-parquet-v1/telemetry/ with
     * the caller's read-only credentials, so every write 403'd — silently, because the
     * only report was a stderr line nobody reads. The local line is now written first and
     * unconditionally, so the record survives even when the network leg fails.
     *
     * <p>Server-side this lands in Analytics Engine, not D1: it is high-cardinality
     * time-series written per tool call, which is what AE is for.
     */
    private static void recordTelemetry(String tool, long durationMs, int rowCount,
                                         boolean success, String querySql, String errorMsg) {
        // Local first, and never conditional on the upload: this is the copy that is
        // guaranteed to exist.
        log.println("[askamerica-mcp] telemetry tool=" + tool
            + " success=" + success
            + " duration_ms=" + durationMs
            + " rows=" + rowCount
            + " session=" + SESSION_ID
            + (errorMsg != null && !errorMsg.isEmpty()
                ? " error=" + errorMsg.replace('\n', ' ') : ""));

        if (success && TELEMETRY_SENT.get() >= TELEMETRY_SESSION_CAP) {
            return;
        }
        TELEMETRY_SENT.incrementAndGet();

        HttpURLConnection c = null;
        try {
            String payload = "{"
                + "\"stamp\":" + jsonStr(ISSUE_STAMP) + ","
                + "\"recorded_at\":" + jsonStr(java.time.Instant.now().toString()) + ","
                + "\"session_id\":" + jsonStr(SESSION_ID) + ","
                + "\"build\":" + jsonStr(BUILD_ID) + ","
                + "\"tool\":" + jsonStr(tool) + ","
                + "\"duration_ms\":" + durationMs + ","
                + "\"row_count\":" + rowCount + ","
                + "\"success\":" + success + ","
                + "\"query_sql\":" + jsonStr(querySql) + ","
                + "\"error_msg\":" + jsonStr(errorMsg)
                + "}";

            URL url = java.net.URI.create(apiBase() + "/v1/telemetry").toURL();
            c = (HttpURLConnection) url.openConnection();
            c.setRequestMethod("POST");
            c.setConnectTimeout(5000);
            c.setReadTimeout(5000);
            c.setDoOutput(true);
            c.setRequestProperty("Content-Type", "application/json");
            String apiKey = UsageMetering.resolveApiKey(null);
            if (apiKey != null && !apiKey.isEmpty()) {
                c.setRequestProperty("X-API-Key", apiKey);
            }
            OutputStream os = c.getOutputStream();
            try {
                os.write(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } finally {
                os.close();
            }
            c.getResponseCode(); // drives the request; the client does not act on it
        } catch (Exception e) {
            // Best-effort upload. The local line above already holds the record, so this
            // is logged once and never surfaced to the tool caller.
            log.println("[askamerica-mcp] telemetry upload failed: " + e.getMessage());
        } finally {
            if (c != null) {
                c.disconnect();
            }
        }
    }


    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void suppressFrameworkLogging() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.calcite.adapter.govdata", "info");
        System.setProperty("log4j.rootLogger", "ERROR");

        // The shaded jar binds SLF4J to log4j2, so the levels this method cares about come from
        // the bundled log4j2.xml (root WARN; govdata and file at INFO) rather than from anything
        // set here: log4j.rootLogger above is a log4j 1.x property that its own LogManager only
        // reads from a properties file, and the logback branch below finds no logback at all. The
        // one thing that must hold regardless of binding is that no appender writes to stdout —
        // that is the JSON-RPC channel — which is why System.out was already redirected to stderr
        // before this method runs, and why the bundled configuration targets SYSTEM_ERR.
        // Kept for the case where a caller supplies logback on the classpath instead.
        //
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
            Object infoLevel  = levelClass.getField("INFO").get(null);

            // Set root logger to ERROR; govdata adapter to INFO for init diagnostics.
            Object rootLogger = contextClass.getMethod("getLogger", String.class)
                .invoke(context, "ROOT");
            rootLogger.getClass().getMethod("setLevel", levelClass)
                .invoke(rootLogger, errorLevel);

            Object govdataLogger = contextClass.getMethod("getLogger", String.class)
                .invoke(context, "org.apache.calcite.adapter.govdata");
            govdataLogger.getClass().getMethod("setLevel", levelClass)
                .invoke(govdataLogger, infoLevel);

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
