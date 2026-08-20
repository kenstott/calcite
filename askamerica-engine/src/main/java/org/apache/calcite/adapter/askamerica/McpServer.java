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
import org.apache.calcite.sql.parser.SqlParser;

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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    /** Tool definitions, built on first use by {@link #toolDefs()}. */
    private static volatile ArrayNode TOOL_DEFS;

    private static final int DEFAULT_LIMIT = 500;
    private static final int MAX_LIMIT = 5000;

    // Random session ID generated once per process — not stored, not user-identifying.
    private static final String SESSION_ID =
        java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 12);

    // Telemetry opt-in state loaded from ~/.askamerica/telemetry.json, refreshed on set.
    private static volatile boolean telemetryOptIn = loadTelemetryOptIn();

    static final String DEFAULT_SCHEMAS =
        "sec,geo,econ,census,crime,weather,ref,fec,"
        + "fedregister,officials,cyber_vuln,cyber_threat,energy,health,edu,econ_reference,"
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
    // Epoch millis each cached connection was opened. isValid(5) only catches a DEAD
    // connection — a live one is handed out forever even though the Iceberg tables it
    // was built against are read once at connection-open and never re-resolved (a
    // schema/table build is a one-time construction, not a per-query lookup). Observed:
    // a 'sec' connection opened hours before a same-day R2 backfill kept serving the
    // pre-backfill row counts indefinitely after the backfill completed, with no error
    // and no way to tell from the response that the answer was stale. This TTL forces a
    // periodic reconnect (which rebuilds the schema from current Iceberg state) so
    // staleness is bounded instead of open-ended.
    private static final ConcurrentHashMap<String, Long> schemaConnOpenedAtMillis =
        new ConcurrentHashMap<>();
    private static final long SCHEMA_CONN_TTL_MILLIS =
        TimeUnit.MINUTES.toMillis(resolveSchemaConnTtlMinutes());

    private static long resolveSchemaConnTtlMinutes() {
        String raw = System.getenv("ASKAMERICA_SCHEMA_CONN_TTL_MINUTES");
        return (raw == null || raw.isEmpty()) ? 30L : Long.parseLong(raw);
    }

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

        configureQueryEmbedder();

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
                // Prompt templates are assembled from constants and touch no schema, so they
                // stay answerable while a query runs, like tools/list above.
                case "prompts/list":     return result(id, QuestionGuidance.promptsList());
                case "prompts/get":      return handlePromptsGet(id, params);
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
        // Prompts are the opt-in surface for the good-question templates. Client support for
        // them is uneven, which is why the same teaching also lives in the tool descriptions
        // — those every capable client reads. This is the bonus channel, not the primary one.
        capabilities.set("prompts", MAPPER.createObjectNode());

        ObjectNode body = MAPPER.createObjectNode();
        body.put("protocolVersion", "2024-11-05");
        body.set("capabilities", capabilities);
        body.set("serverInfo", serverInfo);
        body.put("instructions",
            "Query US government data using PostgreSQL-compatible SQL. "
            + "Schemas: sec (SEC filings/XBRL), geo (TIGER/FIPS), "
            + "econ (BLS/BEA), census (ACS), crime (FBI UCR), "
            + "weather (NOAA GHCND), ref (NAICS/SIC), fec (campaign finance), "
            + "fedregister, officials (Congress.gov members/nominations, FJC judges), "
            + "cyber_vuln (NVD CVEs), cyber_threat (CISA KEV), "
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
            + "corr()/regr_*() only handle ONE predictor. For more than one predictor, a "
            + "suspected instrumented/endogenous relationship, a treatment-vs-control policy "
            + "comparison, or a significance test rather than a bare descriptive statistic, "
            + "use the dedicated stats tools instead of hand-computing over query() rows: "
            + "ols_regression (multivariate OLS — coefficients/SEs/p-values/R²), "
            + "iv_2sls (two-stage least squares with a corrected-SE point estimate, not the "
            + "upward-biased SEs a naive two-OLS-calls implementation gives), "
            + "diff_in_diff (treatment*post interaction regression, with a parallel-trends "
            + "caveat), hypothesis_test (t_test/anova/chi_square/ks_test — is a difference "
            + "large enough to not plausibly be chance, given n), panel_fixed_effects (two-way "
            + "entity+time fixed effects for panel/longitudinal data, e.g. state-year data — "
            + "controls for both fixed state characteristics AND nationwide-per-year shocks, "
            + "which diff_in_diff's simple dummies can't do with more than two periods), and "
            + "robust_regression (OLS with heteroskedasticity-robust or cluster-robust SEs — "
            + "use when observations aren't independent, e.g. repeated years of the same "
            + "state). Each takes a SQL SELECT plus column-name role assignments and runs the "
            + "FULL result set through real matrix algebra (Apache Commons Math), not the "
            + "row-limited query() path. For nonlinear/interaction relationships a linear model "
            + "can't capture, or a causal effect estimate that needs flexible ML nuisance "
            + "models rather than a linear control set: flexible_regression (random forest / "
            + "gradient boosting regression — in-sample fit + variable importance, NOT a "
            + "substitute for a held-out test set), feature_importance (ranks predictors by "
            + "how much a tree ensemble actually used them — not a causal ranking), and "
            + "double_ml_ate (Chernozhukov et al. 2018 Double/Debiased ML average treatment "
            + "effect — valid even with flexible ML nuisance functions, but still ASSUMES "
            + "UNCONFOUNDEDNESS like any observational-data causal estimate; prefer iv_2sls "
            + "when a genuine instrument is available instead of an unconfoundedness "
            + "argument). These three run on Smile, a separate JVM ML dependency from the "
            + "closed-form Commons Math tools above. "
            + "diff_in_diff ASSUMES parallel pre-trends and cannot test them — it collapses "
            + "the pre-period into one indicator, so a treated group already diverging gives "
            + "the same answer as one that was not. event_study estimates a coefficient per "
            + "period around treatment and jointly tests whether the pre-treatment ones are "
            + "zero. Run it alongside any diff_in_diff and report the pre-trend p-value with "
            + "the effect; it also flags staggered adoption, which biases the two-way "
            + "fixed-effects estimator itself. "
            + "NEVER compare raw counts across places or across years — use per_capita, which "
            + "joins Census population at the matching geography and year and returns the "
            + "denominator it used. California exceeding Wyoming on any count is a statement "
            + "about population until it is rated. "
            + "Before reporting ANY regression result as a finding, run "
            + "sensitivity_analysis with the same SQL and a jurisdiction group_col: it "
            + "refits leaving out one jurisdiction at a time and reports whether a single "
            + "unit carries the effect, flips its sign, or moves it across p=0.05. A "
            + "coefficient that has not been leave-one-out tested is not a finding. "
            + "This is a versioned snapshot, not a live feed: describe_table reports a "
            + "table's declared coverage window, and an empty result outside that window "
            + "means the period is not published yet, not zero. Say so rather than "
            + "substituting an outside figure; suggest_external_sources lists keyless "
            + "public endpoints for genuine gaps. The declared window is intent, not fact — "
            + "data_coverage(schema, table) scans the table and reports the years actually "
            + "loaded, the row count per year, and any years missing inside that span. Call "
            + "it before describing a trend or asserting that a period has no data, because "
            + "an unloaded year and a genuinely empty one are identical in a query result. "
            + "NEVER compare dollar figures across years without adjust_inflation — it "
            + "deflates to real dollars against the server's own BLS CPI-U vintage in "
            + "econ.inflation_metrics, in either single-amount or whole-result-set form, and "
            + "reports the index used per year. Do not deflate by hand from a remembered CPI "
            + "figure, and do not present a multi-year nominal comparison as growth. "
            + "render_chart draws a line/bar/pie/scatter/bubble image from data you've "
            + "already fetched — use it instead of hand-building a chart when a "
            + "visualization is requested. line/bar/pie take categories/series and treat a "
            + "null value as a gap rather than a false zero; scatter/bubble take points with "
            + "true numeric x/y (and size, for bubble) axes and have no category axis to "
            + "anchor a gap to, so omit a point instead of passing null. "
            + QuestionGuidance.RUBRIC
            + " Every analytical result also carries a second content block holding a "
            + "structured 'diagnostics' envelope: typed warnings (small_n, low_coverage, "
            + "row_fanout, grain_mismatch, vintage_misalignment, broken_field, "
            + "uncontrolled_confound, collinear_controls) each with a severity of info, "
            + "caution, or high, plus the grain, the observation count, and the declared "
            + "coverage windows of the tables involved. Read it before answering and let it "
            + "set how hard you hedge — a 'high' warning generally means re-query rather than "
            + "caveat. Its absence of warnings is not a clean bill of health, only that no "
            + "listed defect was detected. critique_query runs the same form-level checks on "
            + "SQL you are about to run, without running it.");
        return result(id, body);
    }

    private static ObjectNode handlePromptsGet(JsonNode id, JsonNode params) {
        String name = params.path("name").asText("");
        QuestionGuidance.Template t = QuestionGuidance.template(name);
        if (t == null) {
            return errorResponse(id, -32602, "Unknown prompt: " + name);
        }
        log.println("[askamerica-mcp] prompts/get name=" + name);
        return result(id, QuestionGuidance.promptGet(t, params.path("arguments")));
    }

    private static ObjectNode handleToolsList(JsonNode id) {
        ObjectNode body = MAPPER.createObjectNode();
        body.set("tools", toolDefs());
        return result(id, body);
    }

    /**
     * The tool definitions this server advertises, built once and reused.
     *
     * <p>Split out of {@link #handleToolsList} so argument validation can read the same
     * definitions the client was given. Validating against a hand-maintained second copy of
     * each argument list is how a tool ends up accepting a name it never advertised: the copy
     * drifts, and the drift is invisible until a caller pays for it.
     */
    private static ArrayNode toolDefs() {
        ArrayNode cached = TOOL_DEFS;
        if (cached != null) {
            return cached;
        }
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

        ObjectNode coverageProps = MAPPER.createObjectNode();
        coverageProps.set("schema", prop("string", "Schema name, e.g. 'econ'."));
        coverageProps.set("table", prop("string", "Table name, e.g. 'inflation_metrics'."));
        tools.add(
            tool("data_coverage",
            "Scan a table and report which years are ACTUALLY loaded — the year-by-year row "
            + "counts, any years missing INSIDE the loaded span, and any declared years that "
            + "hold no rows at all. describe_table reports the window the schema declares; "
            + "this reports the window the data has. Call it before stating a trend, a "
            + "year-over-year change, or 'no data exists for X': a zero from a year that was "
            + "never loaded is indistinguishable in a query result from a real zero, and a "
            + "regression over a series with a hole in the middle silently interpolates "
            + "across it. The first call for a table runs the scan and can take a while; "
            + "later calls are instant.",
            schema(coverageProps, new String[]{"schema", "table"})));

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
            + "(4) A <> or = comparison against a nullable column silently drops every NULL row "
            + "(standard SQL three-valued logic) — if you want NULLs included, add "
            + "OR <column> IS NULL explicitly; do not assume <> excludes only the literal value. "
            + "Example: SELECT \"year\", \"type\", SUM(amount) AS total "
            + "FROM fec.individual_contributions "
            + "WHERE \"year\" = '2024' AND (memo_cd <> 'X' OR memo_cd IS NULL) "
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
            "Execute SQL against US government data. Returns a JSON array of row objects, "
            + "plus a second content block holding a structured 'diagnostics' envelope — typed "
            + "warnings (small_n, low_coverage, row_fanout, grain_mismatch, "
            + "vintage_misalignment, broken_field, uncontrolled_confound) with a severity of "
            + "info/caution/high, the grain, the observation count, and the declared coverage "
            + "windows of the tables involved. Read it before answering: a 'high' warning "
            + "usually means re-query rather than caveat. No warnings is not a clean bill of "
            + "health, only that no listed defect was detected."
            + QuestionGuidance.exemplarBlock(),
            schema(queryProps, new String[]{"sql"})));

        ObjectNode critiqueProps = MAPPER.createObjectNode();
        critiqueProps.set("sql", prop("string",
            "The SQL you are about to run. It is not executed."));
        tools.add(
            tool("critique_query",
            "Check a query's form before running it. Returns the same typed diagnostics the "
            + "query tool returns, for the defects visible without executing: a year outside a "
            + "table's declared coverage window, a join that names only part of a multi-column "
            + "declared key (which would silently multiply every SUM and AVG), an association "
            + "aggregate with no observation count, a <> filter that drops NULL rows, and "
            + "tables whose coverage windows do not line up. Cheap — no query runs. Use it "
            + "before an expensive aggregate, or when a result looks surprising and you want "
            + "to know whether the question or the data is at fault. It reports defects it can "
            + "see; silence from it is not approval."
            + QuestionGuidance.exemplarBlock(),
            schema(critiqueProps, new String[]{"sql"})));

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

        ObjectNode entityProps = MAPPER.createObjectNode();
        entityProps.set("term", prop("string",
            "Name or identifier: an org/person name or fragment ('Alphabet', 'berkshire'), an "
            + "LEI, a SEC CIK ('0000320193' or '320193'), an FEC committee id, or an EIN."));
        entityProps.set("type", prop("string",
            "'org' (default, ref.canonical_org_entity) or 'person' (ref.canonical_person_entity)."));
        entityProps.set("source_schema", prop("string",
            "Optional narrowing filter: only entities seen in this schema ('sec', 'fec', "
            + "'patents', 'transport', ...). 'the Alphabet that appears in patents' is often "
            + "exactly what a caller means, and no amount of string matching can infer it."));
        entityProps.set("jurisdiction", prop("string",
            "Optional narrowing filter: GLEIF jurisdiction code ('US', 'GB', 'US-DE'). This is "
            + "what separates entities that are genuinely identically named — ALPHABET LTD (GB) "
            + "from ALPHABET INC. (US) — which no name score can do."));
        entityProps.set("limit", prop("integer",
            "Maximum candidates to return. Default 20, capped at 200. A name fragment can match "
            + "many entities; every candidate is returned rather than assuming the first is right."));
        tools.add(
            tool("resolve_entity",
            "Resolve one name or identifier to a canonical entity that spans EVERY source it "
            + "appears in — one row per real-world org/person, carrying its LEI, SEC CIK, FEC "
            + "committee, patent assignee, EIN, EIA utility, FMCSA DOT, FAA registrant, SBA "
            + "borrower/lender and more, each with a match confidence. Call before joining a "
            + "user-named company or person to any schema, exactly as resolve_geo is called "
            + "before a place join: the colloquial name is rarely the registered one, and the "
            + "key each schema carries differs per source. Backed by ref.canonical_org_entity / "
            + "ref.canonical_person_entity (the resolved layer) — not a name LIKE over one "
            + "registry. Use entity_relationships for corporate parents and siblings.",
            schema(entityProps, new String[]{"term"})));

        ObjectNode relProps = MAPPER.createObjectNode();
        relProps.set("lei", prop("string",
            "The entity's 20-character LEI. Get it from resolve_entity when you have a name."));
        relProps.set("direction", prop("string",
            "'parents' (default) — this entity's direct and ultimate parents; 'children' — "
            + "entities this one consolidates; 'siblings' — entities sharing its direct parent."));
        relProps.set("limit", prop("integer", "Maximum rows. Default 50, capped at 500."));
        tools.add(
            tool("entity_relationships",
            "Walk the legal-entity family tree: direct parent, ultimate parent, children, or "
            + "siblings, from GLEIF's accounting-consolidation relationships. Answers 'who "
            + "ultimately owns this filer', 'what else does this group own', and 'which entities "
            + "are under the same parent' — questions no single filing answers, because the "
            + "group structure lives in GLEIF, not in the filing. Restricted to ACTIVE, "
            + "PUBLISHED ownership edges (fund/branch/feeder edges excluded). Join the returned "
            + "LEIs back through resolve_entity to reach SEC/FEC/patent keys.",
            schema(relProps, new String[]{"lei"})));

        ObjectNode inflProps = MAPPER.createObjectNode();
        inflProps.set("base_year", prop("integer",
            "The year to express every amount in — 'real 2024 dollars' means base_year 2024. "
            + "Required."));
        inflProps.set("sql", prop("string",
            "Optional SQL SELECT returning a nominal amount column and a year column, one row "
            + "per observation. Each row comes back with the deflated amount added. Omit to "
            + "convert a single amount instead, via amount + from_year."));
        inflProps.set("value_col", prop("string",
            "With sql: the column holding the nominal amount."));
        inflProps.set("year_col", prop("string",
            "With sql: the column holding the year the amount is denominated in."));
        inflProps.set("amount", prop("number",
            "Without sql: a single nominal amount to convert."));
        inflProps.set("from_year", prop("integer",
            "Without sql: the year that amount is denominated in."));
        inflProps.set("index", prop("string",
            "'cpi_u' (default — CPI-U all items, BLS series CUUR0000SA0) or 'cpi_u_core' "
            + "(all items less food and energy, CUUR0000SA0L1E). Use one consistently across "
            + "an analysis; mixing them makes two figures incomparable."));
        ObjectNode capitaProps = MAPPER.createObjectNode();
        capitaProps.set("sql", prop("string",
            "SQL SELECT returning the count or amount to rate, the geography code, and the "
            + "year, one row per geography-year."));
        capitaProps.set("value_col", prop("string",
            "Column holding the count or amount to express per person."));
        capitaProps.set("geo_col", prop("string",
            "Column holding the FIPS code — 2-digit state or 5-digit county. Use resolve_geo "
            + "first if what you have is a place name or abbreviation; this joins on the "
            + "code, and will report a name as unmatched rather than guess at it."));
        capitaProps.set("year_col", prop("string",
            "Column holding the year the value is measured in. Population is matched to the "
            + "same year exactly — never a neighbouring year."));
        capitaProps.set("geo_level", prop("string",
            "'state' (default) or 'county' — which population geography to join to."));
        capitaProps.set("per", prop("number",
            "Rate denominator: 1 for per capita (default), 1000 per thousand, 100000 per "
            + "hundred thousand. Crime and mortality rates are conventionally per 100000."));
        capitaProps.set("population_source", prop("string",
            "'acs' (default — census.acs_population, ACS 5-year, the wider year range) or "
            + "'pep' (census.pep_population, the Census annual population estimates, which "
            + "cover fewer years but are the standard rate denominator). Use one "
            + "consistently across a comparison."));
        tools.add(
            tool("per_capita",
            "Convert counts to population rates by joining to Census population at the right "
            + "geography and year. Use whenever comparing counts across places or across "
            + "years: a raw count comparison between California and Wyoming, or between 2015 "
            + "and 2023, is mostly a statement about population size. Returns the population "
            + "used for every row so the denominator is checkable, and reports geographies "
            + "and years with no matching population rather than dropping or approximating "
            + "them.",
            schema(capitaProps, new String[]{"sql", "value_col", "geo_col", "year_col"})));

        tools.add(
            tool("adjust_inflation",
            "Convert nominal dollars to real (inflation-adjusted) dollars against one "
            + "server-side CPI vintage — annual averages of the BLS CPI-U series held in "
            + "econ.inflation_metrics, not a figure recalled from memory. Use whenever "
            + "comparing dollar amounts across years: spending, wages, revenue, damages, "
            + "budgets. An unadjusted multi-year dollar comparison is wrong by however much "
            + "prices moved, which over a decade is most of the effect people report. "
            + "Returns the CPI index and deflator used for every year so the arithmetic is "
            + "checkable, and fails loudly rather than guessing when a year has no loaded CPI.",
            schema(inflProps, new String[]{"base_year"})));

        ObjectNode alignProps = MAPPER.createObjectNode();
        ObjectNode seriesProp = MAPPER.createObjectNode();
        seriesProp.put("type", "array");
        seriesProp.put("description",
            "List of series specs. Each object: table (schema.table), value (column or SQL expr), "
            + "optional name, agg (default avg), where; plus ONE key source matching 'on' — "
            + "time_col (a DATE column), year_col+period_col (BLS year + 'M01'), quarter_col "
            + "(BEA '2023Q1'), year_only_col (annual tables), or geo_col (a FIPS column when on "
            + "is state/county/geo). value/where are composed into SQL, so the same dialect rules "
            + "as the query tool apply: quote reserved words with double quotes (e.g. \"value\", "
            + "\"year\", \"date\", \"type\", \"period\") and remember <> / = against a nullable "
            + "column drops NULL rows unless you add OR <column> IS NULL in where.");
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
            + "regression; for a single-table statistic, just call query with corr()/regr_*()."
            + QuestionGuidance.EXEMPLAR_POINTER,
            schema(alignProps, new String[]{"series"})));

        ObjectNode olsProps = MAPPER.createObjectNode();
        olsProps.set("sql", prop("string",
            "SQL SELECT returning the outcome and predictor columns needed below, one row "
            + "per observation. Same dialect rules as the query tool."));
        olsProps.set("outcome", prop("string", "Column name of the dependent variable (y)."));
        ObjectNode predictorsProp = MAPPER.createObjectNode();
        predictorsProp.put("type", "array");
        predictorsProp.put("description",
            "Column names of the independent variables (x). An intercept is added "
            + "automatically — do not include one.");
        olsProps.set("predictors", predictorsProp);
        tools.add(
            tool("ols_regression",
            "Multivariate OLS regression (y ~ intercept + x1 + x2 + ...) with proper "
            + "multiple covariates — coefficients, standard errors, t-stats, p-values, "
            + "R²/adjusted R², and the overall F-test. Use this instead of corr()/regr_slope() "
            + "in query() when you have more than one predictor; those SQL aggregates only "
            + "do simple bivariate relationships."
            + QuestionGuidance.EXEMPLAR_POINTER,
            schema(olsProps, new String[]{"sql", "outcome", "predictors"})));

        ObjectNode ivProps = MAPPER.createObjectNode();
        ivProps.set("sql", prop("string",
            "SQL SELECT returning the outcome, endogenous, instrument, and control columns "
            + "needed below, one row per observation."));
        ivProps.set("outcome", prop("string", "Column name of the dependent variable (y)."));
        ivProps.set("endogenous", prop("string",
            "Column name of the single endogenous regressor — the variable you suspect is "
            + "correlated with the error term (reverse causality / omitted-variable bias) and "
            + "want to instrument for."));
        ObjectNode instrumentsProp = MAPPER.createObjectNode();
        instrumentsProp.put("type", "array");
        instrumentsProp.put("description",
            "Column names of one or more instruments: variables that plausibly affect the "
            + "endogenous regressor but have no direct effect on the outcome except through "
            + "it. Instrument validity cannot be tested by this tool — it must be argued, not "
            + "computed.");
        ivProps.set("instruments", instrumentsProp);
        ObjectNode ivControlsProp = MAPPER.createObjectNode();
        ivControlsProp.put("type", "array");
        ivControlsProp.put("description",
            "Optional exogenous control column names, included in both stages. Omit for none.");
        ivProps.set("controls", ivControlsProp);
        tools.add(
            tool("iv_2sls",
            "Two-stage least squares (instrumental variables) for a single endogenous "
            + "regressor. Returns the corrected-standard-error 2SLS coefficients "
            + "(NOT the upward-biased SEs a naive 'two OLS calls' implementation would give) "
            + "plus the first-stage F-statistic with a weak-instrument warning "
            + "(Stock-Yogo rule of thumb: F < 10 is weak). Use when you suspect reverse "
            + "causality or omitted-variable bias between a predictor and the outcome and "
            + "have a plausible instrument — otherwise use ols_regression."
            + QuestionGuidance.EXEMPLAR_POINTER,
            schema(ivProps, new String[]{"sql", "outcome", "endogenous", "instruments"})));

        ObjectNode didProps = MAPPER.createObjectNode();
        didProps.set("sql", prop("string",
            "SQL SELECT returning the outcome, treatment, post, and control columns needed "
            + "below, one row per observation (e.g. one row per unit-period)."));
        didProps.set("outcome", prop("string", "Column name of the dependent variable (y)."));
        didProps.set("treatment", prop("string",
            "Column name of the treatment-group indicator (1 = treated unit, 0 = control "
            + "unit) — constant within a unit across periods."));
        didProps.set("post", prop("string",
            "Column name of the post-period indicator (1 = after the policy/event, "
            + "0 = before)."));
        ObjectNode didControlsProp = MAPPER.createObjectNode();
        didControlsProp.put("type", "array");
        didControlsProp.put("description", "Optional control column names. Omit for none.");
        didProps.set("controls", didControlsProp);
        tools.add(
            tool("diff_in_diff",
            "Difference-in-differences: y ~ treatment + post + treatment*post + controls. "
            + "did_estimate (the treatment*post interaction coefficient) is the estimated "
            + "average treatment effect on the treated, valid under the parallel-trends "
            + "assumption — this tool does not test parallel trends itself; check pre-period "
            + "trends separately (e.g. with ols_regression on pre-period-only data) before "
            + "trusting the estimate."
            + QuestionGuidance.EXEMPLAR_POINTER,
            schema(didProps, new String[]{"sql", "outcome", "treatment", "post"})));

        ObjectNode testProps = MAPPER.createObjectNode();
        testProps.set("sql", prop("string",
            "SQL SELECT returning the columns needed below, one row per observation."));
        testProps.set("test", prop("string",
            "'t_test' (two-sample Welch's, or one-sample if one_sample_mu is given), "
            + "'anova' (one-way, 2+ groups), 'chi_square' (test of independence between two "
            + "categorical columns), or 'ks_test' (two-sample Kolmogorov-Smirnov, compares "
            + "whole distributions, not just means)."));
        testProps.set("value_col", prop("string",
            "Numeric column to test. Required for t_test, anova, ks_test."));
        testProps.set("group_col", prop("string",
            "Categorical column whose distinct values define the groups. Required for anova "
            + "and ks_test; required for t_test unless one_sample_mu is given (t_test needs "
            + "exactly 2 distinct group values; ks_test needs exactly 2)."));
        testProps.set("one_sample_mu", prop("number",
            "For a one-sample t_test only: the hypothesized population mean to test value_col "
            + "against. Omit group_col when using this."));
        testProps.set("row_col", prop("string",
            "For chi_square only: first categorical column (contingency table rows)."));
        testProps.set("col_col", prop("string",
            "For chi_square only: second categorical column (contingency table columns)."));
        tools.add(
            tool("hypothesis_test",
            "Statistical significance tests: is a difference between groups (or from a fixed "
            + "value, or between two distributions, or between two categorical variables) "
            + "large enough to not plausibly be chance, given the sample size? Complements "
            + "query()'s descriptive stats (corr, regr_*, stddev_samp, ...), which describe a "
            + "relationship's strength but don't test its statistical significance."
            + QuestionGuidance.EXEMPLAR_POINTER,
            schema(testProps, new String[]{"sql", "test"})));

        ObjectNode feProps = MAPPER.createObjectNode();
        feProps.set("sql", prop("string",
            "SQL SELECT returning the outcome, predictor, entity, and time columns needed "
            + "below, one row per unit-period observation."));
        feProps.set("outcome", prop("string", "Column name of the dependent variable (y)."));
        ObjectNode fePredictorsProp = MAPPER.createObjectNode();
        fePredictorsProp.put("type", "array");
        fePredictorsProp.put("description",
            "Column names of the independent variables. No intercept is reported — it's "
            + "absorbed into the entity and time effects, not because none exists.");
        feProps.set("predictors", fePredictorsProp);
        feProps.set("entity_col", prop("string",
            "Column identifying the unit (e.g. state, county, firm) — constant within a unit "
            + "across periods."));
        feProps.set("time_col", prop("string",
            "Column identifying the period (e.g. year) — constant across units within a "
            + "period."));
        feProps.set("cluster_col", prop("string",
            "Optional column to cluster standard errors on, usually the same as entity_col. "
            + "Omitted, standard errors assume observations are independent — with repeated "
            + "observations of the same entity that is false and overstates precision. "
            + "Clustering also moves inference to (clusters - 1) degrees of freedom."));
        tools.add(
            tool("panel_fixed_effects",
            "Two-way (entity + time) fixed-effects panel regression via the within/demeaning "
            + "estimator — controls for everything constant within an entity over time (e.g. "
            + "fixed state characteristics) AND everything common to all entities in a given "
            + "period (e.g. a nationwide shock), which diff_in_diff's simple treatment/post "
            + "dummies can't do with more than two periods or a staggered treatment timing. "
            + "Standard errors use the correct panel degrees of freedom "
            + "(n - k - (numEntities + numTimes - 1)), not what a naive dummy-variable OLS "
            + "would report if it didn't know about the absorbed fixed effects."
            + QuestionGuidance.EXEMPLAR_POINTER,
            schema(feProps, new String[]{"sql", "outcome", "predictors", "entity_col", "time_col"})));

        ObjectNode robustProps = MAPPER.createObjectNode();
        robustProps.set("sql", prop("string",
            "SQL SELECT returning the outcome, predictor, and (if using cluster_col) cluster "
            + "columns needed below, one row per observation."));
        robustProps.set("outcome", prop("string", "Column name of the dependent variable (y)."));
        ObjectNode robustPredictorsProp = MAPPER.createObjectNode();
        robustPredictorsProp.put("type", "array");
        robustPredictorsProp.put("description",
            "Column names of the independent variables (x). An intercept is added "
            + "automatically.");
        robustProps.set("predictors", robustPredictorsProp);
        robustProps.set("cluster_col", prop("string",
            "Optional categorical column defining clusters whose errors may be correlated "
            + "(e.g. state, so multiple years of the same state aren't treated as independent "
            + "observations). Omit for heteroskedasticity-robust (White/HC1) SEs instead — "
            + "valid when errors vary in magnitude across observations but aren't correlated "
            + "within any grouping."));
        tools.add(
            tool("robust_regression",
            "OLS with heteroskedasticity-robust (White/HC1) or cluster-robust standard errors "
            + "— same coefficients as ols_regression, corrected SEs. Use when observations "
            + "plausibly aren't independent (e.g. repeated observations of the same state "
            + "over years) or error variance plausibly isn't constant — both are common in "
            + "state/county panel data and understate uncertainty if ignored."
            + QuestionGuidance.EXEMPLAR_POINTER,
            schema(robustProps, new String[]{"sql", "outcome", "predictors"})));

        ObjectNode eventProps = MAPPER.createObjectNode();
        eventProps.set("sql", prop("string",
            "SQL SELECT returning one row per unit per period, with the outcome, the unit "
            + "identifier, the period, and the period that unit was treated in."));
        eventProps.set("outcome", prop("string", "Column name of the dependent variable (y)."));
        eventProps.set("unit_col", prop("string",
            "Column identifying the unit observed repeatedly — state_fips, county_fips, "
            + "agency id. Unit fixed effects are absorbed."));
        eventProps.set("time_col", prop("string",
            "Column holding the period of the observation, as an integer year or period "
            + "number. Time fixed effects are absorbed."));
        eventProps.set("treatment_time_col", prop("string",
            "Column holding the period in which THAT ROW'S unit was treated — the same value "
            + "repeated on every row of a treated unit, and NULL on every row of a "
            + "never-treated unit. Never-treated units become the comparison group. Build it "
            + "with a JOIN or a CASE expression in the SQL."));
        eventProps.set("max_lead", prop("integer",
            "How many periods before treatment get their own coefficient (default 5). "
            + "Anything earlier is binned into a single pre_beyond_ term, never dropped."));
        eventProps.set("max_lag", prop("integer",
            "How many periods after treatment get their own coefficient (default 5). "
            + "Anything later is binned into a single post_beyond_ term."));
        eventProps.set("reference_period", prop("integer",
            "The pre-treatment period every coefficient is measured against, as a negative "
            + "offset (default -1, the period before treatment). Must be <= 0."));
        eventProps.set("cluster_col", prop("string",
            "Column to cluster standard errors on. Defaults to unit_col, which is almost "
            + "always right: repeated observations of the same unit are not independent, and "
            + "conventional errors overstate precision badly on this design. Pass a coarser "
            + "column (region for a state panel) if you believe the correlation is wider. "
            + "Pass 'none' to disable clustering, which is rarely correct."));
        tools.add(
            tool("event_study",
            "Estimate a separate effect for each period before and after treatment, with unit "
            + "and time fixed effects — and jointly test whether the pre-treatment "
            + "coefficients are zero. That test is the point: diff_in_diff assumes parallel "
            + "trends and CANNOT check them, because it collapses the whole pre-period into "
            + "one indicator, so a treated group that was already diverging yields the same "
            + "number as one that was not. Run this before believing any diff_in_diff result, "
            + "and report the pre-trend p-value alongside the effect. Also reports whether "
            + "adoption is staggered, which makes the two-way-FE estimator itself suspect. "
            + "Standard errors are cluster-robust on the unit by default, and the pre-trend "
            + "test uses the same covariance.",
            schema(eventProps, new String[]{"sql", "outcome", "unit_col", "time_col",
                "treatment_time_col"})));

        ObjectNode sensProps = MAPPER.createObjectNode();
        sensProps.set("sql", prop("string",
            "SQL SELECT returning the outcome, predictor, and group columns needed below, "
            + "one row per observation. Use the SAME SQL you ran through ols_regression / "
            + "panel_fixed_effects / robust_regression."));
        sensProps.set("outcome", prop("string", "Column name of the dependent variable (y)."));
        ObjectNode sensPredictorsProp = MAPPER.createObjectNode();
        sensPredictorsProp.put("type", "array");
        sensPredictorsProp.put("description", "Column names of the predictor variables.");
        sensProps.set("predictors", sensPredictorsProp);
        sensProps.set("group_col", prop("string",
            "Column identifying the unit to leave out one at a time — usually the "
            + "jurisdiction (state_fips, state_abbr, county_fips), but any grouping whose "
            + "influence you want tested works. Max 200 distinct values."));
        sensProps.set("term", prop("string",
            "Which coefficient to track across the refits. Defaults to the first predictor. "
            + "Use 'intercept' to track the intercept."));
        tools.add(
            tool("sensitivity_analysis",
            "Refit a regression once per jurisdiction with that jurisdiction's rows removed, "
            + "and report how far the coefficient moves. Answers the question a single "
            + "regression cannot: is this result the pattern across units, or is one outlier "
            + "carrying it? Reports the coefficient range across refits, which group moves it "
            + "most (standardized DFBETA influence), and whether dropping any single group "
            + "flips the sign or crosses p=0.05. Run this before reporting any regression "
            + "result as a finding — DC, Alaska, Wyoming, and single-refinery or "
            + "single-hospital counties routinely drive national estimates, and a result that "
            + "survives leave-one-out is a much stronger claim than one that was never tested.",
            schema(sensProps, new String[]{"sql", "outcome", "predictors", "group_col"})));

        ObjectNode flexProps = MAPPER.createObjectNode();
        flexProps.set("sql", prop("string",
            "SQL SELECT returning the outcome and predictor columns needed below, one row "
            + "per observation."));
        flexProps.set("outcome", prop("string", "Column name of the dependent variable (y)."));
        ObjectNode flexPredictorsProp = MAPPER.createObjectNode();
        flexPredictorsProp.put("type", "array");
        flexPredictorsProp.put("description", "Column names of the predictor variables.");
        flexProps.set("predictors", flexPredictorsProp);
        flexProps.set("method", prop("string",
            "'random_forest' (default if omitted) or 'gradient_boosting'."));
        tools.add(
            tool("flexible_regression",
            "Random forest or gradient boosting regression — captures nonlinear relationships "
            + "and interactions that ols_regression's linear form can't, at the cost of "
            + "interpretability (no coefficients, just fit quality and variable importance). "
            + "Use when you suspect the relationship isn't linear/additive, or as an "
            + "exploratory check on whether a linear model is leaving real signal on the "
            + "table; use ols_regression when you need interpretable, reportable coefficients."
            + QuestionGuidance.EXEMPLAR_POINTER,
            schema(flexProps, new String[]{"sql", "outcome", "predictors"})));

        ObjectNode importanceProps = MAPPER.createObjectNode();
        importanceProps.set("sql", prop("string",
            "SQL SELECT returning the outcome and predictor columns needed below, one row "
            + "per observation."));
        importanceProps.set("outcome", prop("string", "Column name of the outcome to predict."));
        ObjectNode importancePredictorsProp = MAPPER.createObjectNode();
        importancePredictorsProp.put("type", "array");
        importancePredictorsProp.put("description",
            "Column names of the candidate predictor variables to rank by importance.");
        importanceProps.set("predictors", importancePredictorsProp);
        importanceProps.set("method", prop("string",
            "'random_forest' (default if omitted) or 'gradient_boosting'."));
        tools.add(
            tool("feature_importance",
            "Ranks predictors by how much a random forest / gradient boosting model actually "
            + "used them to predict the outcome (impurity decrease summed across trees) — "
            + "captures nonlinear and interaction effects a bivariate corr() ranking would "
            + "miss entirely. NOT a causal ranking and not necessarily monotonic — a variable "
            + "can rank high because trees split on it a lot, not because increasing it "
            + "increases the outcome."
            + QuestionGuidance.EXEMPLAR_POINTER,
            schema(importanceProps, new String[]{"sql", "outcome", "predictors"})));

        ObjectNode dmlProps = MAPPER.createObjectNode();
        dmlProps.set("sql", prop("string",
            "SQL SELECT returning the outcome, treatment, and control columns needed below, "
            + "one row per observation."));
        dmlProps.set("outcome", prop("string", "Column name of the outcome (y)."));
        dmlProps.set("treatment", prop("string",
            "Column name of the treatment/exposure variable whose average effect on the "
            + "outcome you want to estimate (can be continuous or 0/1)."));
        ObjectNode dmlControlsProp = MAPPER.createObjectNode();
        dmlControlsProp.put("type", "array");
        dmlControlsProp.put("description",
            "Column names of control variables — at least one required. DML's validity rests "
            + "entirely on these being SUFFICIENT to satisfy unconfoundedness; it cannot "
            + "detect or correct for an insufficient control set.");
        dmlProps.set("controls", dmlControlsProp);
        dmlProps.set("folds", prop("integer",
            "Number of cross-fitting folds (default 5). Each fold's nuisance models are "
            + "trained on the other folds only, so no observation's residual comes from a "
            + "model that saw that observation."));
        dmlProps.set("method", prop("string",
            "'random_forest' (default if omitted) or 'gradient_boosting' — the nuisance-"
            + "function learner for both the treatment and outcome models."));
        tools.add(
            tool("double_ml_ate",
            "Double/Debiased Machine Learning average treatment effect (Chernozhukov et al. "
            + "2018): cross-fits flexible ML nuisance models for treatment and outcome, then "
            + "estimates the treatment effect from the orthogonalized residuals — valid even "
            + "though the nuisance models themselves are biased/noisy ML fits, UNLIKE plugging "
            + "raw ML predictions into a naive comparison. Still ASSUMES UNCONFOUNDEDNESS — "
            + "this corrects for how the nuisance functions are estimated, not for whether the "
            + "controls are the right ones; that's a substantive claim about the data you must "
            + "argue for, this tool cannot verify it. Prefer iv_2sls when you have a genuine "
            + "instrument instead of an unconfoundedness argument."
            + QuestionGuidance.EXEMPLAR_POINTER,
            schema(dmlProps, new String[]{"sql", "outcome", "treatment", "controls"})));

        ObjectNode chartProps = MAPPER.createObjectNode();
        chartProps.set(
            "chart_type", prop("string",
            "'line', 'bar', 'pie', 'scatter', or 'bubble'. Default 'line'. line/bar/pie use "
            + "categories+series (a shared category axis); scatter/bubble use points (true "
            + "numeric x/y axes — there is no category axis to plot against)."));
        chartProps.set("title", prop("string", "Chart title."));
        chartProps.set(
            "x_label", prop("string", "X-axis label. Ignored for 'pie'."));
        chartProps.set(
            "y_label", prop("string", "Y-axis label — keep it SHORT, ideally under about 25 "
                + "characters. It is drawn rotated, so its budget is the panel's plot HEIGHT, "
                + "not its width; a long one wraps to at most three lines and is ellipsised "
                + "past that, which on a short panel leaves a stub like '% change ...'. Put "
                + "the qualification in the panel caption instead, where there is room. "
                + "Ignored for 'pie'."));
        ObjectNode categoriesProp = MAPPER.createObjectNode();
        categoriesProp.put("type", "array");
        categoriesProp.put(
            "description",
            "For 'line'/'bar'/'pie' only. X-axis categories shared by every series, e.g. "
            + "years, dates, or names. For 'pie', these are the slice labels.");
        chartProps.set("categories", categoriesProp);
        ObjectNode chartSeriesProp = MAPPER.createObjectNode();
        chartSeriesProp.put("type", "array");
        chartSeriesProp.put(
            "description",
            "For 'line'/'bar'/'pie' only. List of series to plot. Each object: name (string) "
            + "and values (array of numbers, same length and order as categories). 'pie' "
            + "takes exactly one series, whose values become the slice sizes.");
        chartProps.set("series", chartSeriesProp);
        ObjectNode pointsProp = MAPPER.createObjectNode();
        pointsProp.put("type", "array");
        pointsProp.put(
            "description",
            "For 'scatter'/'bubble' only. List of point series to plot. Each object: name "
            + "(string), x (array of numbers), y (array of numbers, same length as x), and "
            + "for 'bubble' only, size (array of numbers, same length as x — the bubble "
            + "radius at each point). Points have no category axis, so every coordinate must "
            + "be a real number — omit a point instead of passing null for a missing value.");
        chartProps.set("points", pointsProp);
        chartProps.set(
            "width", prop("integer", "Image width in pixels (default 800, max 2000)."));
        chartProps.set(
            "height", prop("integer", "Image height in pixels (default 500, max 2000)."));
        tools.add(
            tool("render_chart",
            "Render ONE chart (line, bar, pie, scatter, or bubble). Returns TWO blocks: a PNG "
            + "for the reader, and the same chart as editable SVG for you. PREFER "
            + "compose_dashboard whenever the answer has more than one figure worth showing — "
            + "which is most of the time — and reach for this only when a single bare chart "
            + "really is the whole deliverable; a reader handed three separate images has to "
            + "assemble the story your analysis already did. line/bar/pie plot "
            + "categories+series against a shared category axis; scatter/bubble plot points "
            + "against true numeric x/y axes (bubble adds a third size dimension) — use "
            + "scatter/bubble for a genuine x-vs-y relationship rather than a trend over "
            + "categories. Build the arrays from a prior query or fetch_aligned_series result — "
            + "this tool only draws, it does not fetch data. EDIT THE RETURNED SVG rather than "
            + "re-rendering when you need an annotation or callout, direct value labels, one "
            + "category greyed out or otherwise de-emphasised, or reworded titles and labels: "
            + "every mark has an id (mark-*, series-*, xtick-*) and every label a class, so "
            + "each of those is a targeted change. Re-render only when the data itself changes. "
            + "Do not move plotted geometry — the coordinates are derived from the values you "
            + "passed, so shifting a mark makes the picture disagree with its own numbers.",
            schema(chartProps, new String[]{})));

        ObjectNode dashProps = MAPPER.createObjectNode();
        dashProps.set("title", prop("string", "Dashboard title, shown top-left."));
        dashProps.set("subtitle", prop("string",
            "One line under the title — the source, vintage, and units the whole board "
            + "shares. ONE line: it does not wrap, so it is shrunk to fit and then "
            + "ellipsised. Around 120 characters is the practical ceiling at a typical board "
            + "width; a 230-character subtitle loses its tail. Per-source detail belongs in "
            + "the panel captions and the footnote, which have the room."));
        dashProps.set("footnote", prop("string", "Caveat line along the bottom."));
        dashProps.set("include_svg", prop("boolean",
            "Return the SVG source as well (default false). The response always carries a PNG "
            + "and a loopback link to the full-size board, which is what a reader needs. Ask "
            + "for the source only when you intend to EDIT the chart — add a callout, grey out "
            + "a category, add value labels — or to save it to a file. It costs roughly 7,000 "
            + "tokens, so do not request it merely to look at the result."));
        dashProps.set("byline", prop("string",
            "Optional attribution line above the AskAmerica mark, bottom-right — e.g. "
            + "'Prepared 2026-08-19' or an analyst name. The mark itself is always present."));
        dashProps.set("columns", prop("integer", "Grid columns, 1-4 (default 2)."));
        dashProps.set("width", prop("integer", "Image width in pixels (default fits the grid)."));
        dashProps.set("height", prop("integer", "Image height in pixels."));
        ObjectNode panelsProp = MAPPER.createObjectNode();
        panelsProp.put("type", "array");
        panelsProp.put("description",
            "The panels, in reading order. Each is either a chart or a stat tile.\n"
            + "CHART panel: {\"type\":\"chart\", plus the same arguments render_chart takes "
            + "— chart_type, title, x_label, y_label, and categories+series (line/bar/pie) or "
            + "points (scatter/bubble)}.\n"
            + "STAT panel: {\"type\":\"stat\", \"label\":\"Real 10-year rise\", "
            + "\"value\":\"+$19,029\", \"delta\":\"+22.0%\", \"delta_direction\":"
            + "\"up\"|\"down\"|\"flat\"} — a headline number the charts explain.\n"
            + "Every panel also takes: \"caption\" (a line under the panel), \"span\" (how "
            + "many columns it occupies, default 1), and \"scale_group\" (a name).\n"
            + "USE scale_group whenever two panels are meant to be compared: panels sharing a "
            + "group get one y-axis domain spanning all of them. Without it each panel fits "
            + "its own axis, so the taller bar can be the smaller number and nothing on either "
            + "panel looks wrong.");
        dashProps.set("panels", panelsProp);
        tools.add(
            tool("compose_dashboard",
            "THE DEFAULT WAY TO VISUALISE AN ANSWER. Composes charts and headline numbers into "
            + "ONE dashboard, returned as a PNG plus a single self-contained SVG you can "
            + "publish as an artifact or drop into an HTML page. Use this rather than several "
            + "render_chart calls whenever the answer is a set of related figures rather than "
            + "one chart — a stat tile for the headline "
            + "number, a trend line, and a ranking bar chart read as one story where three "
            + "separate images do not. Panels are placed, never redrawn, so each panel's "
            + "geometry is still exactly what its own data produced. Pass scale_group on panels "
            + "that should be compared so they share one axis domain.",
            schema(dashProps, new String[]{"panels"})));

        ObjectNode pubProps = MAPPER.createObjectNode();
        pubProps.set("title", prop("string", "The finding, as a sentence a reader could quote."));
        pubProps.set("subtitle", prop("string",
            "Source, vintage and units the whole report shares."));
        ObjectNode sectionsProp = MAPPER.createObjectNode();
        sectionsProp.put("type", "array");
        sectionsProp.put("description",
            "The narrative, in order: [{\"heading\":\"Step 1 — ...\", \"html\":\"<p>...</p>\"}]. "
            + "Bodies are HTML you write directly — <p>, <ul>, <table>, <strong>, <code>, "
            + "<blockquote> all style correctly. Write HTML, not Markdown: nothing renders "
            + "Markdown on the page. Scripts never execute, so do not include any.");
        pubProps.set("sections", sectionsProp);
        ObjectNode dashProp = MAPPER.createObjectNode();
        dashProp.put("type", "object");
        dashProp.put("description",
            "Optional. The same arguments compose_dashboard takes (title, subtitle, columns, "
            + "panels, footnote, byline). The board is composed and inlined at the top of the "
            + "report, so one call produces the whole deliverable.");
        pubProps.set("dashboard", dashProp);
        ObjectNode sourcesProp = MAPPER.createObjectNode();
        sourcesProp.put("type", "array");
        sourcesProp.put("description",
            "Citations: [{\"label\":\"Census ACS 1-year B19013\", \"url\":\"https://...\", "
            + "\"note\":\"2024 vintage\"}]. Include the AskAmerica tables you queried as well "
            + "as web sources — a reader cannot check a number whose origin is not named.");
        pubProps.set("sources", sourcesProp);
        pubProps.set("footnote", prop("string", "The caveat that qualifies the whole report."));
        pubProps.set("byline", prop("string", "Attribution line, e.g. 'Prepared 2026-08-19'."));
        tools.add(
            tool("publish_report",
            "Publish a complete answer — narrative, dashboard and citations — as one "
            + "self-contained HTML page, and return its link. THIS IS THE DELIVERABLE for any "
            + "question worth more than a sentence: the reader gets the finding, the figures, "
            + "the caveats and the sourcing in one page they can open, save, print or send, "
            + "instead of a chart plus prose they have to reassemble. Pass the dashboard "
            + "argument to compose and inline the board in the same call. Costs about twenty "
            + "tokens to return, because what comes back is a link rather than the page.",
            schema(pubProps, new String[]{"title"})));

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

        ObjectNode updateSchemaProps = MAPPER.createObjectNode();
        tools.add(
            tool("update_schema",
            "Rebuild the catalog against current data and reconnect, without a new server "
            + "deploy. Use after a data problem that broke one or more deferred views has been "
            + "fixed (e.g. a sync gap), to retry them now instead of waiting for a query to "
            + "stumble onto each one. Discards every cached connection — the next tool call "
            + "reconnects fresh, so expect one slower call right after this.",
            schema(updateSchemaProps, new String[]{})));

        TOOL_DEFS = tools;
        return tools;
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
    /**
     * Closes a discarded schema connection, logging rather than throwing on failure.
     *
     * <p>A connection being replaced is already suspect — it may be past its TTL or dead — so a
     * failure to close it must not propagate into the caller's tool call. The point is to release
     * the sockets, and a best-effort close does that in every case where a close was possible at
     * all.
     */
    private static void closeQuietly(Connection conn, String schemaName) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (Exception e) {
            log.println("[askamerica-mcp] Failed to close discarded connection for '"
                + schemaName + "': " + e);
        }
    }

    static Connection getSchemaConnection(final String schemaName) throws Exception {
        Connection existing = schemaConns.get(schemaName);
        if (existing != null) {
            Long openedAt = schemaConnOpenedAtMillis.get(schemaName);
            long ageMillis = (openedAt == null) ? Long.MAX_VALUE
                : System.currentTimeMillis() - openedAt;
            // A cached-but-dead connection would otherwise be handed out forever, so a
            // connection that has died since init drops out of the cache and re-inits below.
            // A cached-but-STALE connection is the same failure mode with no exception to
            // catch it by: isValid(5) only pings the connection, it does not know the
            // Iceberg tables built at open time have since changed on R2. TTL expiry forces
            // the same re-init path so staleness is bounded rather than open-ended.
            if (ageMillis < SCHEMA_CONN_TTL_MILLIS && existing.isValid(5)) {
                return existing;
            }
            log.println("[askamerica-mcp] Cached connection for '" + schemaName
                + "' is " + (ageMillis >= SCHEMA_CONN_TTL_MILLIS ? "past its TTL" : "dead")
                + " — discarding and re-initializing.");
            schemaConns.remove(schemaName, existing);
            schemaConnOpenedAtMillis.remove(schemaName);
            schemaLatches.remove(schemaName);
            schemaErrors.remove(schemaName);
            // Dropping the map entry does NOT release the connection. Everything the schema
            // built at open time stays reachable through it — including the S3 client each
            // govdata schema creates for its materialized and cache storage, with a
            // 200-connection Apache pool behind it. Left unclosed, those sockets sit idle
            // until the object store sends FIN and nothing answers, so they pile up in
            // CLOSE_WAIT: a user on a MinIO-backed dev server reported 214 of them.
            // Reported as "calling MinIO every few minutes and never closing the connection";
            // the periodic part is this TTL re-init, the leak is that the old connection was
            // only forgotten, never closed.
            closeQuietly(existing, schemaName);
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
                    schemaConnOpenedAtMillis.put(k, System.currentTimeMillis());
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

        // Cleared per call so a repair reported below belongs to this statement, not a
        // previous one that happened to run on the same worker thread.
        LAST_REPAIR_NOTICE.remove();
        long t0 = System.currentTimeMillis();
        String text;
        String telemetrySql = null;
        byte[] chartPng = null;
        String chartSvg = null;
        // The diagnostics envelope for this call, when the tool is an analytical one. Kept
        // separate from `text` so the data payload stays byte-identical to what it was before
        // diagnostics existed — a host that ignores the extra content block sees no change.
        ObjectNode diagnostics = null;
        try {
            // Inside the try so a rejection reaches the caller the same way every other tool
            // failure does — as a result with isError=true. Thrown from outside it, the
            // exception escaped the handler and the caller was answered with nothing at all.
            applyArgAliases(name, args);
            validateArgs(name, args);
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
                    ArrayNode rows = query(sql, limit);
                    text = rows.toString();
                    diagnostics = diagnose(sql, rows, limit);
                    break;
                }
                case "critique_query": {
                    String sql = args.path("sql").asText();
                    log.println("[askamerica-mcp] tool=critique_query sql=" + sql);
                    text = critiqueQuery(sql);
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
                case "update_schema": {
                    log.println("[askamerica-mcp] tool=update_schema");
                    text = updateSchema();
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
                case "resolve_entity": {
                    String term = args.path("term").asText();
                    String entType = args.has("type") && !args.get("type").isNull()
                        ? args.get("type").asText() : "org";
                    int entLimit = args.has("limit") && !args.get("limit").isNull()
                        ? args.get("limit").asInt() : 20;
                    String entSchema = args.has("source_schema")
                        && !args.get("source_schema").isNull()
                        ? args.get("source_schema").asText() : null;
                    String entJur = args.has("jurisdiction") && !args.get("jurisdiction").isNull()
                        ? args.get("jurisdiction").asText() : null;
                    log.println("[askamerica-mcp] tool=resolve_entity term=" + term
                        + " type=" + entType + " schema=" + entSchema + " jur=" + entJur);
                    text = resolveEntity(term, entType, entLimit, entSchema, entJur);
                    break;
                }
                case "entity_relationships": {
                    String relLei = args.path("lei").asText();
                    String relDir = args.has("direction") && !args.get("direction").isNull()
                        ? args.get("direction").asText() : "parents";
                    int relLimit = args.has("limit") && !args.get("limit").isNull()
                        ? args.get("limit").asInt() : 50;
                    log.println("[askamerica-mcp] tool=entity_relationships lei=" + relLei
                        + " direction=" + relDir);
                    text = entityRelationships(relLei, relDir, relLimit);
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
                    RowsWithSql aligned =
                        fetchAlignedSeries(seriesNode, on, stat, alignLimit);
                    text = aligned.rows.toString();
                    diagnostics = diagnose(aligned.sql, aligned.rows,
                        stat != null ? 5 : alignLimit);
                    break;
                }
                case "data_coverage": {
                    String schema = args.path("schema").asText();
                    String table = args.path("table").asText();
                    log.println("[askamerica-mcp] tool=data_coverage " + schema + "." + table);
                    text = dataCoverage(schema, table);
                    break;
                }
                case "adjust_inflation": {
                    // base_year is declared required, but MCP clients do not necessarily
                    // enforce the input schema. Without this check a missing value became
                    // asInt()==0 and surfaced downstream as "no CPI loaded for base_year 0",
                    // which names the symptom rather than the missing parameter. Callers also
                    // reach for to_year/target_year by analogy with other tools, so accept
                    // those spellings instead of failing on a guessable name.
                    Integer baseYearArg = optInt(args, "base_year", "to_year", "target_year");
                    if (baseYearArg == null) {
                        throw new IllegalArgumentException("base_year is required — the year "
                            + "whose dollars you want the result expressed in. Example: "
                            + "{\"amount\": 61933, \"from_year\": 2014, \"base_year\": 2024}. "
                            + "(to_year and target_year are accepted as aliases.)");
                    }
                    int baseYear = baseYearArg.intValue();
                    String sql = args.has("sql") && !args.get("sql").isNull()
                        ? args.get("sql").asText() : null;
                    String valueCol = args.has("value_col") && !args.get("value_col").isNull()
                        ? args.get("value_col").asText() : null;
                    String yearCol = args.has("year_col") && !args.get("year_col").isNull()
                        ? args.get("year_col").asText() : null;
                    Double amount = args.has("amount") && !args.get("amount").isNull()
                        ? Double.valueOf(args.get("amount").asDouble()) : null;
                    Integer fromYear = args.has("from_year") && !args.get("from_year").isNull()
                        ? Integer.valueOf(args.get("from_year").asInt()) : null;
                    String index = args.has("index") && !args.get("index").isNull()
                        ? args.get("index").asText() : null;
                    log.println("[askamerica-mcp] tool=adjust_inflation base_year=" + baseYear
                        + " index=" + index + " mode=" + (sql != null ? "sql" : "scalar"));
                    text = adjustInflationTool(baseYear, sql, valueCol, yearCol, amount,
                        fromYear, index);
                    break;
                }
                case "per_capita": {
                    String sql = args.path("sql").asText();
                    String valueCol = args.path("value_col").asText();
                    String geoCol = args.path("geo_col").asText();
                    String yearCol = args.path("year_col").asText();
                    String geoLevel = args.has("geo_level") && !args.get("geo_level").isNull()
                        ? args.get("geo_level").asText() : null;
                    double per = args.has("per") && !args.get("per").isNull()
                        ? args.get("per").asDouble() : 1.0;
                    String popSource = args.has("population_source")
                        && !args.get("population_source").isNull()
                        ? args.get("population_source").asText() : null;
                    log.println("[askamerica-mcp] tool=per_capita level=" + geoLevel
                        + " per=" + per + " source=" + popSource);
                    text = perCapitaTool(sql, valueCol, geoCol, yearCol, geoLevel, per,
                        popSource);
                    break;
                }
                case "event_study": {
                    String sql = args.path("sql").asText();
                    String outcome = args.path("outcome").asText();
                    String unitCol = args.path("unit_col").asText();
                    String timeCol = args.path("time_col").asText();
                    String treatTimeCol = args.path("treatment_time_col").asText();
                    int maxLead = args.has("max_lead") && !args.get("max_lead").isNull()
                        ? args.get("max_lead").asInt() : 5;
                    int maxLag = args.has("max_lag") && !args.get("max_lag").isNull()
                        ? args.get("max_lag").asInt() : 5;
                    int reference = args.has("reference_period")
                        && !args.get("reference_period").isNull()
                        ? args.get("reference_period").asInt() : -1;
                    String eventCluster = args.has("cluster_col")
                        && !args.get("cluster_col").isNull()
                        ? args.get("cluster_col").asText() : unitCol;
                    log.println("[askamerica-mcp] tool=event_study outcome=" + outcome
                        + " unit=" + unitCol + " window=[-" + maxLead + "," + maxLag + "]"
                        + " cluster=" + eventCluster);
                    StatsOutput r = eventStudyTool(sql, outcome, unitCol, timeCol, treatTimeCol,
                        maxLead, maxLag, reference, eventCluster);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "sensitivity_analysis": {
                    String sql = args.path("sql").asText();
                    String outcome = args.path("outcome").asText();
                    List<String> predictors = textArray(args.path("predictors"));
                    String groupCol = args.path("group_col").asText();
                    String term = args.has("term") && !args.get("term").isNull()
                        ? args.get("term").asText() : null;
                    log.println("[askamerica-mcp] tool=sensitivity_analysis outcome=" + outcome
                        + " group_col=" + groupCol);
                    StatsOutput r = sensitivityAnalysisTool(sql, outcome, predictors, groupCol, term);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "ols_regression": {
                    String sql = args.path("sql").asText();
                    String outcome = args.path("outcome").asText();
                    List<String> predictors = textArray(args.path("predictors"));
                    log.println("[askamerica-mcp] tool=ols_regression outcome=" + outcome
                        + " predictors=" + predictors);
                    StatsOutput r = olsRegressionTool(sql, outcome, predictors);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "iv_2sls": {
                    String sql = args.path("sql").asText();
                    String outcome = args.path("outcome").asText();
                    String endogenous = args.path("endogenous").asText();
                    List<String> instruments = textArray(args.path("instruments"));
                    List<String> controls = textArray(args.path("controls"));
                    log.println("[askamerica-mcp] tool=iv_2sls outcome=" + outcome
                        + " endogenous=" + endogenous + " instruments=" + instruments);
                    StatsOutput r = iv2slsTool(sql, outcome, endogenous, instruments, controls);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "diff_in_diff": {
                    String sql = args.path("sql").asText();
                    String outcome = args.path("outcome").asText();
                    String treatment = args.path("treatment").asText();
                    String post = args.path("post").asText();
                    List<String> controls = textArray(args.path("controls"));
                    log.println("[askamerica-mcp] tool=diff_in_diff outcome=" + outcome);
                    StatsOutput r = diffInDiffTool(sql, outcome, treatment, post, controls);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "hypothesis_test": {
                    String sql = args.path("sql").asText();
                    String test = args.path("test").asText();
                    String valueCol = args.has("value_col") && !args.get("value_col").isNull()
                        ? args.get("value_col").asText() : null;
                    String groupCol = args.has("group_col") && !args.get("group_col").isNull()
                        ? args.get("group_col").asText() : null;
                    Double oneSampleMu = args.has("one_sample_mu")
                        && !args.get("one_sample_mu").isNull()
                        ? args.get("one_sample_mu").asDouble() : null;
                    String rowCol = args.has("row_col") && !args.get("row_col").isNull()
                        ? args.get("row_col").asText() : null;
                    String colCol = args.has("col_col") && !args.get("col_col").isNull()
                        ? args.get("col_col").asText() : null;
                    log.println("[askamerica-mcp] tool=hypothesis_test test=" + test);
                    StatsOutput r = hypothesisTestTool(sql, test, valueCol, groupCol, oneSampleMu,
                        rowCol, colCol);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "panel_fixed_effects": {
                    String sql = args.path("sql").asText();
                    String outcome = args.path("outcome").asText();
                    List<String> predictors = textArray(args.path("predictors"));
                    String entityCol = args.path("entity_col").asText();
                    String timeCol = args.path("time_col").asText();
                    String feCluster = args.has("cluster_col")
                        && !args.get("cluster_col").isNull()
                        ? args.get("cluster_col").asText() : null;
                    log.println("[askamerica-mcp] tool=panel_fixed_effects outcome=" + outcome
                        + " cluster=" + feCluster);
                    StatsOutput r = panelFixedEffectsTool(sql, outcome, predictors, entityCol,
                        timeCol, feCluster);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "robust_regression": {
                    String sql = args.path("sql").asText();
                    String outcome = args.path("outcome").asText();
                    List<String> predictors = textArray(args.path("predictors"));
                    String clusterCol = args.has("cluster_col") && !args.get("cluster_col").isNull()
                        ? args.get("cluster_col").asText() : null;
                    log.println("[askamerica-mcp] tool=robust_regression outcome=" + outcome);
                    StatsOutput r = robustRegressionTool(sql, outcome, predictors, clusterCol);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "flexible_regression": {
                    String sql = args.path("sql").asText();
                    String outcome = args.path("outcome").asText();
                    List<String> predictors = textArray(args.path("predictors"));
                    String method = args.has("method") && !args.get("method").isNull()
                        ? args.get("method").asText() : null;
                    log.println("[askamerica-mcp] tool=flexible_regression outcome=" + outcome);
                    StatsOutput r = flexibleRegressionTool(sql, outcome, predictors, method);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "feature_importance": {
                    String sql = args.path("sql").asText();
                    String outcome = args.path("outcome").asText();
                    List<String> predictors = textArray(args.path("predictors"));
                    String method = args.has("method") && !args.get("method").isNull()
                        ? args.get("method").asText() : null;
                    log.println("[askamerica-mcp] tool=feature_importance outcome=" + outcome);
                    StatsOutput r = featureImportanceTool(sql, outcome, predictors, method);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "double_ml_ate": {
                    String sql = args.path("sql").asText();
                    String outcome = args.path("outcome").asText();
                    String treatment = args.path("treatment").asText();
                    List<String> controls = textArray(args.path("controls"));
                    Integer folds = args.has("folds") && !args.get("folds").isNull()
                        ? args.get("folds").asInt() : null;
                    String method = args.has("method") && !args.get("method").isNull()
                        ? args.get("method").asText() : null;
                    log.println("[askamerica-mcp] tool=double_ml_ate outcome=" + outcome
                        + " treatment=" + treatment);
                    StatsOutput r = doubleMlAteTool(sql, outcome, treatment, controls, folds, method);
                    text = r.text;
                    diagnostics = r.diagnostics;
                    break;
                }
                case "publish_report": {
                    String rTitle = args.path("title").asText(null);
                    String rSub = args.has("subtitle") && !args.get("subtitle").isNull()
                        ? args.get("subtitle").asText() : null;
                    java.util.List<ReportPage.Section> secs = new java.util.ArrayList<>();
                    for (JsonNode sec : args.path("sections")) {
                        secs.add(new ReportPage.Section(
                            sec.path("heading").asText(null), sec.path("html").asText("")));
                    }
                    java.util.List<ReportPage.Source> srcs = new java.util.ArrayList<>();
                    for (JsonNode src : args.path("sources")) {
                        srcs.add(new ReportPage.Source(src.path("label").asText(null),
                            src.path("url").asText(null),
                            src.has("note") ? src.get("note").asText(null) : null));
                    }
                    String boardSvg = null;
                    String boardSvgUrl = null;
                    byte[] thumb = null;
                    JsonNode dash = args.path("dashboard");
                    if (dash.isObject() && dash.has("panels")) {
                        java.util.List<DashboardLayout.Panel> ps = new java.util.ArrayList<>();
                        for (JsonNode pn : dash.path("panels")) {
                            ps.add(readPanel(pn));
                        }
                        int c = dash.has("columns")
                            ? Math.min(Math.max(1, dash.get("columns").asInt()), 4) : 2;
                        int[] sz = DashboardLayout.defaultSize(ps, c);
                        DashboardLayout.Dashboard board = DashboardLayout.compose(
                            dash.path("title").asText(null),
                            dash.has("subtitle") ? dash.get("subtitle").asText(null) : null,
                            dash.has("footnote") ? dash.get("footnote").asText(null) : null,
                            dash.has("byline") ? dash.get("byline").asText(null) : null,
                            ps, c, sz[0], sz[1]);
                        boardSvg = board.toSvg();
                        boardSvgUrl = ArtifactServer.publishSvg(boardSvg);
                        // A 40% render. A quarter scale was cheaper still but showed only the
                        // shape of the board — panel count and which way the lines went — with
                        // every label illegible. At 40% the titles and headline figures can be
                        // read, which is the difference between a picture that says "open the
                        // link" and one that answers the question at a glance. Roughly an
                        // eighth of a full-size board's image tokens either way.
                        thumb = board.toPng(0.40);
                    }
                    chartPng = thumb;
                    String html = ReportPage.render(rTitle, rSub, secs, boardSvg, boardSvgUrl,
                        srcs,
                        args.has("footnote") ? args.get("footnote").asText(null) : null,
                        args.has("byline") ? args.get("byline").asText(null) : null);
                    String url = ArtifactServer.publish(
                        html.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        "text/html; charset=utf-8", "html");
                    log.println("[askamerica-mcp] tool=publish_report sections=" + secs.size()
                        + " sources=" + srcs.size() + " board=" + (boardSvg != null));
                    text = url == null
                        ? "Report built (" + html.length() + " bytes) but no local server is "
                            + "available to serve it."
                        : "Report published: " + url + "\n\nGive the reader that link — it is "
                        + "the whole answer in one page: " + secs.size() + " section(s), "
                        + srcs.size() + " citation(s)"
                        + (boardSvg == null ? "" : ", dashboard inlined")
                        + ". It is self-contained and local to this machine.";
                    break;
                }
                case "compose_dashboard": {
                    java.util.List<DashboardLayout.Panel> panels = new java.util.ArrayList<>();
                    for (JsonNode pn : args.path("panels")) {
                        panels.add(readPanel(pn));
                    }
                    int cols = args.has("columns")
                        ? Math.min(Math.max(1, args.get("columns").asInt()), 4) : 2;
                    int[] size = DashboardLayout.defaultSize(panels, cols);
                    int dw = args.has("width")
                        ? Math.min(Math.max(300, args.get("width").asInt()), 2400) : size[0];
                    int dh = args.has("height")
                        ? Math.min(Math.max(200, args.get("height").asInt()), 2400) : size[1];
                    String dTitle = args.has("title") && !args.get("title").isNull()
                        ? args.get("title").asText() : null;
                    String dSub = args.has("subtitle") && !args.get("subtitle").isNull()
                        ? args.get("subtitle").asText() : null;
                    String dFoot = args.has("footnote") && !args.get("footnote").isNull()
                        ? args.get("footnote").asText() : null;
                    log.println("[askamerica-mcp] tool=compose_dashboard panels="
                        + panels.size() + " cols=" + cols);
                    String dBy = args.has("byline") && !args.get("byline").isNull()
                        ? args.get("byline").asText() : null;
                    DashboardLayout.Dashboard dash = DashboardLayout.compose(
                        dTitle, dSub, dFoot, dBy, panels, cols, dw, dh);
                    chartPng = dash.toPng();
                    chartSvg = dash.toSvg();
                    int stats = 0;
                    for (DashboardLayout.Panel p : panels) {
                        if ("stat".equals(p.kind)) {
                            stats++;
                        }
                    }
                    String dashUrl = ArtifactServer.publishSvg(chartSvg);
                    boolean wantSvg = args.has("include_svg")
                        && args.get("include_svg").asBoolean(false);
                    text = "Composed a " + cols + "-column dashboard"
                        + (dTitle == null ? "" : " '" + dTitle + "'") + " — "
                        + (panels.size() - stats) + " chart panel(s), " + stats
                        + " stat tile(s).\n\n"
                        + (dashUrl == null ? ""
                            : "GIVE THE READER THIS LINK: " + dashUrl + "\n"
                            + "It opens the dashboard full size in a browser, on this machine "
                            + "only. Share the link — do NOT paste the SVG below into your "
                            + "reply. The SVG is roughly 7,000 tokens; the link is twenty, and "
                            + "it shows the same picture.\n\n")
                        + "The image above is the same board as a PNG, already viewable "
                        + "inline."
                        + (wantSvg
                            ? (oversizeSvgNotice(chartSvg, dashUrl) == null
                                ? " The block after this is the SVG source you asked for. Panel "
                                + "ids are namespaced p1-, p2-, ... so per-panel edits work "
                                + "(p2-mark-california), and each panel has a pN-annotations "
                                + "group that paints last."
                                : " " + oversizeSvgNotice(chartSvg, dashUrl))
                            : " To edit the chart — a callout, a greyed-out category, direct "
                            + "value labels — call again with include_svg:true to get the "
                            + "editable source. It is not returned by default because it costs "
                            + "roughly 7,000 tokens and the link already shows the board.");
                    if (!wantSvg || oversizeSvgNotice(chartSvg, dashUrl) != null) {
                        chartSvg = null;
                    }
                    break;
                }
                case "render_chart": {
                    // Reject unrecognised argument names instead of ignoring them.
                    //
                    // chart_type used to fall back to "line" whenever it was absent, which meant a
                    // caller that named it anything else got a line chart and a success response.
                    // Observed 2026-08-17: an agent passed "type" seven times; the first six
                    // failed with errors about categories/series that had nothing to do with the
                    // real mistake (its points payload was routed down the categories path because
                    // the type had silently become "line"), and the seventh "succeeded" into a
                    // line chart of two series on incompatible scales — unreadable, and reported
                    // as a bar chart because that is what the caller thought it had asked for.
                    // One unknown key cost six round trips and produced a wrong artifact.
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

                    if ("scatter".equals(chartType) || "bubble".equals(chartType)) {
                        java.util.List<ChartRenderer.PointSeriesSpec> points =
                            new java.util.ArrayList<>();
                        for (JsonNode s : args.path("points")) {
                            java.util.List<Double> x = new java.util.ArrayList<>();
                            for (JsonNode v : s.path("x")) {
                                x.add(v.isNull() ? null : v.asDouble());
                            }
                            java.util.List<Double> y = new java.util.ArrayList<>();
                            for (JsonNode v : s.path("y")) {
                                y.add(v.isNull() ? null : v.asDouble());
                            }
                            java.util.List<Double> size = null;
                            if (s.has("size") && !s.get("size").isNull()) {
                                size = new java.util.ArrayList<>();
                                for (JsonNode v : s.path("size")) {
                                    size.add(v.isNull() ? null : v.asDouble());
                                }
                            }
                            points.add(new ChartRenderer.PointSeriesSpec(
                                s.path("name").asText(), x, y, size));
                        }

                        log.println("[askamerica-mcp] tool=render_chart chart_type=" + chartType
                            + " points=" + points.size());
                        ChartScene pointScene = ChartRenderer.layoutPoints(
                            chartType, title, xLabel, yLabel, points, width, height);
                        chartPng = pointScene.toPng();
                        chartSvg = pointScene.toSvg();
                        text = chartSummary(chartType, title, points.size() + " point series");
                        break;
                    }

                    java.util.List<String> categories = new java.util.ArrayList<>();
                    for (JsonNode c : args.path("categories")) {
                        categories.add(c.asText());
                    }
                    java.util.List<ChartRenderer.SeriesSpec> series = new java.util.ArrayList<>();
                    for (JsonNode s : args.path("series")) {
                        java.util.List<Double> values = new java.util.ArrayList<>();
                        for (JsonNode v : s.path("values")) {
                            // A JSON null marks a missing data point and must render as a gap,
                            // not a silent 0 — v.asDouble() would coerce null to 0.0.
                            values.add(v.isNull() ? null : v.asDouble());
                        }
                        series.add(new ChartRenderer.SeriesSpec(s.path("name").asText(), values));
                    }

                    log.println("[askamerica-mcp] tool=render_chart chart_type=" + chartType
                        + " categories=" + categories.size() + " series=" + series.size());
                    ChartScene scene = ChartRenderer.layout(
                        chartType, title, xLabel, yLabel, categories, series, width, height);
                    chartPng = scene.toPng();
                    chartSvg = scene.toSvg();
                    text = chartSummary(chartType, title,
                        series.size() + " series over " + categories.size() + " categories");
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
            // The one refusal this server makes on analytical grounds rather than on a bad
            // request: the aggregate could not be evaluated on either path. It is typed here
            // so a host can route on it instead of parsing prose. Every other imperfection
            // returns its data with warnings attached; refusal is reserved for the un-runnable.
            //
            // The remedy deliberately does NOT send the caller to fetch_aligned_series any
            // more. That tool needs warehouse tables, so it is no help when the operand was
            // inline data — offering it there wasted an investigation on a workaround for a
            // defect that should have been reported.
            if (QuestionDiagnostics.isPushdownFailure(compact)) {
                ObjectNode refusal = QuestionDiagnostics.forRefusal("no_pushdown", compact,
                    "These aggregates run in DuckDB when the query pushes down and in Java "
                    + "otherwise, so both paths failing is a defect, not a query shape to "
                    + "work around. Report it with report_issue, quoting the underlying "
                    + "error above. To keep moving meanwhile, ols_regression computes the "
                    + "same relationship from any SELECT, including inline VALUES.");
                ObjectNode refusalBlock = MAPPER.createObjectNode();
                refusalBlock.put("type", "text");
                refusalBlock.put("text", refusal.toString());
                errContent.add(refusalBlock);
            }
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

        // The same scene as editable markup, alongside the picture. The PNG is what a host
        // displays — MCP image blocks render png/jpeg, not svg — but a raster is opaque to the
        // caller that asked for it: it cannot check the chart against its own numbers, and its
        // only way to change one label is to render the whole thing again. The SVG carries ids
        // and classes for exactly that, and both come from one set of coordinates, so the
        // markup the caller edits is provably the picture the reader saw.
        if (chartSvg != null) {
            ObjectNode svgBlock = MAPPER.createObjectNode();
            svgBlock.put("type", "text");
            svgBlock.put("text", chartSvg);
            content.add(svgBlock);
        }

        // The envelope rides as its own block, after the data. Merging it into the payload
        // would change the bytes every existing host parses; as a sibling block it is additive
        // — a host that ignores it reads exactly what it read before, and one that reads it
        // can re-query with a control, change grain, or hedge.
        addRepairNotice(diagnostics);
        if (diagnostics != null) {
            ObjectNode diagBlock = MAPPER.createObjectNode();
            diagBlock.put("type", "text");
            diagBlock.put("text", diagnostics.toString());
            content.add(diagBlock);
        }
        // A tool with no diagnostics envelope still owes the caller the rewrite it made.
        String standaloneRepair = LAST_REPAIR_NOTICE.get();
        LAST_REPAIR_NOTICE.remove();
        if (standaloneRepair != null && diagnostics == null) {
            ObjectNode noticeBlock = MAPPER.createObjectNode();
            noticeBlock.put("type", "text");
            noticeBlock.put("text", "Note: " + standaloneRepair);
            content.add(noticeBlock);
        }

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
     *       These aggregates now have real Java implementations, so failing to push down is
     *       normally not an error at all — the query runs locally and returns the same value.
     *       A failure reaching here therefore means the LOCAL path broke, and must not be
     *       explained as a cross-schema join: that explanation is simply false when the
     *       operand is a derived relation (a {@code VALUES} literal has neither join nor
     *       schema), and stating it sent a real investigation chasing a problem that did not
     *       exist. Carry the underlying error instead of guessing.</li>
     * </ul>
     */
    static String compactErrorMessage(Throwable e) {
        // TRY_CAST is correct DuckDB and correct in most dialects, and callers reach for it
        // constantly on this warehouse because so many measure columns are VARCHAR. Calcite's
        // parser does not know it: it reads the AS as an argument separator and reports a
        // signature like TRY_CAST(<CHARACTER>, <NUMERIC>), which names nothing the caller can
        // act on. It cannot simply be rewritten to CAST — TRY_CAST yields NULL where CAST
        // throws, so that rewrite converts a working query into a failing one on exactly the
        // dirty data TRY_CAST exists for. Say what is wrong and give the equivalent that parses.
        for (Throwable t = e; t != null; t = safeCause(t)) {
            String msg = t.getMessage();
            if (msg != null && msg.contains("TRY_CAST")
                && msg.contains("No match found for function signature")) {
                return "TRY_CAST is not supported by this SQL parser (the engine reports it as an"
                    + " unknown two-argument function, because it reads the AS as a separator)."
                    + " It is NOT the same as CAST and is not rewritten to it: TRY_CAST returns"
                    + " NULL where CAST fails, which is usually the whole reason for using it."
                    + " For a VARCHAR measure column, the equivalent that parses here is a"
                    + " guarded CAST, e.g."
                    + " CASE WHEN <col> IS NOT NULL AND TRIM(<col>) <> '' THEN CAST(<col> AS"
                    + " DOUBLE) END — add whatever further guard the column needs. Original"
                    + " error: " + msg;
            }
        }
        // A query stopped by the time bound surfaces as DuckDB's own "INTERRUPT Error:
        // Interrupted!", which names neither the bound nor the fact that one exists. An agent
        // reading it cannot tell a timeout from a crash, so it retries the same shape and loses
        // the same minutes again — observed live, twice in a row on one table. Say what
        // happened and what would change the outcome.
        for (Throwable t = e; t != null; t = safeCause(t)) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("INTERRUPT Error") || msg.contains("Interrupted!"))) {
                int secs = queryTimeoutSeconds();
                return "The query was stopped after " + secs + "s by the per-query time bound"
                    + " (ASKAMERICA_QUERY_TIMEOUT_SECONDS). It was not a crash and the data is"
                    + " not missing — the statement was still running. If the query scans"
                    + " broadly, narrowing it usually helps: filter on the partition columns,"
                    + " shorten the year range, or aggregate in SQL rather than scanning rows."
                    + " DISTINCT and ORDER BY are common culprits, since both must read"
                    + " everything before returning a first row. But if an already-narrow query"
                    + " times out — few columns, tight filters, a small FETCH FIRST — then the"
                    + " table itself is the problem, not the query: stop rewriting it, source"
                    + " the figure elsewhere, and file it with report_issue naming the exact"
                    + " statement. Do not read a timeout as evidence about the data's contents.";
            }
        }
        for (Throwable t = e; t != null; t = safeCause(t)) {
            String msg = t.getMessage();
            boolean compileShape = msg != null
                && msg.contains("No applicable constructor/method found")
                && msg.contains("DuckDBStatsFunctions$");
            // The stub's own throw, from when these aggregates had no Java implementation and
            // result() refused outright. Nothing raises it any more — the implementations are
            // real — but an older engine jar in the same deployment still can, so the shape is
            // still recognised rather than leaked as an unhandled message.
            boolean stubShape = msg != null
                && msg.contains("is a DuckDB-only aggregate and must be pushed down");
            if (compileShape || stubShape) {
                // These aggregates have Java implementations (DuckDBStatsFunctions), so a
                // failure here is NOT the old "cannot run outside DuckDB" limitation and must
                // not be reported as one. It means the local path itself broke — typically a
                // type the generated code could not bind. Carry the real message: guessing a
                // cause the caller cannot verify is what sent an earlier investigation looking
                // for a cross-schema join that did not exist.
                return "A statistical aggregate (corr, regr_*, median, skewness, kurtosis, mad, "
                    + "quantile_cont, or quantile_disc) could not be evaluated. It neither "
                    + "pushed down to DuckDB nor ran locally. Underlying error: " + msg;
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
    /**
     * Points {@code EmbeddingService} at a query-time embedder, so {@code SEMANTIC_SEARCH} can
     * embed the query rather than failing with "no embedder configured".
     *
     * <p>Nothing else in the product sets {@code calcite.embed.*}, so without this the consumer
     * half of semantic search could never start: {@code vss-local.py} writes the corpus codes
     * and the query side had no way to reach the same vector space.
     *
     * <p>Order matters and is not arbitrary:
     * <ol>
     *   <li>an explicit {@code calcite.embed.command|home|script} always wins — never override a
     *       deliberate choice;</li>
     *   <li>a bundle home ({@code bin/hugot-embed}, {@code lib/}, {@code model/}) under the
     *       operating dir — the self-contained option, the only one viable on a client device;</li>
     *   <li>the CPU embed venv + {@code embed.py} that {@code vss-embed-setup.sh} provisions —
     *       the dev/ETL-box path, which needs torch and is not client-shippable.</li>
     * </ol>
     *
     * <p>Resolution only; it never installs anything. If neither exists the properties stay unset
     * and {@code SEMANTIC_SEARCH} reports the same explicit error as before — a wrong embedder
     * would be far worse than none, because query vectors from a different pipeline than the
     * corpus still return rows, just silently mis-ranked.
     */
    private static void configureQueryEmbedder() {
        if (!System.getProperty("calcite.embed.command", "").isEmpty()
            || !System.getProperty("calcite.embed.home", "").isEmpty()
            || !System.getProperty("calcite.embed.script", "").isEmpty()) {
            log.println("[askamerica-mcp] embedder: explicitly configured, leaving as-is");
            return;
        }

        String dataDir = System.getProperty("ASKAMERICA_DATA_DIR", "");
        if (!dataDir.isEmpty()) {
            java.io.File home = new java.io.File(dataDir, "embedder");
            if (new java.io.File(home, "bin/hugot-embed").isFile()) {
                System.setProperty("calcite.embed.home", home.getAbsolutePath());
                log.println("[askamerica-mcp] embedder: bundle home " + home.getAbsolutePath());
                return;
            }
        }

        String venv = System.getenv("VSS_EMBED_VENV");
        String govdataHome = System.getenv("GOVDATA_HOME");
        java.io.File py = (venv != null && !venv.isEmpty())
            ? new java.io.File(venv, "bin/python")
            : (govdataHome != null && !govdataHome.isEmpty()
                ? new java.io.File(govdataHome, "build/.venv-embed/bin/python") : null);
        java.io.File script = (govdataHome != null && !govdataHome.isEmpty())
            ? new java.io.File(govdataHome, "scripts/embed.py") : null;
        if (py != null && py.canExecute() && script != null && script.isFile()) {
            System.setProperty("calcite.embed.python", py.getAbsolutePath());
            System.setProperty("calcite.embed.script", script.getAbsolutePath());
            log.println("[askamerica-mcp] embedder: venv " + py.getAbsolutePath()
                + " + " + script.getAbsolutePath());
            return;
        }

        log.println("[askamerica-mcp] embedder: none found — semantic_search/SEMANTIC_SEARCH will "
            + "report 'no embedder configured'. Provide a bundle at <data-dir>/embedder "
            + "(bin/hugot-embed, lib/, model/), or set GOVDATA_HOME with the venv from "
            + "govdata/scripts/vss-embed-setup.sh.");
    }

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

    /**
     * Keyword search over the catalog.
     *
     * <p>An empty {@code []} used to stand for three unrelated conditions: the caller passed no
     * query, the catalog was not loaded, and the catalog was searched and held nothing. Only the
     * last is a real answer, and the other two are indistinguishable from it — a caller that
     * asked for "income" and got {@code []} concluded the warehouse had no income tables and
     * stopped looking, when in fact four such tables exist and the query had never arrived.
     * The first case is now rejected before reaching here; the second says so; and a genuine
     * miss names the fallback that does work.
     */
    private static String searchCatalog(String query, int limit) {
        if (query == null || query.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "search_catalog requires a non-empty 'query'.");
        }
        if (!Catalog.available()) {
            throw new IllegalStateException(
                "The catalog is not loaded, so search_catalog cannot answer. This is a server "
                + "state problem, not an empty result — use list_schemas and list_tables, "
                + "which read the live connection instead.");
        }
        ArrayNode hits = Catalog.search(query.trim(), limit);
        if (hits.size() == 0) {
            ObjectNode empty = MAPPER.createObjectNode();
            empty.put("matches", 0);
            empty.put("query", query.trim());
            empty.put("hint",
                "No catalog entry matched. The catalog was searched and is loaded, so this is a "
                + "real miss: try fewer or more general words (one noun beats a phrase), or "
                + "list_schemas then list_tables to browse. Do not conclude the data is absent "
                + "from one unmatched search.");
            return empty.toString();
        }
        return hits.toString();
    }

    /**
     * Maps the SQL-standard {@code information_schema} TABLE_TYPE to the vocabulary the MCP
     * tools expose: "BASE TABLE" becomes "TABLE"; "VIEW" (and anything else) passes through.
     */
    private static String mcpTableType(String informationSchemaType) {
        return "BASE TABLE".equals(informationSchemaType) ? "TABLE" : informationSchemaType;
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
                o.put("type", mcpTableType(rs.getString(2)));
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
                out.put("type", mcpTableType(rs.getString(1)));
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

    // ── data_coverage ────────────────────────────────────────────────────────

    /**
     * What a table actually holds, year by year, against what its schema declares.
     *
     * <p>{@code describe_table} answers with the declared window because it must stay fast.
     * That window is a statement of intent, and the two diverge for ordinary reasons — a
     * backfill still running, a source that skipped a year, a partition that failed to
     * materialize and was never retried. The difference is invisible in query results: a
     * year that was never loaded returns the same empty set as a year that genuinely holds
     * no matching rows, and an average over a series with a hole in it is computed over the
     * years that survived without saying so.
     *
     * <p>So this runs the scan the fast path refuses to, and reports both windows plus the
     * years each one has that the other does not.
     */
    private static String dataCoverage(String schema, String table) throws Exception {
        String s = safeIdent(schema);
        String t = safeIdent(table);

        ObjectNode out = MAPPER.createObjectNode();
        out.put("schema", s);
        out.put("table", t);

        ObjectNode declared = Catalog.coverage(s, t);
        String yearCol = declared != null
            ? declared.path("column").asText("year") : yearColumnOf(s, t);
        if (yearCol == null) {
            out.put("status", "no_year_column");
            out.put("note", "This table has no year column to scan, so it has no year "
                + "coverage to report. Either it is a reference table that carries no time "
                + "dimension, or the name is wrong — check list_tables(" + s + ").");
            return out.toString();
        }
        out.put("year_column", yearCol);
        if (declared != null) {
            out.set("declared", declared);
        }

        IngestedYears.Result r = IngestedYears.measure(s, t, yearCol);
        if (r == null) {
            out.put("status", "measuring");
            out.put("note", "The scan did not finish in time and is still running — it will "
                + "be cached when it lands, so calling data_coverage again shortly will "
                + "return it. No coverage is reported here; do not read this as an empty "
                + "table.");
            return out.toString();
        }
        out.set("observed", IngestedYears.detail(r));

        if (declared != null && r.status == null) {
            out.set("missing_vs_declared", IngestedYears.missingVersusDeclared(r,
                intOrNull(declared, "first_year"), intOrNull(declared, "last_year")));
        }
        out.put("note", "observed.years_present and observed.rows_by_year come from a row "
            + "scan of this table, not from the schema. interior_gaps are years between the "
            + "first and last loaded year that hold no rows at all — a series crossing one "
            + "is discontinuous, and any trend, year-over-year change, or regression over it "
            + "silently spans the hole. missing_vs_declared are years the schema declares "
            + "but the table does not hold. A year that IS present may still be partially "
            + "loaded: this counts rows, it does not verify that a year is complete.");
        log.println("[askamerica-mcp] data_coverage " + s + "." + t
            + " status=" + (r.status == null ? "measured" : r.status));
        return out.toString();
    }

    private static Integer intOrNull(ObjectNode n, String field) {
        return n.hasNonNull(field) ? Integer.valueOf(n.get(field).asInt()) : null;
    }

    /**
     * The year column of a table that declares no coverage window — views, mostly. Reads the
     * resolved row type rather than the catalog, which is why it can answer for a view at
     * all. Throws when the table itself is unknown, since reporting "no year column" for a
     * misspelled name would send the caller looking for the wrong problem.
     */
    private static String yearColumnOf(String schema, String table) throws Exception {
        Connection c = getCatalogConnection();
        ArrayNode cols = MAPPER.createArrayNode();
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT column_name, data_type FROM information_schema.columns "
                 + "WHERE lower(table_schema) = '" + schema + "' "
                 + "AND lower(table_name) = '" + table + "' ORDER BY ordinal_position")) {
            while (rs.next()) {
                ObjectNode col = MAPPER.createObjectNode();
                col.put("name", rs.getString(1));
                col.put("type", rs.getString(2));
                cols.add(col);
            }
        }
        if (cols.size() == 0) {
            throw new IllegalArgumentException("no table or view named " + schema + "."
                + table + " — call list_tables('" + schema + "') for the names that exist");
        }
        return resolvedYearColumn(cols);
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
     * Normalise caller SQL: quote reserved words used as identifiers, and accept {@code !=}.
     *
     * <p>Both exist so a caller writing ordinary SQL is not made to learn this engine's quirks.
     * {@code !=} is spelled that way in every mainstream dialect and rejected here only by
     * Calcite's conformance level, which is a fact about our configuration rather than about the
     * query; it is rewritten to {@code <>}. Doing it in this pass rather than a second one is
     * deliberate — the scan below already skips string literals, quoted identifiers and
     * comments, and a naive replace would corrupt {@code WHERE note = 'a != b'}.
     *
     * <p>Only dot-adjacent tokens are QUOTED, because those are the positions where a token
     * is unambiguously an identifier: the token before a {@code .} is a schema or table
     * qualifier, the token after a {@code .} is a table or column name. A bare {@code YEAR} is
     * left untouched — it may be {@code EXTRACT(YEAR FROM d)} or {@code ORDER BY}, and quoting
     * those breaks a query that would otherwise parse. String literals, quoted identifiers and
     * comments are skipped: the previous regex rewrote inside them, so
     * {@code WHERE note = 'see ref.table'} came out malformed.
     */
    static String normalizeCallerSql(String sql) {
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
            if (ch == '!' && i + 1 < n && sql.charAt(i + 1) == '=') {
                out.append("<>");
                i += 2;
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

    private static ArrayNode query(String sql, int limit) throws Exception {
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
            return runSqlRows(sql, limit);
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

    /**
     * The rewrite applied to the statement now being handled, or null when none was.
     *
     * <p>Per-thread because one worker handles one call at a time, and the notice has to travel
     * from deep inside the execution path back out to the response the caller reads.
     */
    private static final ThreadLocal<String> LAST_REPAIR_NOTICE = new ThreadLocal<>();

    /**
     * Runs caller-supplied SQL, quoting reserved-word column names if that is what broke it.
     *
     * <p>This is the single place caller SQL reaches the database, and the repair has to live
     * here rather than in one tool. It was first wired into {@code query} alone, and an eval run
     * immediately found the hole: the same {@code CAST(year AS INTEGER)} that {@code query} now
     * accepts still failed under {@code adjust_inflation}, so the caller met the identical error
     * one tool over and paid for it a second time. {@code per_capita},
     * {@code fetch_aligned_series} and the statistical tools all took SQL through their own
     * executeQuery calls and had the same gap.
     *
     * <p>Attempted only when the statement failed to parse, and kept only if the rewrite then
     * runs, so a working statement is never touched and a real error is never replaced by a
     * confusing one.
     */
    static ResultSet executeWithRepair(Statement st, String sql) throws Exception {
        String effective = normalizeCallerSql(sql);
        try {
            return st.executeQuery(effective);
        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg == null || !msg.contains("parse failed")) {
                throw e;
            }
            java.util.Set<String> candidates = reservedColumnWords();
            if (candidates.isEmpty()) {
                throw e;
            }
            java.util.List<String> quoted = new ArrayList<>();
            String repaired = quoteBareReservedColumns(effective, candidates, quoted);
            if (repaired.equals(effective)) {
                throw e;
            }
            ResultSet rs;
            try {
                rs = st.executeQuery(repaired);
            } catch (Exception second) {
                // The repair worked and the statement has ANOTHER defect. Rethrowing the
                // original error names the word we just fixed, which sends the caller to fix
                // something that is already correct — observed live: a statement with a bare
                // `year` AND a `!=` reported only `year`, so the agent hand-quoted it and met
                // the real error on the next round trip, concluding it must always quote by
                // hand. Report the remaining problem instead, and say plainly that the SQL was
                // rewritten first, since the message quotes text the caller did not write.
                throw new RuntimeException(
                    second.getMessage()
                    + " (reserved-word column" + (quoted.size() == 1 ? " " : "s ")
                    + String.join(", ", quoted)
                    + " were auto-quoted first; that part succeeded and this is what remains)",
                    second);
            }
            noteReservedWordRepair(quoted, repaired);
            return rs;
        }
    }

    /** Reserved words this run has already repaired, so the notice is only attached once. */
    private static final java.util.Set<String> REPAIRED_WORDS =
        java.util.Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /**
     * Records a repair so the caller learns the rule instead of only getting rows back.
     *
     * <p>Silently fixing the SQL would spend the same tokens again on the caller's next query.
     * The notice names the words and shows the statement that ran, and is emitted once per
     * word per process so a long session is not lectured on every call.
     */
    private static void noteReservedWordRepair(java.util.List<String> words, String repairedSql) {
        java.util.List<String> fresh = new ArrayList<>();
        for (String w : words) {
            if (REPAIRED_WORDS.add(w)) {
                fresh.add(w);
            }
        }
        log.println("[askamerica-mcp] repaired reserved words " + words + " -> " + repairedSql);
        LAST_REPAIR_NOTICE.set(String.join(", ", words)
            + (words.size() == 1 ? " is a SQL reserved word" : " are SQL reserved words")
            + " and must be double-quoted to be read as a column. The statement was rewritten "
            + "and run as: " + repairedSql);
    }

    /** True when the token is a SQL reserved word, per the parser's own word list. */
    static boolean isReservedWord(String token) {
        try {
            return SqlParser.create("VALUES 1").getMetadata()
                .isReservedWord(token.toUpperCase(java.util.Locale.ROOT));
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** Every column name that can appear in a query, lowercased. Built once. */
    private static volatile java.util.Set<String> QUERYABLE_COLUMNS;

    /**
     * Column names taken from the live connection, falling back to the authored catalog.
     *
     * <p>{@code information_schema} is the authority here, not the catalog JSON: partition
     * columns are declared as dimensions in the schema YAML and never reach the JSON's
     * {@code columns} array, so {@code year} — the most common reserved-word collision in this
     * warehouse — is absent from the catalog while being perfectly queryable.
     */
    private static java.util.Set<String> queryableColumnNames() {
        java.util.Set<String> cached = QUERYABLE_COLUMNS;
        if (cached != null) {
            return cached;
        }
        java.util.Set<String> names = new java.util.HashSet<>();
        try (Statement st = getCatalogConnection().createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT DISTINCT column_name FROM information_schema.columns")) {
            while (rs.next()) {
                String n = rs.getString(1);
                if (n != null && !n.isEmpty()) {
                    names.add(n.toLowerCase(java.util.Locale.ROOT));
                }
            }
        } catch (Exception e) {
            log.println("[askamerica-mcp] column list unavailable, using catalog only: "
                + e.getMessage());
        }
        if (Catalog.available()) {
            names.addAll(Catalog.allColumnNames());
        }
        java.util.Set<String> frozen = java.util.Collections.unmodifiableSet(names);
        QUERYABLE_COLUMNS = frozen;
        return frozen;
    }

    private static volatile java.util.Set<String> RESERVED_COLUMN_WORDS;

    /**
     * Words that are both SQL reserved words and column names in this warehouse.
     *
     * <p>The set a rewrite may consider. {@code year}, {@code period} and {@code value} are in
     * it; so, less obviously, are {@code count}, {@code order}, {@code desc}, {@code state} and
     * {@code type} — which is exactly why membership alone cannot justify quoting a word.
     */
    private static java.util.Set<String> reservedColumnWords() {
        java.util.Set<String> cached = RESERVED_COLUMN_WORDS;
        if (cached != null) {
            return cached;
        }
        java.util.Set<String> words = new java.util.HashSet<>();
        for (String column : queryableColumnNames()) {
            if (isReservedWord(column)) {
                words.add(column);
            }
        }
        java.util.Set<String> frozen = java.util.Collections.unmodifiableSet(words);
        RESERVED_COLUMN_WORDS = frozen;
        return frozen;
    }

    /**
     * Tokens after which a word must be a name rather than syntax.
     *
     * <p>This is the whole discriminator. {@code count} names a column somewhere in this
     * warehouse and is also how everyone writes an aggregate; {@code order} names a column and
     * is also half of {@code ORDER BY}. Quoting either on the strength of the name alone turns
     * {@code COUNT(*)} into {@code "count"(*)} and breaks a statement that was nearly right.
     * What separates the two uses is position: a word sitting where a column reference belongs
     * — just after SELECT, a comma, BY, WHERE, AND, a dot, an operator — is a name, and a word
     * anywhere else is left exactly as written.
     */
    private static final java.util.Set<String> IDENTIFIER_POSITION_TOKENS =
        new java.util.HashSet<>(java.util.Arrays.asList(
            "SELECT", "WHERE", "AND", "OR", "BY", "ON", "HAVING", "AS", "SET", "DISTINCT",
            "NOT", "THEN", "ELSE", "WHEN", "OVER", "USING",
            ",", "(", ".", "=", "<", ">", "+", "-", "*", "/"));

    /**
     * Double-quotes reserved words used as column references, leaving syntax untouched.
     *
     * <p>Skips string literals and already-quoted names, matches whole words only, and emits
     * the lowercase form because the engine folds unquoted identifiers to lowercase — so
     * lowercase is what the caller's bare spelling already meant. A word directly followed by
     * an opening parenthesis is a function call and is never quoted, which is what keeps
     * {@code COUNT(*)} intact even though {@code count} is a column name here.
     *
     * <p>This is the bare-token counterpart to {@link #normalizeCallerSql(String)}, which
     * rewrites only dot-adjacent tokens because that is the one position it can be certain of
     * without risking a working query. Going further is safe here and only here: this runs on a
     * statement that has already failed to parse, and the rewrite is kept only if it then runs.
     *
     * <p>Every word actually rewritten is appended to {@code quoted}, so the caller can say
     * which words were repaired rather than silently changing someone's SQL.
     */
    static String quoteBareReservedColumns(String sql, java.util.Set<String> candidates,
        java.util.List<String> quoted) {
        StringBuilder out = new StringBuilder(sql.length() + 16);
        java.util.LinkedHashSet<String> seen = new java.util.LinkedHashSet<>();
        String prev = "SELECT";
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"') {
                int close = sql.indexOf(c, i + 1);
                if (close < 0) {
                    out.append(sql, i, sql.length());
                    break;
                }
                out.append(sql, i, close + 1);
                prev = "";
                i = close + 1;
                continue;
            }
            if (Character.isLetter(c) || c == '_') {
                int j = i;
                while (j < sql.length()
                    && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) {
                    j++;
                }
                String ident = sql.substring(i, j);
                String lower = ident.toLowerCase(java.util.Locale.ROOT);
                int k = j;
                while (k < sql.length() && Character.isWhitespace(sql.charAt(k))) {
                    k++;
                }
                boolean isCall = k < sql.length() && sql.charAt(k) == '(';
                if (candidates.contains(lower) && !isCall
                    && IDENTIFIER_POSITION_TOKENS.contains(prev)) {
                    out.append('"').append(lower).append('"');
                    seen.add(lower);
                } else {
                    out.append(ident);
                }
                prev = ident.toUpperCase(java.util.Locale.ROOT);
                i = j;
                continue;
            }
            out.append(c);
            if (!Character.isWhitespace(c)) {
                prev = String.valueOf(c);
            }
            i++;
        }
        quoted.addAll(seen);
        return out.toString();
    }

    /** Reads one dashboard panel out of its JSON, chart or stat tile. */
    private static DashboardLayout.Panel readPanel(JsonNode pn) {
        DashboardLayout.Panel p = new DashboardLayout.Panel();
        p.kind = pn.path("type").asText("chart");
        p.span = pn.has("span") ? Math.max(1, pn.get("span").asInt()) : 1;
        p.caption = pn.has("caption") && !pn.get("caption").isNull()
            ? pn.get("caption").asText() : null;
        p.scaleGroup = pn.has("scale_group") && !pn.get("scale_group").isNull()
            ? pn.get("scale_group").asText() : null;
        if ("stat".equals(p.kind)) {
            p.label = pn.path("label").asText(null);
            p.value = pn.path("value").asText(null);
            p.delta = pn.has("delta") && !pn.get("delta").isNull()
                ? pn.get("delta").asText() : null;
            p.deltaDirection = pn.path("delta_direction").asText("flat");
            return p;
        }
        p.chartType = pn.has("chart_type") && !pn.get("chart_type").isNull()
            ? pn.get("chart_type").asText() : "line";
        p.title = pn.has("title") && !pn.get("title").isNull() ? pn.get("title").asText() : null;
        p.xLabel = pn.has("x_label") && !pn.get("x_label").isNull()
            ? pn.get("x_label").asText() : null;
        p.yLabel = pn.has("y_label") && !pn.get("y_label").isNull()
            ? pn.get("y_label").asText() : null;
        if (pn.has("points") && pn.get("points").isArray() && pn.get("points").size() > 0) {
            p.points = new java.util.ArrayList<>();
            for (JsonNode sNode : pn.path("points")) {
                java.util.List<Double> xs = new java.util.ArrayList<>();
                for (JsonNode v : sNode.path("x")) {
                    xs.add(v.isNull() ? null : v.asDouble());
                }
                java.util.List<Double> ys = new java.util.ArrayList<>();
                for (JsonNode v : sNode.path("y")) {
                    ys.add(v.isNull() ? null : v.asDouble());
                }
                java.util.List<Double> sz = null;
                if (sNode.has("size") && !sNode.get("size").isNull()) {
                    sz = new java.util.ArrayList<>();
                    for (JsonNode v : sNode.path("size")) {
                        sz.add(v.isNull() ? null : v.asDouble());
                    }
                }
                p.points.add(new ChartRenderer.PointSeriesSpec(
                    sNode.path("name").asText(), xs, ys, sz));
            }
            return p;
        }
        p.categories = new java.util.ArrayList<>();
        for (JsonNode c : pn.path("categories")) {
            p.categories.add(c.asText());
        }
        p.series = new java.util.ArrayList<>();
        for (JsonNode sNode : pn.path("series")) {
            java.util.List<Double> vals = new java.util.ArrayList<>();
            for (JsonNode v : sNode.path("values")) {
                vals.add(v.isNull() ? null : v.asDouble());
            }
            p.series.add(new ChartRenderer.SeriesSpec(sNode.path("name").asText(), vals));
        }
        if (p.categories.isEmpty() || p.series.isEmpty()) {
            throw new IllegalArgumentException(
                "chart panel '" + (p.title == null ? "untitled" : p.title)
                + "' needs categories + series (line/bar/pie) or points (scatter/bubble)");
        }
        return p;
    }

    /**
     * The text block that accompanies a rendered chart, naming what the caller can now do.
     *
     * <p>The response carries the picture and its source, and a caller that does not know the
     * source is there will not use it. Saying so once, on every chart, is what turns the SVG
     * from a second payload into the thing that gets adjusted.
     */
    private static String chartSummary(String chartType, String title, String shape) {
        return "Rendered " + chartType + " chart"
            + (title == null ? "" : " '" + title + "'") + " (" + shape + "). "
            + "The next block is the same chart as editable SVG — every mark carries an id "
            + "(mark-*, series-*, xtick-*) and every label a class (title, tick, axis-title, "
            + "value-label, callout). Edit it directly rather than re-rendering when you need "
            + "an annotation, a callout, direct value labels, or one category de-emphasised; "
            + "re-render only when the underlying data changes.";
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
        return runSqlRows(sql, limit).toString();
    }

    /** As {@link #runSqlOn} but hands back the rows themselves, so a caller that needs to
     *  inspect the result (the diagnostics envelope) does not re-parse its own JSON. The
     *  serialized form is identical either way. */
    private static ArrayNode runSqlRows(String sql, int limit) throws Exception {
        String effective = sql;
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
            ResultSet rs = executeWithRepair(stmt, effective);
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            String[] names = new String[cols];
            for (int i = 0; i < cols; i++) {
                names[i] = meta.getColumnLabel(i + 1);
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
            return arr;
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

    // ── resolve_entity ───────────────────────────────────────────────────────

    /**
     * Resolves a name or identifier against the canonical entity layer — the company/person
     * counterpart to {@link #resolveGeo}.
     *
     * <p>Reads {@code ref.canonical_org_entity} / {@code ref.canonical_person_entity}, which hold
     * ONE row per real-world entity across every source that mentions it, with each source's
     * natural key and a {@code _confidence} sibling. That is the whole point: a caller asking
     * about a company does not know whether the schema they need keys on CIK, an FEC committee
     * id, a patent assignee id, an EIN, an EIA utility id or a raw name — this returns all of
     * them at once. Matching a name LIKE against one registry (as an earlier draft of this tool
     * did) would answer for that registry only and silently miss every other context.
     *
     * <p>{@code known_in} summarises which source systems the entity was actually matched in, so
     * a caller can see at a glance whether the entity reaches the schema they intend to join,
     * without reading twenty mostly-null identifier columns.
     */
    private static String buildResolveEntitySql(String term, String type, int limit,
            String sourceSchema, String jurisdiction, boolean exactOnly,
            String aliasNorm) {
        String t = term == null ? "" : term.trim();
        if (t.isEmpty()) {
            throw new IllegalArgumentException("term must be non-empty");
        }
        String kind = (type == null || type.isEmpty()) ? "org" : type.toLowerCase(
            java.util.Locale.ROOT);
        if (!"org".equals(kind) && !"person".equals(kind)) {
            throw new IllegalArgumentException("type must be 'org' or 'person'; got " + type);
        }
        int cap = Math.min(Math.max(1, limit), 200);
        String upper = sqlStr(t.toUpperCase(java.util.Locale.ROOT));
        String like = sqlStr("%" + t.toLowerCase(java.util.Locale.ROOT) + "%");
        // SEC CIK is stored zero-padded to 10. A caller typing the unpadded number must still
        // match, so pad here rather than making them know the convention.
        String padded = sqlStr(t);
        if (t.matches("\\d{1,10}")) {
            StringBuilder sb = new StringBuilder(t);
            while (sb.length() < 10) {
                sb.insert(0, '0');
            }
            padded = sqlStr(sb.toString());
        }

        if ("person".equals(kind)) {
            return "SELECT canonical_entity_id, canonical_name "
                + "FROM ref.canonical_person_entity "
                + "WHERE lower(canonical_name) LIKE " + like + " "
                + "OR canonical_entity_id = " + sqlStr(t) + " "
                + "ORDER BY canonical_name "
                + "FETCH FIRST " + cap + " ROWS ONLY";
        }

        // ref.entity_org_bridge is the search surface, not canonical_org_entity. The bridge
        // holds ONE ROW PER NAME MENTION across every org-type source, already normalized
        // (lowercased, punctuation and legal suffixes stripped) and already scored, so every
        // variant a caller might type is indexed there. canonical_org_entity carries a single
        // canonical_name per entity, so searching it means matching one spelling and then
        // bolting on a join per identifier type to cover the rest — which is what the first
        // version of this method did, and why "alphabet" returned SBA borrowers.
        //
        // Ranking comes from the data too: match_score is 1.0 for exact matches and the raw
        // jaro_winkler similarity for fuzzy ones, so ordering by it reproduces what the
        // previous version hand-built out of a CASE ladder, and does it better.
        String norm = normalizeOrgName(t);
        String entityKey = "COALESCE(lei, sec_cik, source_name_normalized)";
        // Scoring, not tiering: one pass, with the name score defined as
        //   exact normalized match            -> 1.0
        //   fails the word-boundary test      -> 0   (dropped)
        //   otherwise                         -> jaro_winkler similarity
        //
        // The word-boundary test is a recall filter and jaro-winkler is the precision
        // filter, ANDed: a candidate must both contain the term as a whole word AND be
        // string-similar to it. Unanchored substring alone matched INSIDE words (the ticker
        // AAPL returned MAAPLE CORP); jaro-winkler alone would rank every short name against
        // every other. Together they keep "Alphabet Inc" while dropping "MAAPLE".
        //
        // jaro_winkler_similarity is deliberate rather than an embedding: it is the SAME
        // function the bridge itself scored its fuzzy matches with (match_score is documented
        // as 1.0 for exact, raw jaro_winkler for fuzzy), it needs no vector index, and it
        // keeps "Alphabet Inc" distinct from "Alphabet Energy Inc" — a distinction embedding
        // similarity blurs precisely where this data is already ambiguous.
        String normLit = sqlStr(norm);
        // Scoring uses both anchors, but only the PREFIX form goes in the WHERE clause below.
        // A leading wildcard ('% alphabet%') defeats every min/max and zone-map prune the
        // Parquet reader has, forcing a full scan of 1.07M rows — measured at 168s, and a
        // 183s timeout for a term with more matches. The prefix form prunes.
        String wordBoundary = "(source_name_normalized LIKE " + sqlStr(norm + "%")
            + " OR source_name_normalized LIKE " + sqlStr("% " + norm + "%") + ")";
        String prunablePredicate = "source_name_normalized LIKE " + sqlStr(norm + "%");
        String nameScore =
            "CASE WHEN source_name_normalized = " + normLit + " THEN 1.0 "
            + (aliasNorm == null || aliasNorm.isEmpty() ? ""
                : "WHEN source_name_normalized = " + sqlStr(aliasNorm) + " THEN 1.0 ")
            + "WHEN " + wordBoundary + " THEN JARO_WINKLER("
            + "source_name_normalized, " + normLit + ") "
            + "ELSE 0 END";
        // Identifier hits are exact by construction and bypass name scoring entirely. The
        // ticker path reaches canonical_org_entity, NOT the bridge: bridge.sec_cik is
        // populated only for EIN-path matches, so a ticker resolved through it found nothing.
        // A ticker is NOT matched as an identifier here. entity_org_bridge.sec_cik is populated
        // only for EIN-path matches, so a filer matched through GLEIF carries a lei and a null
        // sec_cik — AAPL resolved to CIK 0000320193 and then matched no bridge row at all.
        // canonical_org_entity.sec_cik is populated the same way and is equally sparse. The
        // ticker is instead resolved to its registered name by the caller and passed in as
        // aliasNorm, turning it into a name match, which the bridge does index.
        String identifierHit = "upper(lei) = " + upper + " OR sec_cik = " + padded;

        return "WITH raw AS ("
            + "SELECT " + entityKey + " AS entity_key, lei, sec_cik, gleif_legal_name, "
            + "match_score, match_method, match_confidence, source_schema, source_name_raw, "
            + "CASE WHEN " + identifierHit + " THEN 1.0 ELSE " + nameScore + " END AS name_score "
            + "FROM ref.entity_org_bridge "
            + "WHERE (" + identifierHit
            + (exactOnly ? " OR source_name_normalized = " + normLit
                         : " OR " + prunablePredicate)
            + (aliasNorm == null || aliasNorm.isEmpty() ? ""
                : (exactOnly ? " OR source_name_normalized = " + sqlStr(aliasNorm)
                             : " OR source_name_normalized LIKE " + sqlStr(aliasNorm + "%")))
            + ")"
            + (sourceSchema == null || sourceSchema.isEmpty() ? ""
                : " AND source_schema = " + sqlStr(sourceSchema)) + "), "
            // Pre-deduplicate before aggregating. string_agg(DISTINCT ...) did not dedupe
            // through this stack — matched_in came back as "patents sec sec sec" — so the
            // DISTINCT is applied here, where it is a plain relational operation.
            //
            // Single-argument string_agg (default ',' separator) because the two-argument
            // form fails with "Separator argument to StringAgg must be a constant": the
            // literal does not survive Calcite's translation as a constant.
            + "sch AS (SELECT DISTINCT entity_key, source_schema FROM raw), "
            + "nm AS (SELECT DISTINCT entity_key, source_name_raw FROM raw), "
            + "agg_sch AS (SELECT entity_key, string_agg(source_schema) AS matched_in "
            + "FROM sch GROUP BY entity_key), "
            + "agg_nm AS (SELECT entity_key, string_agg(source_name_raw) AS variants, "
            + "COUNT(*) AS name_variants FROM nm GROUP BY entity_key), "
            + "m AS ("
            + "SELECT r.entity_key, "
            + "MAX(r.lei) AS lei, MAX(r.sec_cik) AS sec_cik, "
            + "MAX(r.gleif_legal_name) AS gleif_legal_name, "
            + "MAX(r.match_score) AS best_match_score, "
            + "MAX(r.match_method) AS match_method, "
            + "MAX(r.match_confidence) AS match_confidence, "
            + "MAX(r.name_score) AS name_score, "
            + "COUNT(*) AS mentions "
            + "FROM raw r WHERE r.name_score > 0 GROUP BY r.entity_key) "
            // Join the canonical row on whichever key the bridge resolved, so a caller gets
            // the canonical identity AND the variants that led to it in one result.
            + "SELECT c.canonical_entity_id, "
            + "COALESCE(c.canonical_name, m.gleif_legal_name, m.entity_key) AS canonical_name, "
            + "m.lei, m.sec_cik, c.fec_committee_id, c.patents_assignee_id, c.exempt_org_ein, "
            + "c.eia_utility_id, c.fmcsa_dot_number, "
            + "m.name_score, m.best_match_score, m.match_method, m.match_confidence, "
            + "m.mentions, agg_nm.name_variants, agg_sch.matched_in, agg_nm.variants "
            + "FROM m "
            + (jurisdiction == null || jurisdiction.isEmpty() ? ""
                : " JOIN ref.gleif_entities ge ON ge.lei = m.lei AND upper(ge.jurisdiction) = "
                  + sqlStr(jurisdiction.toUpperCase(java.util.Locale.ROOT)) + " ")
            + "LEFT JOIN agg_sch ON agg_sch.entity_key = m.entity_key "
            + "LEFT JOIN agg_nm ON agg_nm.entity_key = m.entity_key "
            // Equi-join on a single key, NOT a disjunction. canonical_entity_id is documented
            // as "the LEI when present; the SEC CIK when EIN-matched but no LEI", so
            // COALESCE(lei, sec_cik) selects the same rows the previous OR did — but as an
            // equality DuckDB can hash-join. The OR form could not be hash-joined and
            // re-evaluated canonical_org_entity (itself a VIEW over large sources) per outer
            // row: measured at 49.2s for the full query, versus 25.1s here and 8.2s with the
            // join removed entirely.
            + "LEFT JOIN ref.canonical_org_entity c "
            + "ON c.canonical_entity_id = COALESCE(m.lei, m.sec_cik) "
            // Best-scoring entity first, then the one seen in the most places — a company
            // mentioned across many registries is the one a caller naming it usually means.
            // Name score first. Ties among identically-scored names — three distinct LEIs all
            // score 1.0 for "Alphabet, Inc." — are broken by corroboration: an entity
            // carrying a registry identifier, and seen in more places, is far more likely
            // the one a caller naming a company means than a name-only loan record.
            + "ORDER BY m.name_score DESC, "
            + "CASE WHEN m.lei IS NOT NULL OR m.sec_cik IS NOT NULL THEN 0 ELSE 1 END, "
            + "m.mentions DESC "
            + "FETCH FIRST " + cap + " ROWS ONLY";
    }

    /**
     * Mirrors the normalization {@code entity_org_bridge.source_name_normalized} was built with —
     * lowercase, punctuation dropped, common legal suffixes removed, whitespace collapsed — so a
     * caller typing "Alphabet Inc." matches rows stored as "alphabet".
     *
     * <p>A term normalized differently from the column simply fails to match, silently, so this
     * deliberately stays conservative: it strips only what the bridge's own comment names.
     */
    private static String normalizeOrgName(String term) {
        String s = term.toLowerCase(java.util.Locale.ROOT)
            .replaceAll("[^a-z0-9 ]", " ")
            .replaceAll("\\b(inc|incorporated|corp|corporation|co|company|llc|llp|lp|ltd|"
                + "limited|plc|sa|nv|ag|gmbh|holdings|holding|group|the)\\b", " ")
            .replaceAll("\\s+", " ")
            .trim();
        // An all-suffix term ("The Company") would normalize to nothing and match everything.
        return s.isEmpty() ? term.toLowerCase(java.util.Locale.ROOT).trim() : s;
    }

    /**
     * Runs the tiers in order and returns the first that matches anything.
     *
     * <p>Falling through only on an empty result is the point: an exact hit — the overwhelmingly
     * common case for a caller who typed a real company name — never pays for the scan tiers,
     * and the scan tiers still exist for the cases exact cannot reach. Merging all tiers into
     * one OR'd predicate would make every lookup cost the worst tier, which is what the 141s
     * measurement was.
     */
    /**
     * Exact first, then the scored scan — and the split is about cost, not scoring.
     *
     * <p>Both passes score identically (exact 1.0, word-boundary failure dropped, otherwise
     * jaro-winkler). The difference is the WHERE clause: pass one is an equality probe on the
     * normalized name plus the identifier hits, pass two opens it to prefix matching. The bridge
     * has 1.07M rows and no index on the name, so pass two is a full scan — measured at 176s for
     * a common term and a 180s timeout for "apple". A single merged predicate made EVERY lookup
     * pay that, including the exact hits that are the common case; running exact alone first
     * returns those in seconds and leaves the scan for terms that genuinely need it.
     */
    /**
     * Resolves a ticker to the normalized form of its registered company name, or null.
     *
     * <p>Tickers are absent from the entity bridge entirely, and the CIK they resolve to is
     * a dead end there: bridge.sec_cik is filled only for EIN-path matches, so a filer matched
     * through GLEIF has a null one. Translating the ticker into the name it trades under turns
     * the lookup into a name match, which the bridge does index — "AAPL" becomes "apple inc",
     * normalized here through the same function every other name goes through so the two sides
     * cannot drift apart.
     *
     * <p>ref.sec_company_tickers is ~10k rows, so this costs a small scan and only runs when
     * the term could plausibly be a ticker.
     */
    private static String tickerAlias(String term) {
        String t = term == null ? "" : term.trim();
        // Tickers are 1-5 characters, letters with an optional class suffix. Anything else is
        // not worth a query, and a long phrase certainly is not.
        if (!t.matches("[A-Za-z]{1,5}([.\\-][A-Za-z])?")) {
            return null;
        }
        try {
            ArrayNode rows = runSqlRows(
                "SELECT title FROM ref.sec_company_tickers WHERE upper(ticker) = "
                + sqlStr(t.toUpperCase(java.util.Locale.ROOT)), 1);
            if (rows.size() == 0) {
                return null;
            }
            String title = rows.get(0).path("title").asText("");
            if (title.isEmpty()) {
                return null;
            }
            String norm = normalizeOrgName(title);
            log.println("[askamerica-mcp] ticker " + t + " -> \"" + title + "\" -> " + norm);
            return norm;
        // fallback-guard: allow a failed ticker lookup degrades to a plain name search, which is the correct behaviour for a term that is not a ticker
        } catch (Exception e) {
            log.println("[askamerica-mcp] ticker lookup for " + t + " failed: " + e.getMessage());
            return null;
        }
    }

    private static String resolveEntity(String term, String type, int limit,
            String sourceSchema, String jurisdiction) throws Exception {
        int cap = Math.min(Math.max(1, limit), 200);
        String aliasNorm = tickerAlias(term);
        String exact = runSqlOn(
            buildResolveEntitySql(term, type, cap, sourceSchema, jurisdiction, true, aliasNorm),
            cap);
        if (exact != null && !exact.trim().equals("[]")) {
            return exact;
        }
        return runSqlOn(
            buildResolveEntitySql(term, type, cap, sourceSchema, jurisdiction, false, aliasNorm),
            cap);
    }

    // ── entity_relationships ─────────────────────────────────────────────────

    /**
     * Walks the GLEIF consolidation tree from {@code ref.current_gleif_parents}, which already
     * collapses GLEIF's repeating period/qualifier slots and restricts to ACTIVE, PUBLISHED
     * ownership edges. One row per (child, IS_DIRECTLY_CONSOLIDATED_BY |
     * IS_ULTIMATELY_CONSOLIDATED_BY), so an entity appears twice when its immediate parent and
     * the top of its group differ — which is exactly the distinction a caller asking "who really
     * owns this" needs to see, so both are returned rather than collapsed.
     *
     * <p>Siblings are derived rather than stored: entities sharing this one's DIRECT parent.
     */
    private static String buildEntityRelationshipsSql(String lei, String direction, int limit) {
        String l = lei == null ? "" : lei.trim();
        if (l.isEmpty()) {
            throw new IllegalArgumentException("lei must be non-empty");
        }
        String dir = (direction == null || direction.isEmpty()) ? "parents"
            : direction.toLowerCase(java.util.Locale.ROOT);
        int cap = Math.min(Math.max(1, limit), 500);
        String up = sqlStr(l.toUpperCase(java.util.Locale.ROOT));
        String cols = "child_lei, child_legal_name, child_jurisdiction, relationship_type, "
            + "parent_lei, parent_legal_name, parent_jurisdiction, relationship_start_date";
        switch (dir) {
            case "parents":
                return "SELECT " + cols + " FROM ref.current_gleif_parents "
                    + "WHERE upper(child_lei) = " + up + " "
                    + "ORDER BY relationship_type FETCH FIRST " + cap + " ROWS ONLY";
            case "children":
                return "SELECT " + cols + " FROM ref.current_gleif_parents "
                    + "WHERE upper(parent_lei) = " + up + " "
                    + "ORDER BY relationship_type, child_legal_name "
                    + "FETCH FIRST " + cap + " ROWS ONLY";
            case "siblings":
                // Same DIRECT parent — an ultimate-parent match would sweep in the entire
                // group, which is 'children of the ultimate parent', a different question.
                return "SELECT s." + cols.replace(", ", ", s.") + " "
                    + "FROM ref.current_gleif_parents s "
                    + "JOIN ref.current_gleif_parents me "
                    + "ON s.parent_lei = me.parent_lei "
                    + "AND s.relationship_type = me.relationship_type "
                    + "WHERE upper(me.child_lei) = " + up + " "
                    + "AND me.relationship_type = 'IS_DIRECTLY_CONSOLIDATED_BY' "
                    + "AND upper(s.child_lei) <> " + up + " "
                    + "ORDER BY s.child_legal_name FETCH FIRST " + cap + " ROWS ONLY";
            default:
                throw new IllegalArgumentException(
                    "direction must be 'parents', 'children', or 'siblings'; got " + direction);
        }
    }

    private static String entityRelationships(String lei, String direction, int limit)
            throws Exception {
        return runSqlOn(buildEntityRelationshipsSql(lei, direction, limit),
            Math.min(Math.max(1, limit), 500));
    }

    // ── per_capita ───────────────────────────────────────────────────────────

    /**
     * The population tables this server will use as a denominator, keyed by the
     * {@code population_source} argument: table, value column, and the geography-level column
     * to join on.
     *
     * <p>Fixed server-side for the same reason the CPI series is: two rates built on different
     * denominators look comparable and are not. ACS 5-year and the Census annual estimates
     * disagree for any given county-year, so a comparison that mixes them is measuring the
     * difference between the two programs as much as anything in the data.
     */
    private static final java.util.Set<String> POPULATION_SOURCES =
        new java.util.LinkedHashSet<>(java.util.Arrays.asList("acs", "pep"));

    /** Population by (year, FIPS) for one geography level, from one named source. */
    private static java.util.Map<String, Long> populationBy(String source, String geoLevel,
            java.util.Set<Integer> years) throws Exception {
        String table = "acs".equals(source) ? "acs_population" : "pep_population";
        String valueCol = "acs".equals(source) ? "total_population" : "population";
        String keyCol = "county".equals(geoLevel) ? "county_fips" : "state";

        StringBuilder inList = new StringBuilder();
        for (Integer y : new java.util.TreeSet<>(years)) {
            if (inList.length() > 0) {
                inList.append(", ");
            }
            // The year column is a hive partition and may be typed either way; comparing
            // against a string literal is what the partition column actually holds.
            inList.append('\'').append(y.intValue()).append('\'');
        }
        String sql = "SELECT t.\"year\", t." + keyCol + ", t." + valueCol
            + " FROM census." + table + " t WHERE t.geography = '"
            + ("county".equals(geoLevel) ? "county" : "state") + "' "
            + "AND CAST(t.\"year\" AS VARCHAR) IN (" + inList + ")";

        Connection c = getCatalogConnection();
        java.util.Map<String, Long> out = new java.util.HashMap<>();
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Integer year = parseYear(rs.getString(1));
                String fips = rs.getString(2);
                long pop = rs.getLong(3);
                if (year == null || fips == null || rs.wasNull()) {
                    continue;
                }
                out.put(popKey(year.intValue(), fips), Long.valueOf(pop));
            }
        }
        if (out.isEmpty()) {
            throw new IllegalStateException("census." + table + " holds no " + geoLevel
                + "-level population for years " + new java.util.TreeSet<>(years)
                + " — no rate can be computed. Run data_coverage('census', '" + table
                + "') to see which years are loaded; do not substitute a remembered "
                + "population figure.");
        }
        return out;
    }

    private static String popKey(int year, String fips) {
        return year + ":" + fips;
    }

    /**
     * A FIPS code padded to the width its geography uses, or null when the value is not a
     * code at all.
     *
     * <p>Null rather than a best guess: a state named "CA" instead of "06" must surface as
     * unmatched and be sent through resolve_geo, because silently failing to join would
     * subtract that state from the analysis without saying so.
     */
    static String normalizeFips(String raw, String geoLevel) {
        if (raw == null) {
            return null;
        }
        String t = raw.trim();
        if (t.isEmpty() || !t.chars().allMatch(Character::isDigit)) {
            return null;
        }
        int width = "county".equals(geoLevel) ? 5 : 2;
        if (t.length() > width) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = t.length(); i < width; i++) {
            sb.append('0');
        }
        return sb.append(t).toString();
    }

    /**
     * Counts to population rates, joined at the geography and year the caller's own rows sit
     * at.
     *
     * <p>A row whose geography or year has no matching population is returned with a null rate
     * and a status, never with a rate built from a neighbouring year or a national average.
     * The population used is attached to every row, so the denominator can be checked rather
     * than trusted.
     */
    private static String perCapitaTool(String sql, String valueCol, String geoCol,
            String yearCol, String geoLevel, double per, String populationSource)
            throws Exception {
        String level = (geoLevel == null || geoLevel.isEmpty()) ? "state" : geoLevel;
        if (!"state".equals(level) && !"county".equals(level)) {
            throw new IllegalArgumentException(
                "geo_level must be 'state' or 'county'; got '" + level + "'");
        }
        String source = (populationSource == null || populationSource.isEmpty())
            ? "acs" : populationSource;
        if (!POPULATION_SOURCES.contains(source)) {
            throw new IllegalArgumentException("population_source must be one of "
                + POPULATION_SOURCES + "; got '" + source + "'");
        }
        if (per <= 0) {
            throw new IllegalArgumentException("per must be positive; got " + per);
        }

        // First pass: the caller's rows, so the population query can be restricted to the
        // years actually in play rather than pulling every loaded year.
        Connection c = getCatalogConnection();
        ArrayNode rows = MAPPER.createArrayNode();
        List<String> rowFips = new ArrayList<>();
        List<Integer> rowYear = new ArrayList<>();
        List<Double> rowValue = new ArrayList<>();
        java.util.Set<Integer> years = new java.util.TreeSet<>();
        try (Statement st = c.createStatement();
             ResultSet rs = executeWithRepair(st, sql)) {
            java.sql.ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            int valueIdx = rs.findColumn(valueCol);
            int geoIdx = rs.findColumn(geoCol);
            int yearIdx = rs.findColumn(yearCol);
            while (rs.next()) {
                if (rows.size() >= MAX_LIMIT) {
                    throw new IllegalArgumentException("the SQL returned more than " + MAX_LIMIT
                        + " rows — aggregate it before rating. Every row is returned here, so "
                        + "this tool does not truncate.");
                }
                ObjectNode row = MAPPER.createObjectNode();
                for (int i = 1; i <= cols; i++) {
                    putColumn(row, md.getColumnLabel(i), md.getColumnType(i), rs, i);
                }
                double v = rs.getDouble(valueIdx);
                boolean vNull = rs.wasNull();
                String fips = normalizeFips(rs.getString(geoIdx), level);
                Integer year = parseYear(rs.getString(yearIdx));
                rows.add(row);
                rowFips.add(fips);
                rowYear.add(year);
                rowValue.add(vNull ? null : Double.valueOf(v));
                if (year != null) {
                    years.add(year);
                }
            }
        }
        if (rows.size() == 0) {
            throw new IllegalArgumentException("the SQL returned no rows to rate");
        }
        if (years.isEmpty()) {
            throw new IllegalArgumentException("not one row had a readable year in '" + yearCol
                + "' — population is matched by year, so no rate can be computed");
        }

        java.util.Map<String, Long> population = populationBy(source, level, years);
        String rateCol = per == 1.0 ? valueCol + "_per_capita"
            : valueCol + "_per_" + (long) per;
        int rated = 0;
        java.util.Set<String> unmatchedGeo = new java.util.TreeSet<>();
        java.util.Set<Integer> unmatchedYear = new java.util.TreeSet<>();
        int unreadableGeo = 0;
        for (int i = 0; i < rows.size(); i++) {
            ObjectNode row = (ObjectNode) rows.get(i);
            String fips = rowFips.get(i);
            Integer year = rowYear.get(i);
            Double value = rowValue.get(i);
            if (fips == null) {
                unreadableGeo++;
                row.putNull(rateCol);
                row.put("rate_status", "unreadable_geography");
                continue;
            }
            if (year == null) {
                row.putNull(rateCol);
                row.put("rate_status", "unreadable_year");
                continue;
            }
            if (value == null) {
                row.putNull(rateCol);
                row.put("rate_status", "null_value");
                continue;
            }
            Long pop = population.get(popKey(year.intValue(), fips));
            if (pop == null) {
                // Distinguish "this geography is never in the population table" from "this
                // year is not loaded" — they send the caller to different fixes.
                boolean geoSeen = false;
                for (Integer y : years) {
                    if (population.containsKey(popKey(y.intValue(), fips))) {
                        geoSeen = true;
                        break;
                    }
                }
                if (geoSeen) {
                    unmatchedYear.add(year);
                } else {
                    unmatchedGeo.add(fips);
                }
                row.putNull(rateCol);
                row.put("rate_status", geoSeen ? "no_population_for_year"
                    : "no_population_for_geography");
                continue;
            }
            if (pop.longValue() == 0) {
                row.putNull(rateCol);
                row.put("rate_status", "zero_population");
                continue;
            }
            row.put("population", pop.longValue());
            row.put(rateCol, value.doubleValue() / pop.longValue() * per);
            rated++;
        }

        ObjectNode out = MAPPER.createObjectNode();
        out.put("geo_level", level);
        out.put("population_source", source);
        out.put("population_table", "census."
            + ("acs".equals(source) ? "acs_population" : "pep_population"));
        out.put("per", per);
        out.put("rate_column", rateCol);
        out.put("rows_returned", rows.size());
        out.put("rows_rated", rated);
        if (unreadableGeo > 0) {
            out.put("rows_with_unreadable_geography", unreadableGeo);
            out.put("geography_hint", "Values in '" + geoCol + "' are not FIPS codes. "
                + "resolve_geo turns a name or abbreviation into the code this joins on.");
        }
        if (!unmatchedGeo.isEmpty()) {
            ArrayNode a = MAPPER.createArrayNode();
            for (String g : unmatchedGeo) {
                a.add(g);
            }
            out.set("geographies_without_population", a);
        }
        if (!unmatchedYear.isEmpty()) {
            ArrayNode a = MAPPER.createArrayNode();
            for (Integer y : unmatchedYear) {
                a.add(y.intValue());
            }
            out.set("years_without_population", a);
        }
        if (rated == 0) {
            throw new IllegalStateException("not one of the " + rows.size() + " rows could be "
                + "rated. Geographies with no population: " + unmatchedGeo + "; years with "
                + "none: " + unmatchedYear + "; rows whose geography was not a FIPS code: "
                + unreadableGeo + ". Check geo_col holds a "
                + ("county".equals(level) ? "5-digit county" : "2-digit state")
                + " FIPS code and that geo_level matches it.");
        }
        out.set("rows", rows);
        out.put("note", "Rates are value / population * " + per + ", joined on exact year and "
            + ("acs".equals(source)
                ? "ACS 5-year population, which is a five-year average labelled by its end "
                    + "year — appropriate for county-level rates, but it smooths sharp "
                    + "single-year population changes."
                : "the Census annual population estimates, the conventional rate denominator, "
                    + "which cover fewer years than ACS.")
            + " The population used is on every row. Rows with no matching population are "
            + "returned unrated with a status, never rated against a nearby year.");
        return out.toString();
    }

    // ── adjust_inflation ─────────────────────────────────────────────────────

    /**
     * The CPI series this server will deflate against, keyed by the {@code index} argument.
     *
     * <p>Fixed here rather than taken from the caller so that every deflation in a session —
     * and across sessions — runs against the same definition. A caller free to name its own
     * series can produce two figures that look comparable and are not, and the difference
     * between CPI-U and CPI-U-core over a decade is large enough to reverse a finding.
     * econ.inflation_metrics also carries a PPI series, which is not a consumer deflator and
     * is deliberately unreachable from this tool.
     */
    private static final java.util.Map<String, String> CPI_SERIES;

    static {
        java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
        m.put("cpi_u", "CUUR0000SA0");
        m.put("cpi_u_core", "CUUR0000SA0L1E");
        CPI_SERIES = java.util.Collections.unmodifiableMap(m);
    }

    /**
     * First present integer argument among {@code names}, or null if none is supplied.
     *
     * <p>Jackson's {@code path(...).asInt()} yields 0 for a missing field, which turns a
     * forgotten required argument into a plausible-looking year and pushes the failure
     * downstream where the message no longer names the real problem. Returning null lets
     * the caller reject it with a message that does.
     */
    static Integer optInt(JsonNode args, String... names) {
        for (String name : names) {
            JsonNode node = args.get(name);
            if (node != null && !node.isNull() && node.isNumber()) {
                return Integer.valueOf(node.asInt());
            }
            // A year arriving as "2024" is a caller convenience, not an error.
            if (node != null && !node.isNull() && node.isTextual()
                && node.asText().trim().matches("-?\\d+")) {
                return Integer.valueOf(Integer.parseInt(node.asText().trim()));
            }
        }
        return null;
    }

    /** One year's CPI: the annual average, and how many monthly readings it averages. */
    static final class CpiYear {
        final double index;
        final int months;

        CpiYear(double index, int months) {
            this.index = index;
            this.months = months;
        }
    }

    /**
     * The largest SVG worth inlining, in characters.
     *
     * <p>MCP clients cap a single content block; the comparative eval measured one truncating at
     * roughly 30 KB. This sits far enough below that to survive a smaller cap, and the cost of
     * being wrong in this direction is one extra fetch, against a silently corrupted document in
     * the other.
     */
    private static final int INLINE_SVG_LIMIT = 24576;

    /**
     * Refuses to inline an SVG large enough that the client will truncate it, or null if it fits.
     *
     * <p>The 2026-08-19e eval watched an agent ask for the SVG four times. Each response carried
     * 32,111 characters, each was cut off in transit, and the agent spent three extra round-trips
     * slicing the fragments together — then reported the dashboard as undeliverable. Nothing was
     * broken except that we handed over a document we could have known would not arrive.
     *
     * <p>Truncation is the worst available outcome because it is silent: the caller receives
     * something that looks like an SVG, is not well-formed, and fails later at a place that has
     * nothing to do with the cause. Returning the link instead is a smaller answer that is
     * actually true. The board is already viewable as the PNG in the same response, so nothing
     * is lost but the ability to hand-edit it in place — and the link fetches the same source.
     */
    static String oversizeSvgNotice(String svg, String url) {
        if (svg == null || svg.length() <= INLINE_SVG_LIMIT) {
            return null;
        }
        return "YOUR BOARD IS COMPLETE AND UNCHANGED — only this response's inline copy of "
            + "its source is withheld. The SVG is " + svg.length() + " characters, past the "
            + "roughly 30,000 an MCP client delivers in one block, so inlining it would hand "
            + "you a truncated, malformed document rather than an error. "
            + (url == null
                ? "The PNG above shows the board; the saved file holds the full source."
                : "Fetch the full source from " + url + " when you need to edit it. The PNG "
                + "above already shows the board.")
            + " Do NOT drop panels, simplify the board, or re-run to make the source fit: the "
            + "size of this response has nothing to do with the quality of the dashboard, and "
            + "a smaller board is a worse answer, not a fix.";
    }

    /** The BLS series id for an {@code index} argument, or a failure naming the choices. */
    static String cpiSeriesFor(String index) {
        String key = (index == null || index.isEmpty()) ? "cpi_u" : index;
        String series = CPI_SERIES.get(key);
        if (series == null) {
            throw new IllegalArgumentException("index must be one of " + CPI_SERIES.keySet()
                + "; got '" + key + "'");
        }
        return series;
    }

    /**
     * The part of an adjust_inflation answer that describes the deflator itself: which series,
     * which base year, what its index is, and which loaded years average fewer than twelve
     * months. Built before any row is touched so a caller can see what the adjustment was made
     * against even when the adjustment then fails.
     */
    static ObjectNode inflationHeader(String indexKey, String series, int baseYear,
            java.util.Map<Integer, CpiYear> cpi) {
        CpiYear base = cpi.get(Integer.valueOf(baseYear));
        if (base == null) {
            throw new IllegalArgumentException("no CPI loaded for base_year " + baseYear
                + " — loaded CPI years are " + cpi.keySet() + ". Pick a base year inside "
                + "that range, or run data_coverage('econ', 'inflation_metrics') to see what "
                + "is loaded.");
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("index", indexKey);
        out.put("series", series);
        out.put("base_year", baseYear);
        out.put("base_year_cpi", base.index);
        out.put("base_year_months_averaged", base.months);
        ArrayNode available = MAPPER.createArrayNode();
        ArrayNode partial = MAPPER.createArrayNode();
        for (java.util.Map.Entry<Integer, CpiYear> e : cpi.entrySet()) {
            available.add(e.getKey().intValue());
            if (e.getValue().months < 12) {
                ObjectNode p = MAPPER.createObjectNode();
                p.put("year", e.getKey().intValue());
                p.put("months_averaged", e.getValue().months);
                partial.add(p);
            }
        }
        out.set("cpi_years_loaded", available);
        if (partial.size() > 0) {
            out.set("partial_cpi_years", partial);
        }
        if (base.months < 12) {
            out.put("warning", "base_year " + baseYear + " averages only " + base.months
                + " monthly CPI readings, not 12 — it is a partial-year mean, not an annual "
                + "average, and figures expressed in it are not comparable to a full-year "
                + "base. Either fetch the missing months from the publisher and complete the "
                + "year (see the staleness block), or fall back to the most recent year with "
                + "12 months. Do not report a partial-year mean as \"today's dollars\" "
                + "without saying which months it covers.");
        }
        ObjectNode stale = cpiStaleness(series, cpi);
        if (stale != null) {
            out.set("staleness", stale);
        }
        return out;
    }

    /**
     * Says how far behind the publisher this loaded CPI is, and tells the caller to close the gap.
     *
     * <p>Added after the comparative eval measured the same failure three runs running. The
     * warehouse's CPI stopped at May 2026 while BLS had published July; the ungrounded arms
     * fetched July in one call, and the grounded arms did not — they deflated to a five-month
     * partial mean and disclosed it in a footnote. Both had a standing instruction to splice a
     * stale series from the publisher, and both ignored it, because nothing in the response said
     * the series was behind <em>the world</em>. The old warning only ever pointed backwards —
     * "prefer the most recent year with 12 months" — which is advice to retreat further into the
     * warehouse.
     *
     * <p>So the response now states the gap in the same terms a person would: what is loaded,
     * what should exist by now, and where to get the difference. An agent that has been handed
     * the publisher's URL and a month count has no remaining excuse for reporting stale dollars
     * as current ones.
     *
     * <p>Returns null when the series is current, so a healthy answer carries no noise.
     */
    static ObjectNode cpiStaleness(String series, java.util.Map<Integer, CpiYear> cpi) {
        int latestYear = Integer.MIN_VALUE;
        for (Integer y : cpi.keySet()) {
            latestYear = Math.max(latestYear, y.intValue());
        }
        CpiYear latest = cpi.get(Integer.valueOf(latestYear));
        if (latest == null) {
            return null;
        }
        java.time.YearMonth loaded = java.time.YearMonth.of(latestYear, Math.max(1, latest.months));
        // BLS publishes a month's CPI in the middle of the following month, so the newest
        // reading that should exist is the previous calendar month once we are past mid-month,
        // and the one before that otherwise. Erring a month late keeps this from crying stale
        // on the days around a release.
        java.time.LocalDate today = java.time.LocalDate.now();
        java.time.YearMonth expected = java.time.YearMonth.from(today)
            .minusMonths(today.getDayOfMonth() >= 15 ? 1 : 2);
        long behind = java.time.temporal.ChronoUnit.MONTHS.between(loaded, expected);
        if (behind < 1) {
            return null;
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("series", series);
        out.put("latest_loaded", loaded.toString());
        out.put("expected_latest", expected.toString());
        out.put("months_behind", behind);
        out.put("publisher", "https://www.bls.gov/cpi/");
        out.put("publisher_api", "https://api.bls.gov/publicAPI/v2/timeseries/data/" + series);
        out.put("action", "REQUIRED before you describe any result as \"today's dollars\": "
            + "fetch " + series + " for " + loaded.plusMonths(1) + " through " + expected
            + " from the publisher above and splice it onto this series. The overlapping months "
            + "must agree before you splice; if they do not, say so and stop rather than "
            + "stitching two different vintages. If you cannot fetch it, state in your answer "
            + "that the deflator is " + behind + " month(s) stale and name " + loaded
            + " as the actual base — do not present it as current.");
        return out;
    }

    /** Converts one amount and records both indices, so the arithmetic is checkable. */
    static void scalarAdjustment(ObjectNode out, java.util.Map<Integer, CpiYear> cpi,
            double amount, int fromYear, int baseYear) {
        CpiYear from = cpi.get(Integer.valueOf(fromYear));
        if (from == null) {
            throw new IllegalArgumentException("no CPI loaded for from_year " + fromYear
                + " — loaded CPI years are " + cpi.keySet());
        }
        double deflator = cpi.get(Integer.valueOf(baseYear)).index / from.index;
        out.put("from_year", fromYear);
        out.put("from_year_cpi", from.index);
        out.put("nominal_amount", amount);
        out.put("deflator", deflator);
        out.put("real_amount", amount * deflator);
    }

    /**
     * Annual-average CPI by year from econ.inflation_metrics, which holds the BLS monthly
     * index levels.
     *
     * <p>The month count travels with each year because it decides whether the average is an
     * annual average at all. The current year is always partial, and a schema whose ingest
     * window is mid-backfill can leave any year short — deflating against a three-month mean
     * and calling it 2026 dollars is wrong in a way no downstream consumer can detect, so the
     * count is reported and short years are named.
     */
    private static java.util.Map<Integer, CpiYear> cpiByYear(String series) throws Exception {
        Connection c = getCatalogConnection();
        java.util.Map<Integer, CpiYear> out = new java.util.TreeMap<>();
        // "year" and "value" are reserved identifiers in this lex, quoted here rather than
        // left to normalizeCallerSql — that pass only rewrites dot-adjacent tokens.
        String sql = "SELECT t.\"year\", AVG(t.\"value\"), COUNT(t.\"value\") "
            + "FROM econ.inflation_metrics t WHERE t.series = '" + series + "' "
            + "GROUP BY t.\"year\" ORDER BY t.\"year\"";
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                // Read the year as text, not getInt: year is a hive partition column here and
                // surfaces as VARCHAR, where the integer accessor throws outright.
                String rawYear = rs.getString(1);
                double avg = rs.getDouble(2);
                if (rs.wasNull() || rawYear == null) {
                    continue;
                }
                Integer y;
                try {
                    y = Integer.valueOf(rawYear.trim());
                } catch (NumberFormatException e) {
                    throw new IllegalStateException("econ.inflation_metrics has a year column "
                        + "holding '" + rawYear + "', which is not a year — the CPI table "
                        + "cannot be read as an annual series", e);
                }
                out.put(y, new CpiYear(avg, rs.getInt(3)));
            }
        }
        if (out.isEmpty()) {
            throw new IllegalStateException("econ.inflation_metrics holds no rows for series "
                + series + " — the CPI table is not loaded in this deployment, so no "
                + "inflation adjustment can be made. Do not substitute a remembered CPI "
                + "figure; report the gap.");
        }
        return out;
    }

    /**
     * Nominal to real, against one server-side CPI vintage.
     *
     * <p>Two modes, because both questions are asked: a whole result set gets each row
     * deflated in place ({@code sql} + {@code value_col} + {@code year_col}), and a single
     * figure gets converted on its own ({@code amount} + {@code from_year}).
     *
     * <p>A year with no loaded CPI is never deflated against a neighbouring year's index or
     * an assumed rate — the row comes back with a null real value and a status naming the
     * problem, and the years involved are listed at the top level.
     */
    private static String adjustInflationTool(int baseYear, String sql, String valueCol,
            String yearCol, Double amount, Integer fromYear, String index) throws Exception {
        String key = (index == null || index.isEmpty()) ? "cpi_u" : index;
        String series = cpiSeriesFor(key);
        java.util.Map<Integer, CpiYear> cpi = cpiByYear(series);
        ObjectNode out = inflationHeader(key, series, baseYear, cpi);
        CpiYear base = cpi.get(Integer.valueOf(baseYear));

        if (sql != null && !sql.trim().isEmpty()) {
            if (valueCol == null || yearCol == null) {
                throw new IllegalArgumentException(
                    "value_col and year_col are both required when sql is given");
            }
            adjustRows(out, sql, valueCol, yearCol, cpi, base, baseYear);
        } else {
            if (amount == null || fromYear == null) {
                throw new IllegalArgumentException("give either sql + value_col + year_col to "
                    + "deflate a result set, or amount + from_year to convert one figure");
            }
            scalarAdjustment(out, cpi, amount.doubleValue(), fromYear.intValue(), baseYear);
        }

        out.put("note", "Real amounts are nominal * (base-year CPI / source-year CPI), using "
            + "annual averages of BLS " + series + " from econ.inflation_metrics. CPI-U "
            + "measures urban consumer prices; it is the general-purpose deflator, not the "
            + "right one for construction costs, medical inputs, or government purchases, "
            + "each of which has its own index. Years with no loaded CPI are reported, never "
            + "interpolated.");
        return out.toString();
    }

    /** Deflates each row of {@code sql} in place, carrying every original column through. */
    private static void adjustRows(ObjectNode out, String sql, String valueCol, String yearCol,
            java.util.Map<Integer, CpiYear> cpi, CpiYear base, int baseYear) throws Exception {
        Connection c = getCatalogConnection();
        ArrayNode rows = MAPPER.createArrayNode();
        java.util.Set<Integer> missing = new java.util.TreeSet<>();
        int converted = 0;
        String realCol = valueCol + "_real_" + baseYear;
        try (Statement st = c.createStatement();
             ResultSet rs = executeWithRepair(st, sql)) {
            java.sql.ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            int valueIdx = rs.findColumn(valueCol);
            int yearIdx = rs.findColumn(yearCol);
            while (rs.next()) {
                if (rows.size() >= MAX_LIMIT) {
                    throw new IllegalArgumentException("the SQL returned more than " + MAX_LIMIT
                        + " rows — aggregate it before deflating, or add a WHERE clause. "
                        + "Every row is returned here, so this tool does not truncate.");
                }
                ObjectNode row = MAPPER.createObjectNode();
                for (int i = 1; i <= cols; i++) {
                    putColumn(row, md.getColumnLabel(i), md.getColumnType(i), rs, i);
                }
                double nominal = rs.getDouble(valueIdx);
                boolean nominalNull = rs.wasNull();
                // The year column is usually the table's hive partition column, which
                // surfaces as VARCHAR — getInt throws on it, so parse the text instead.
                Integer y = parseYear(rs.getString(yearIdx));
                boolean yearNull = y == null;
                CpiYear from = yearNull ? null : cpi.get(y);
                if (nominalNull || yearNull) {
                    row.putNull(realCol);
                    row.put("adjustment_status", nominalNull ? "null_value" : "unreadable_year");
                } else if (from == null) {
                    missing.add(y);
                    row.putNull(realCol);
                    row.put("adjustment_status", "no_cpi_for_year");
                } else {
                    row.put(realCol, nominal * (base.index / from.index));
                    row.put("cpi_deflator", base.index / from.index);
                    converted++;
                }
                rows.add(row);
            }
        }
        out.put("real_value_column", realCol);
        out.put("rows_returned", rows.size());
        out.put("rows_adjusted", converted);
        if (!missing.isEmpty()) {
            ArrayNode m = MAPPER.createArrayNode();
            for (Integer y : missing) {
                m.add(y.intValue());
            }
            out.set("years_without_loaded_cpi", m);
        }
        if (converted == 0 && rows.size() > 0) {
            throw new IllegalStateException("not one of the " + rows.size() + " rows could be "
                + "deflated: years present in the data " + missing + " have no loaded CPI. "
                + "Check year_col names the year the amount is denominated in, and run "
                + "data_coverage('econ', 'inflation_metrics').");
        }
        out.set("rows", rows);
    }

    /**
     * A 4-digit year from a column that may be typed as either an integer or a hive partition
     * string, or null when it holds neither. Null rather than a guess: a row whose year cannot
     * be read is reported unadjusted, never deflated against an assumed year.
     */
    static Integer parseYear(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            int y = Integer.parseInt(raw.trim());
            return (y >= 1800 && y <= 2200) ? Integer.valueOf(y) : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Copies one result-set column into {@code row}, typed as JSON rather than stringified. */
    private static void putColumn(ObjectNode row, String name, int t, ResultSet rs, int i)
            throws java.sql.SQLException {
        if (t == java.sql.Types.INTEGER || t == java.sql.Types.BIGINT
                || t == java.sql.Types.SMALLINT || t == java.sql.Types.TINYINT) {
            long v = rs.getLong(i);
            if (rs.wasNull()) {
                row.putNull(name);
            } else {
                row.put(name, v);
            }
        } else if (t == java.sql.Types.DOUBLE || t == java.sql.Types.FLOAT
                || t == java.sql.Types.REAL || t == java.sql.Types.DECIMAL
                || t == java.sql.Types.NUMERIC) {
            double v = rs.getDouble(i);
            if (rs.wasNull()) {
                row.putNull(name);
            } else {
                row.put(name, v);
            }
        } else if (t == java.sql.Types.BOOLEAN || t == java.sql.Types.BIT) {
            boolean v = rs.getBoolean(i);
            if (rs.wasNull()) {
                row.putNull(name);
            } else {
                row.put(name, v);
            }
        } else {
            Object v = rs.getObject(i);
            if (rs.wasNull() || v == null) {
                row.putNull(name);
            } else {
                row.put(name, v.toString());
            }
        }
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
            // Project the key expression once in an inner subquery and GROUP BY the resulting
            // plain column, rather than repeating the (sometimes CASE/SUBSTRING-laden) key
            // expression as its own literal text in both SELECT and GROUP BY: Calcite's GROUP BY
            // expander fails to resolve identifiers inside that second, textually-duplicated copy
            // for some expression shapes (quarter_col's CASE, and the state grain's join-derived
            // COALESCE) — "Column ... not found" — even though the identical SELECT-list copy
            // resolves fine. A GROUP BY on a real projected column never hits that path.
            ctes.add("s" + i + " AS (SELECT k, " + agg + "(v) AS " + name + " FROM (SELECT "
                + key + " AS k, " + value + " AS v FROM " + tableFrom + whereClause + ") p" + i
                + " GROUP BY k)");
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

    /** Rows plus the SQL that produced them, so diagnostics read the query that actually ran
     *  rather than the series spec that was composed into it. */
    private static final class RowsWithSql {
        final ArrayNode rows;
        final String sql;

        RowsWithSql(ArrayNode rows, String sql) {
            this.rows = rows;
            this.sql = sql;
        }
    }

    private static RowsWithSql fetchAlignedSeries(JsonNode series, String on, String stat,
            int limit) throws Exception {
        String sql = buildAlignedSql(series, on, stat);
        // Validates that every series names a schema-qualified table; the result is not used
        // to scope the connection, which is always the all-schemas one.
        schemasOf(series);
        // stat returns a single scalar row; a frame gets the caller's limit.
        return new RowsWithSql(runSqlRows(sql, stat != null ? 5 : limit), sql);
    }

    /** Reads a JSON array-of-strings node into a {@link List}; treats a missing/null node as
     *  empty rather than throwing, since several stats tools take optional string arrays
     *  (e.g. controls). */
    private static List<String> textArray(JsonNode node) {
        List<String> out = new ArrayList<>();
        if (node != null && node.isArray()) {
            for (JsonNode n : node) {
                out.add(n.asText());
            }
        }
        return out;
    }

    // ─── Regression / hypothesis-test tools (StatsEngine) ────────────────────────

    static StatsOutput olsRegressionTool(String sql, String outcome, List<String> predictors)
            throws Exception {
        if (sql == null || sql.trim().isEmpty() || outcome == null || outcome.trim().isEmpty()) {
            throw new IllegalArgumentException("sql and outcome are required for ols_regression");
        }
        String[] cols = new String[1 + predictors.size()];
        cols[0] = outcome;
        for (int i = 0; i < predictors.size(); i++) {
            cols[1 + i] = predictors.get(i);
        }
        Connection c = getCatalogConnection();
        StatsEngine.Extraction ex = StatsEngine.extractColumns(c, sql, cols);
        double[] y = ex.column(0);
        double[][] x = ex.columnsFor(predictors.toArray(new String[0]));
        StatsEngine.OlsResult result = StatsEngine.ols(y, x, predictors.toArray(new String[0]));
        ObjectNode out = result.toJson(MAPPER);
        return statsResult(out, sql, predictors, ex);
    }

    private static StatsOutput iv2slsTool(String sql, String outcome, String endogenous,
            List<String> instruments, List<String> controls) throws Exception {
        List<String> cols = new ArrayList<>();
        cols.add(outcome);
        cols.add(endogenous);
        cols.addAll(instruments);
        cols.addAll(controls);
        Connection c = getCatalogConnection();
        StatsEngine.Extraction ex = StatsEngine.extractColumns(c, sql, cols.toArray(new String[0]));
        double[] y = ex.column(outcome);
        double[] endog = ex.column(endogenous);
        double[][] instr = ex.columnsFor(instruments.toArray(new String[0]));
        double[][] ctrl = ex.columnsFor(controls.toArray(new String[0]));
        StatsEngine.Iv2slsResult result = StatsEngine.iv2sls(y, endog, instr, ctrl,
            instruments.toArray(new String[0]), controls.toArray(new String[0]));
        ObjectNode out = result.toJson(MAPPER);
        return statsResult(out, sql, controls, ex);
    }

    private static StatsOutput diffInDiffTool(String sql, String outcome, String treatment,
            String post, List<String> controls) throws Exception {
        List<String> cols = new ArrayList<>();
        cols.add(outcome);
        cols.add(treatment);
        cols.add(post);
        cols.addAll(controls);
        Connection c = getCatalogConnection();
        StatsEngine.Extraction ex = StatsEngine.extractColumns(c, sql, cols.toArray(new String[0]));
        double[] y = ex.column(outcome);
        double[] treat = ex.column(treatment);
        double[] postCol = ex.column(post);
        double[][] ctrl = ex.columnsFor(controls.toArray(new String[0]));
        StatsEngine.DiffInDiffResult result = StatsEngine.diffInDiff(y, treat, postCol, ctrl,
            controls.toArray(new String[0]));
        ObjectNode out = result.toJson(MAPPER);
        return statsResult(out, sql, controls, ex);
    }

    private static StatsOutput hypothesisTestTool(String sql, String test, String valueCol,
            String groupCol, Double oneSampleMu, String rowCol, String colCol) throws Exception {
        Connection c = getCatalogConnection();
        if ("chi_square".equals(test)) {
            if (rowCol == null || colCol == null) {
                throw new IllegalArgumentException(
                    "chi_square requires row_col and col_col");
            }
            StatsEngine.ContingencyTable table =
                StatsEngine.extractContingencyTable(c, sql, rowCol, colCol);
            ObjectNode out = StatsEngine.hypothesisTest(MAPPER, test, java.util.Collections.emptyMap(),
                null, table.counts);
            ArrayNode rowLabels = MAPPER.createArrayNode();
            table.rowLabels.forEach(rowLabels::add);
            ArrayNode colLabels = MAPPER.createArrayNode();
            table.colLabels.forEach(colLabels::add);
            out.set("row_labels", rowLabels);
            out.set("col_labels", colLabels);
            int cells = 0;
            for (long[] rowCounts : table.counts) {
                for (long v : rowCounts) {
                    cells += (int) v;
                }
            }
            return new StatsOutput(out.toString(), diagnoseStats(sql,
                java.util.Collections.<String>emptyList(), null, cells, 0, 0));
        }
        if (valueCol == null) {
            throw new IllegalArgumentException("value_col is required for test '" + test + "'");
        }
        Map<String, double[]> groups;
        if (groupCol != null) {
            groups = StatsEngine.extractGroupedColumn(c, sql, groupCol, valueCol);
        } else {
            if (oneSampleMu == null) {
                throw new IllegalArgumentException(
                    "either group_col or one_sample_mu is required for test '" + test + "'");
            }
            StatsEngine.Extraction ex = StatsEngine.extractColumns(c, sql, new String[]{valueCol});
            groups = new LinkedHashMap<>();
            groups.put(valueCol, ex.column(0));
        }
        ObjectNode out = StatsEngine.hypothesisTest(MAPPER, test, groups, oneSampleMu, null);
        int observations = 0;
        for (double[] group : groups.values()) {
            observations += group.length;
        }
        return new StatsOutput(out.toString(), diagnoseStats(sql,
            java.util.Collections.<String>emptyList(), null, observations, 0, 0));
    }

    private static StatsOutput panelFixedEffectsTool(String sql, String outcome,
            List<String> predictors, String entityCol, String timeCol, String clusterCol)
            throws Exception {
        List<String> numCols = new ArrayList<>();
        numCols.add(outcome);
        numCols.addAll(predictors);
        // Requesting the cluster column only when asked keeps the no-cluster path — and the
        // rows it drops for nulls — exactly as it was before clustering existed.
        String[] labelCols = clusterCol == null
            ? new String[]{entityCol, timeCol}
            : new String[]{entityCol, timeCol, clusterCol};
        Connection c = getCatalogConnection();
        StatsEngine.LabeledExtraction ex = StatsEngine.extractColumnsWithLabels(c, sql,
            numCols.toArray(new String[0]), labelCols);
        double[] y = ex.column(outcome);
        double[][] x = ex.columnsFor(predictors.toArray(new String[0]));
        String[] entityIds = ex.labelColumn(entityCol);
        String[] timeIds = ex.labelColumn(timeCol);
        String[] clusterIds = clusterCol == null ? null : ex.labelColumn(clusterCol);
        StatsEngine.PanelFixedEffectsResult result = StatsEngine.panelFixedEffects(y, x,
            predictors.toArray(new String[0]), entityIds, timeIds, clusterIds);
        ObjectNode out = result.toJson(MAPPER);
        if (clusterCol != null) {
            result.describeStandardErrors(out, clusterCol);
        }
        return statsResult(out, sql, predictors, ex);
    }

    private static StatsOutput robustRegressionTool(String sql, String outcome,
            List<String> predictors, String clusterCol) throws Exception {
        List<String> numCols = new ArrayList<>();
        numCols.add(outcome);
        numCols.addAll(predictors);
        Connection c = getCatalogConnection();
        if (clusterCol != null) {
            StatsEngine.LabeledExtraction ex = StatsEngine.extractColumnsWithLabels(c, sql,
                numCols.toArray(new String[0]), new String[]{clusterCol});
            double[] y = ex.column(outcome);
            double[][] x = ex.columnsFor(predictors.toArray(new String[0]));
            String[] clusterIds = ex.labelColumn(clusterCol);
            StatsEngine.RobustRegressionResult result = StatsEngine.robustRegression(y, x,
                predictors.toArray(new String[0]), clusterIds);
            ObjectNode out = result.toJson(MAPPER);
            return statsResult(out, sql, predictors, ex);
        }
        StatsEngine.Extraction ex = StatsEngine.extractColumns(c, sql, numCols.toArray(new String[0]));
        double[] y = ex.column(0);
        double[][] x = ex.columnsFor(predictors.toArray(new String[0]));
        StatsEngine.RobustRegressionResult result = StatsEngine.robustRegression(y, x,
            predictors.toArray(new String[0]), null);
        ObjectNode out = result.toJson(MAPPER);
        return statsResult(out, sql, predictors, ex);
    }

    /**
     * Event study over a unit-period panel.
     *
     * <p>Extraction is written out here rather than delegated to
     * {@link StatsEngine#extractColumnsWithLabels}, because that drops any row with a null in
     * any requested column — and a null treatment time is not missing data here, it is the
     * definition of a never-treated unit. Delegating would silently discard exactly the
     * comparison group the design rests on.
     */
    private static StatsOutput eventStudyTool(String sql, String outcome, String unitCol,
            String timeCol, String treatTimeCol, int maxLead, int maxLag, int reference,
            String clusterCol) throws Exception {
        // "none" is the explicit opt-out; anything else names a column that must exist, so a
        // misspelled cluster_col fails loudly instead of quietly reverting to unclustered.
        boolean clustered = clusterCol != null && !"none".equalsIgnoreCase(clusterCol);
        Connection c = getCatalogConnection();
        List<Double> ys = new ArrayList<>();
        List<String> units = new ArrayList<>();
        List<String> times = new ArrayList<>();
        List<Integer> relative = new ArrayList<>();
        List<String> clusters = new ArrayList<>();
        int totalRows = 0;
        int droppedForNull = 0;
        int neverTreatedRows = 0;
        try (Statement st = c.createStatement();
             ResultSet rs = executeWithRepair(st, sql)) {
            int yIdx = rs.findColumn(outcome);
            int unitIdx = rs.findColumn(unitCol);
            int timeIdx = rs.findColumn(timeCol);
            int treatIdx = rs.findColumn(treatTimeCol);
            int clusterIdx = clustered ? rs.findColumn(clusterCol) : -1;
            while (rs.next()) {
                if (++totalRows > StatsEngine.STATS_MAX_ROWS) {
                    throw new IllegalArgumentException("the SQL returned more than "
                        + StatsEngine.STATS_MAX_ROWS + " rows — narrow it before running an "
                        + "event study over it");
                }
                double y = rs.getDouble(yIdx);
                boolean yNull = rs.wasNull();
                String unit = rs.getString(unitIdx);
                Integer time = readInt(rs, timeIdx);
                // A null here means never treated, which is a usable row. Every other null
                // leaves the observation unplaceable in the panel, so it is dropped and
                // counted.
                Integer treat = readInt(rs, treatIdx);
                if (yNull || unit == null || time == null) {
                    droppedForNull++;
                    continue;
                }
                if (clustered) {
                    String cluster = rs.getString(clusterIdx);
                    if (cluster == null) {
                        // A null cluster key would silently pool every such row into one
                        // pseudo-cluster and shrink the standard errors.
                        droppedForNull++;
                        continue;
                    }
                    clusters.add(cluster);
                }
                ys.add(Double.valueOf(y));
                units.add(unit);
                times.add(String.valueOf(time.intValue()));
                if (treat == null) {
                    neverTreatedRows++;
                    relative.add(null);
                } else {
                    relative.add(Integer.valueOf(time.intValue() - treat.intValue()));
                }
            }
        }
        if (ys.isEmpty()) {
            throw new IllegalArgumentException("the SQL returned no usable rows — every row "
                + "was missing the outcome, the unit, or the period");
        }
        double[] y = new double[ys.size()];
        for (int i = 0; i < ys.size(); i++) {
            y[i] = ys.get(i).doubleValue();
        }
        StatsEngine.EventStudyResult result = StatsEngine.eventStudy(y,
            units.toArray(new String[0]), times.toArray(new String[0]),
            relative.toArray(new Integer[0]), maxLead, maxLag, reference,
            clustered ? clusters.toArray(new String[0]) : null);
        result.clusterColumn = clustered ? clusterCol : null;
        ObjectNode out = result.toJson(MAPPER);
        out.put("rows_returned_by_sql", totalRows);
        out.put("rows_dropped_for_null", droppedForNull);
        out.put("never_treated_rows", neverTreatedRows);
        return new StatsOutput(out.toString(), diagnoseStats(sql,
            java.util.Collections.<String>emptyList(), null, ys.size(), totalRows,
            droppedForNull));
    }

    /**
     * An integer from a column that may be typed as an integer or as a partition string, or
     * null when it holds neither. Panel period columns are as often VARCHAR as INT here.
     */
    private static Integer readInt(ResultSet rs, int idx) throws java.sql.SQLException {
        String raw = rs.getString(idx);
        if (raw == null) {
            return null;
        }
        try {
            return Integer.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Leave-one-group-out refits of an OLS the caller has already run.
     *
     * <p>Deliberately re-extracts rather than reusing anything from the earlier call: the
     * caller passes the same SQL, and this runs it again, so the sensitivity result cannot
     * silently describe a different sample than the one it claims to test.
     */
    private static StatsOutput sensitivityAnalysisTool(String sql, String outcome,
            List<String> predictors, String groupCol, String term) throws Exception {
        if (predictors.isEmpty()) {
            throw new IllegalArgumentException("predictors must name at least one column");
        }
        List<String> numCols = new ArrayList<>();
        numCols.add(outcome);
        numCols.addAll(predictors);
        Connection c = getCatalogConnection();
        StatsEngine.LabeledExtraction ex = StatsEngine.extractColumnsWithLabels(c, sql,
            numCols.toArray(new String[0]), new String[]{groupCol});
        double[] y = ex.column(outcome);
        double[][] x = ex.columnsFor(predictors.toArray(new String[0]));
        String[] groups = ex.labelColumn(groupCol);
        String tracked = (term == null || term.isEmpty()) ? predictors.get(0) : term;
        StatsEngine.SensitivityResult result =
            StatsEngine.leaveOneGroupOut(y, x, predictors.toArray(new String[0]), groups,
                tracked);
        ObjectNode out = result.toJson(MAPPER);
        out.put("group_column", groupCol);
        return statsResult(out, sql, predictors, ex);
    }

    private static StatsOutput flexibleRegressionTool(String sql, String outcome,
            List<String> predictors, String method) throws Exception {
        String resolvedMethod = method != null ? method : "random_forest";
        String[] cols = new String[1 + predictors.size()];
        cols[0] = outcome;
        for (int i = 0; i < predictors.size(); i++) {
            cols[1 + i] = predictors.get(i);
        }
        Connection c = getCatalogConnection();
        StatsEngine.Extraction ex = StatsEngine.extractColumns(c, sql, cols);
        double[] y = ex.column(0);
        double[][] x = ex.columnsFor(predictors.toArray(new String[0]));
        StatsMlEngine.FlexibleRegressionResult result = StatsMlEngine.flexibleRegression(y, x,
            outcome, predictors.toArray(new String[0]), resolvedMethod);
        ObjectNode out = result.toJson(MAPPER);
        return statsResult(out, sql, predictors, ex);
    }

    private static StatsOutput featureImportanceTool(String sql, String outcome,
            List<String> predictors, String method) throws Exception {
        String resolvedMethod = method != null ? method : "random_forest";
        String[] predictorNames = predictors.toArray(new String[0]);
        String[] cols = new String[1 + predictorNames.length];
        cols[0] = outcome;
        System.arraycopy(predictorNames, 0, cols, 1, predictorNames.length);
        Connection c = getCatalogConnection();
        StatsEngine.Extraction ex = StatsEngine.extractColumns(c, sql, cols);
        double[] y = ex.column(0);
        double[][] x = ex.columnsFor(predictorNames);
        StatsMlEngine.FlexibleRegressionResult result =
            StatsMlEngine.flexibleRegression(y, x, outcome, predictorNames, resolvedMethod);

        Integer[] order = new Integer[predictorNames.length];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        java.util.Arrays.sort(order,
            (a, b) -> Double.compare(result.importance[b], result.importance[a]));

        ObjectNode out = MAPPER.createObjectNode();
        out.put("method", resolvedMethod);
        out.put("n", result.n);
        ArrayNode ranked = MAPPER.createArrayNode();
        for (int idx : order) {
            ObjectNode row = MAPPER.createObjectNode();
            row.put("predictor", predictorNames[idx]);
            row.put("importance", result.importance[idx]);
            ranked.add(row);
        }
        out.set("ranked_importance", ranked);
        out.put("note", "Ranked by impurity decrease summed across trees (" + resolvedMethod
            + ") — reflects how much the model relied on each predictor to split, not a "
            + "causal or necessarily monotonic effect size.");
        return statsResult(out, sql, predictors, ex);
    }

    private static StatsOutput doubleMlAteTool(String sql, String outcome, String treatment,
            List<String> controls, Integer folds, String method) throws Exception {
        String resolvedMethod = method != null ? method : "random_forest";
        int resolvedFolds = folds != null ? folds : 5;
        List<String> cols = new ArrayList<>();
        cols.add(outcome);
        cols.add(treatment);
        cols.addAll(controls);
        Connection c = getCatalogConnection();
        StatsEngine.Extraction ex = StatsEngine.extractColumns(c, sql, cols.toArray(new String[0]));
        double[] y = ex.column(outcome);
        double[] treat = ex.column(treatment);
        double[][] ctrl = ex.columnsFor(controls.toArray(new String[0]));
        StatsMlEngine.DoubleMlResult result = StatsMlEngine.doubleMlAte(y, treat, ctrl,
            controls.toArray(new String[0]), resolvedFolds, resolvedMethod);
        ObjectNode out = result.toJson(MAPPER);
        return statsResult(out, sql, controls, ex);
    }

    /**
     * Diagnostics for a row-returning analytical tool.
     *
     * <p>A failure inside a check is reported as its own envelope rather than dropped. The
     * silent alternative is worse than no diagnostics at all: an empty warnings array is
     * indistinguishable from a clean result, so a broken check would read as a passing one.
     */
    private static ObjectNode diagnose(String sql, ArrayNode rows, int rowLimit) {
        try {
            return QuestionDiagnostics.forQuery(getCatalogConnection(), sql, rows, rowLimit);
        } catch (Exception e) {
            String reason = compactErrorMessage(e);
            log.println("[askamerica-mcp] diagnostics failed: " + reason);
            return QuestionDiagnostics.incomplete(reason);
        }
    }

    /**
     * Tells the caller its SQL was rewritten, on the same response that carries the rows.
     *
     * <p>Repairing silently would buy one query and cost the next: the caller writes the same
     * unquoted column again because nothing ever said otherwise. It is also simply not our
     * statement to change without saying so — the notice names the words and prints the SQL
     * that actually ran, so the result can be checked against what was executed.
     *
     * <p>Attached to whichever envelope the tool produced. A tool with no diagnostics still has
     * to report the rewrite, so the caller of {@code adjust_inflation} learns the same rule as
     * the caller of {@code query}; see the standalone block in the response assembly.
     */
    private static void addRepairNotice(ObjectNode diagnostics) {
        String notice = LAST_REPAIR_NOTICE.get();
        if (notice == null || diagnostics == null) {
            return;
        }
        // The envelope nests as {"diagnostics":{"warnings":[...]}} and its warnings are objects,
        // not strings — a bare string added at the top level is well-formed JSON that no reader
        // of this envelope looks at, which is indistinguishable from not warning at all.
        ObjectNode inner = diagnostics.get("diagnostics") instanceof ObjectNode
            ? (ObjectNode) diagnostics.get("diagnostics")
            : diagnostics;
        ArrayNode warnings = inner.get("warnings") instanceof ArrayNode
            ? (ArrayNode) inner.get("warnings")
            : inner.putArray("warnings");
        ObjectNode w = MAPPER.createObjectNode();
        w.put("type", "sql_rewritten");
        w.put("severity", "info");
        w.put("note", notice);
        warnings.add(w);
    }

    /** {@link #diagnose} for the stats tools, which measure their own n and covariates. */
    private static ObjectNode diagnoseStats(String sql, List<String> covariates,
            double[][] covariateCols, int n, int totalRows, int dropped) {
        try {
            return QuestionDiagnostics.forExtraction(sql, covariates, covariateCols, n,
                totalRows, dropped);
        } catch (Exception e) {
            String reason = compactErrorMessage(e);
            log.println("[askamerica-mcp] diagnostics failed: " + reason);
            return QuestionDiagnostics.incomplete(reason);
        }
    }

    /** {@code critique_query}: a form-level read of a proposed query, without running it. */
    private static String critiqueQuery(String sql) throws Exception {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("sql is required");
        }
        ObjectNode out;
        try {
            out = QuestionDiagnostics.critique(getCatalogConnection(), sql);
        } catch (Exception e) {
            String reason = compactErrorMessage(e);
            log.println("[askamerica-mcp] critique failed: " + reason);
            out = QuestionDiagnostics.incomplete(reason);
        }
        out.put("rubric", QuestionGuidance.RUBRIC);
        return out.toString();
    }

    /** A stats tool's payload plus its diagnostics envelope. The two travel separately so the
     *  payload stays byte-identical to what a host received before diagnostics existed. */
    private static final class StatsOutput {
        final String text;
        final ObjectNode diagnostics;

        StatsOutput(String text, ObjectNode diagnostics) {
            this.text = text;
            this.diagnostics = diagnostics;
        }
    }

    /**
     * Finishes a stats tool: attaches the sample-size bookkeeping to the payload and builds
     * the diagnostics envelope from the same extraction the estimator used. The covariate
     * matrix is read back here rather than recomputed, so collinearity is measured on exactly
     * the columns that entered the model.
     */
    private static StatsOutput statsResult(ObjectNode out, String sql, List<String> covariates,
            StatsEngine.Extraction ex) {
        addExtractionMeta(out, ex);
        double[][] cols = covariates.isEmpty()
            ? null : ex.columnsFor(covariates.toArray(new String[0]));
        return new StatsOutput(out.toString(), diagnoseStats(sql, covariates, cols, ex.n(),
            ex.totalRows, ex.droppedForNull));
    }

    /** {@link #statsResult(ObjectNode, String, List, StatsEngine.Extraction)} for the
     *  labeled-extraction path (panel_fixed_effects, robust_regression with cluster_col). */
    private static StatsOutput statsResult(ObjectNode out, String sql, List<String> covariates,
            StatsEngine.LabeledExtraction ex) {
        addExtractionMeta(out, ex);
        double[][] cols = covariates.isEmpty()
            ? null : ex.columnsFor(covariates.toArray(new String[0]));
        return new StatsOutput(out.toString(), diagnoseStats(sql, covariates, cols, ex.n(),
            ex.totalRows, ex.droppedForNull));
    }

    /** Attaches sample-size bookkeeping every stats tool result shares — how many source
     *  rows the SQL returned vs. how many survived complete-case filtering, so a caller isn't
     *  left guessing whether "n" in the result quietly excludes rows with a null. */
    private static void addExtractionMeta(ObjectNode out, StatsEngine.Extraction ex) {
        out.put("rows_returned_by_sql", ex.totalRows);
        out.put("rows_dropped_for_null", ex.droppedForNull);
    }

    /** Same as {@link #addExtractionMeta(ObjectNode, StatsEngine.Extraction)} for the
     *  labeled-extraction path (panel_fixed_effects, robust_regression with cluster_col). */
    private static void addExtractionMeta(ObjectNode out, StatsEngine.LabeledExtraction ex) {
        out.put("rows_returned_by_sql", ex.totalRows);
        out.put("rows_dropped_for_null", ex.droppedForNull);
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

    /**
     * Rebuilds every still-pending deferred view against current data on the live catalog
     * connection, then discards all cached schema connections so the next call reconnects
     * fresh. The rebuild writes into the on-disk DuckDB catalog file, which makes it newer
     * than the running jar's bundled seed — {@code GovDataSeedInstaller} checks exactly that
     * before ever re-extracting, so a later process restart won't overwrite this rebuild.
     */
    private static String updateSchema() throws Exception {
        Connection conn = getCatalogConnection();
        org.apache.calcite.jdbc.CalciteConnection calciteConn =
            conn.unwrap(org.apache.calcite.jdbc.CalciteConnection.class);
        org.apache.calcite.adapter.file.duckdb.DuckDBCatalogMaintenance.rebuildPendingViews(
            calciteConn);

        int discarded = 0;
        for (String key : new java.util.ArrayList<>(schemaConns.keySet())) {
            Connection existing = schemaConns.remove(key);
            schemaConnOpenedAtMillis.remove(key);
            schemaLatches.remove(key);
            schemaErrors.remove(key);
            closeQuietly(existing, key);
            discarded++;
        }
        log.println("[askamerica-mcp] update_schema rebuilt catalog, discarded " + discarded
            + " cached connection(s)");
        return "Schema catalog rebuilt against current data. " + discarded
            + " cached connection(s) discarded — the next tool call reconnects fresh.";
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

    /**
     * Fails on an argument name the tool does not define, naming the closest one it does.
     *
     * <p>A misspelled optional argument is otherwise invisible: the reader asks for the name it
     * knows, does not find it, and uses a default. The caller sees either a success carrying the
     * wrong thing, or an error about some downstream field that has nothing to do with the
     * mistake. {@code render_chart} lost six round trips to exactly this in one run — the caller
     * sent {@code type} instead of {@code chart_type}, silently got {@code line}, and then chased
     * errors about categories and series while its actual payload was fine.
     *
     * <p>Suggesting a near-miss matters more than the rejection: {@code type} vs
     * {@code chart_type} is the whole diagnosis, and without it the caller has only the tool
     * schema to re-read.
     */
    static void checkKnownArgs(JsonNode args, String tool, String... known) {
        checkArgs(args, tool, new java.util.LinkedHashSet<>(java.util.Arrays.asList(known)),
            java.util.Collections.<String>emptySet());
    }

    /**
     * Validates one call's arguments against the tool's advertised schema.
     *
     * <p>Two failures, both of which used to pass silently:
     *
     * <p>An <b>unknown</b> name means the value the caller sent is never read. The reader asks
     * for the name it knows, does not find it, and proceeds on a default or an empty string.
     * {@code render_chart} lost six round trips to this in one run. {@code search_catalog} lost
     * an entire investigation to it: a caller sent {@code keyword} instead of {@code query},
     * the missing {@code query} read as {@code ""}, and the tool answered {@code []} — which
     * the caller reasonably read as "this catalog has no income tables" and stopped searching.
     *
     * <p>A <b>missing required</b> name is the same wound from the other side. {@code required}
     * is declared in the schema the client was handed, so honouring it here costs nothing and
     * turns a plausible-looking empty answer into a diagnosis.
     */
    static void checkArgs(JsonNode args, String tool, java.util.Set<String> allowed,
        java.util.Set<String> required) {
        java.util.List<String> unknown = new java.util.ArrayList<>();
        java.util.List<String> missing = new java.util.ArrayList<>();
        if (args != null && args.isObject()) {
            java.util.Iterator<String> names = args.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                if (!allowed.contains(name)) {
                    unknown.add(name);
                }
            }
        }
        for (String req : required) {
            JsonNode v = args == null ? null : args.get(req);
            if (v == null || v.isNull()
                || (v.isTextual() && v.asText().trim().isEmpty())) {
                missing.add(req);
            }
        }
        if (unknown.isEmpty() && missing.isEmpty()) {
            return;
        }
        StringBuilder msg = new StringBuilder();
        for (String bad : unknown) {
            if (msg.length() > 0) {
                msg.append("; ");
            }
            msg.append(tool).append(" has no argument '").append(bad).append("'");
            String near = closestArg(bad, allowed, missing);
            if (near != null) {
                msg.append(" — did you mean '").append(near).append("'?");
            }
        }
        for (String req : missing) {
            if (msg.length() > 0) {
                msg.append("; ");
            }
            msg.append(tool).append(" requires '").append(req).append("'");
        }
        msg.append(". Known arguments: ").append(String.join(", ", allowed));
        throw new IllegalArgumentException(msg.toString());
    }

    /**
     * The known argument a bad one most plausibly meant, or null when nothing is close.
     *
     * <p>Ordered by how much the suggestion can be trusted. A required argument the call left
     * unfilled is the strongest evidence available — when exactly one is missing and one name
     * is unrecognised, the unrecognised name was meant to be it, whatever the two words look
     * like. That is the only rule that maps {@code keyword} to {@code query}; nothing about
     * the spelling relates them. Containment and edit distance then catch the ordinary
     * abbreviations and typos ({@code type} for {@code chart_type}, {@code querry}).
     */
    private static String closestArg(String bad, java.util.Set<String> known,
        java.util.List<String> missingRequired) {
        if (missingRequired.size() == 1) {
            return missingRequired.get(0);
        }
        String lower = bad.toLowerCase(java.util.Locale.ROOT);
        // A containment match catches the common shape: an abbreviation of the real name.
        for (String k : known) {
            String kl = k.toLowerCase(java.util.Locale.ROOT);
            if (kl.contains(lower) || lower.contains(kl)) {
                return k;
            }
        }
        // Then a typo: near in edit distance relative to the length of the name.
        String best = null;
        int bestDist = Integer.MAX_VALUE;
        for (String k : known) {
            int d = editDistance(lower, k.toLowerCase(java.util.Locale.ROOT));
            if (d < bestDist) {
                bestDist = d;
                best = k;
            }
        }
        return best != null && bestDist <= Math.max(1, best.length() / 3) ? best : null;
    }

    /**
     * Damerau-Levenshtein distance, used only to suggest a near-miss argument name.
     *
     * <p>Counts an adjacent transposition as one edit rather than two. Plain Levenshtein scores
     * {@code limti} two edits away from {@code limit} and so rejects it at any sane threshold,
     * which loses the single most common typing mistake there is.
     */
    private static int editDistance(String a, String b) {
        int[][] d = new int[a.length() + 1][b.length() + 1];
        for (int i = 0; i <= a.length(); i++) {
            d[i][0] = i;
        }
        for (int j = 0; j <= b.length(); j++) {
            d[0][j] = j;
        }
        for (int i = 1; i <= a.length(); i++) {
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                d[i][j] = Math.min(d[i - 1][j - 1] + cost,
                    Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1));
                if (i > 1 && j > 1
                    && a.charAt(i - 1) == b.charAt(j - 2)
                    && a.charAt(i - 2) == b.charAt(j - 1)) {
                    d[i][j] = Math.min(d[i][j], d[i - 2][j - 2] + 1);
                }
            }
        }
        return d[a.length()][b.length()];
    }

    /**
     * Validates a call against the tool's own advertised {@code inputSchema}.
     *
     * <p>Reads {@link #toolDefs()} rather than a per-tool argument list written out at the call
     * site, so a tool cannot advertise one set of arguments and enforce another, and a tool
     * added later is covered without anyone remembering to wire it up.
     */
    /**
     * Argument names callers reach for that are not the ones we chose, mapped to the ones we did.
     *
     * <p>Measured, not guessed: {@code search_catalog}'s argument is {@code query}, and across
     * six eval runs agents reached for {@code keyword} or {@code keywords} four times. The
     * suggestion in the rejection message is good enough that they self-correct on the next
     * call, so nothing was ever wrong with the ANSWER — it just cost a round trip every single
     * run. A search tool taking a keyword is not a mistake worth charging for.
     *
     * <p>This is an explicit rename, not a relaxation. Unknown arguments are still rejected, and
     * that rejection is deliberate — silently ignoring a misspelled argument once made this
     * server answer as though data were absent when the caller had simply named a field wrong.
     * An alias only applies when the real argument is ABSENT: passing both leaves the alias in
     * place and the call is rejected, rather than one of the two being quietly discarded.
     */
    private static final java.util.Map<String, java.util.Map<String, String>> ARG_ALIASES;

    static {
        java.util.Map<String, java.util.Map<String, String>> m = new java.util.HashMap<>();
        java.util.Map<String, String> search = new java.util.LinkedHashMap<>();
        search.put("keyword", "query");
        search.put("keywords", "query");
        search.put("term", "query");
        search.put("terms", "query");
        search.put("q", "query");
        search.put("search", "query");
        m.put("search_catalog", java.util.Collections.unmodifiableMap(search));
        ARG_ALIASES = java.util.Collections.unmodifiableMap(m);
    }

    /** Renames known synonyms in place; see {@link #ARG_ALIASES}. */
    static void applyArgAliases(String tool, JsonNode args) {
        java.util.Map<String, String> aliases = ARG_ALIASES.get(tool);
        if (aliases == null || args == null || !args.isObject()) {
            return;
        }
        ObjectNode obj = (ObjectNode) args;
        for (java.util.Map.Entry<String, String> e : aliases.entrySet()) {
            if (obj.has(e.getKey()) && !obj.has(e.getValue())) {
                obj.set(e.getValue(), obj.get(e.getKey()));
                obj.remove(e.getKey());
            }
        }
    }

    private static void validateArgs(String tool, JsonNode args) {
        for (JsonNode def : toolDefs()) {
            if (!tool.equals(def.path("name").asText())) {
                continue;
            }
            JsonNode schema = def.path("inputSchema");
            java.util.Set<String> allowed = new java.util.LinkedHashSet<>();
            java.util.Iterator<String> props = schema.path("properties").fieldNames();
            while (props.hasNext()) {
                allowed.add(props.next());
            }
            java.util.Set<String> required = new java.util.LinkedHashSet<>();
            for (JsonNode r : schema.path("required")) {
                required.add(r.asText());
            }
            checkArgs(args, tool, allowed, required);
            return;
        }
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
