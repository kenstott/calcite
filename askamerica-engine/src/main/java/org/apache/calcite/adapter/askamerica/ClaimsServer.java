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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Serves the claim-by-claim verdicts of recently-published validations over loopback, keyed by
 * the article URL, so a browser extension can overlay them on the page a reader is looking at.
 *
 * <p>Storage is a small table ({@code askamerica_reports}) in the shared pgwire-govdata DuckDB
 * catalog (see {@link PgwireGovDataConnector}), not an in-memory map. Measured live 2026-09-23:
 * every Desktop conversation runs its own separate engine process, so a per-process in-memory
 * map meant a validation published in one conversation was invisible to every other conversation
 * (and to the extension, if that conversation's process wasn't the one that happened to win the
 * loopback port below) — a report could publish successfully and still never appear anywhere the
 * user could see it. Routing storage through the connection every conversation already shares
 * fixes that for free: whichever conversation writes a row, every conversation (and the
 * extension) reading through this same shared catalog sees it. All SQL here synchronizes on
 * {@link McpServer#DB_LOCK}, the same lock guarding every other use of that shared connection —
 * see that field's javadoc for why a second, uncoordinated lock would be unsafe.
 *
 * <p>"Recently" means the last {@value #WINDOW_HOURS} hours, not "this session" — there is no
 * well-defined single session once storage is shared across every open conversation, so a rolling
 * time window is the honest replacement: old enough to be irrelevant to what's likely still open
 * in a browser tab, recent enough that re-validating a fast-moving story is expected.
 *
 * <p>Unlike {@link ArtifactServer} this listens on a <b>fixed</b> port (default
 * {@value #DEFAULT_PORT}, overridable with {@code -Daskamerica.claims.port}): a client that has
 * no way to discover an ephemeral port has to know where to knock. What it gives up in
 * obscurity it keeps elsewhere:
 *
 * <ul>
 *   <li>bound to <b>127.0.0.1</b> only;</li>
 *   <li><b>no HTTP enumeration</b>: this loopback server itself has no listing endpoint. A
 *       caller gets a validation back only by presenting the exact article URL it was published
 *       for, and {@code /status} reports a count, never the URLs — because any webpage's own
 *       JavaScript can reach a plain loopback HTTP server with a bare {@code fetch()}, so an
 *       enumeration endpoint here would let any site read a user's whole validation history.
 *       {@link #listAll()} is a different trust boundary: it backs the {@code list_reports} MCP
 *       tool, reachable only through a real MCP client connection (Claude Desktop's connector),
 *       never by arbitrary page JS — the privacy concern above does not apply to it;</li>
 *   <li><b>GET and HEAD only</b>, read-only.</li>
 * </ul>
 *
 * <p>If the port is already taken (a second engine process on the same machine already bound it)
 * this server simply does not start on THAT process — but publishing and reading both go through
 * the shared table regardless, so this only affects which process's HTTP endpoint the extension
 * happens to be pointed at, not whether a given conversation's data is visible anywhere.
 */
final class ClaimsServer {

    private ClaimsServer() {}

    static final int DEFAULT_PORT = 45123;
    private static final int WINDOW_HOURS = 24;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static volatile boolean tableEnsured;

    private static HttpServer server;
    private static int port = -1;

    /**
     * A shared secret gating {@code /reports}, unlike {@code /claims} (single exact-URL
     * lookup — an attacker page has to already know the URL to probe) and {@code /status} (just
     * a count). {@code /reports} returns the last {@value #WINDOW_HOURS} hours of validation
     * history in one call, so it needs real protection, not just obscurity: {@code cors()}'s wildcard
     * {@code Access-Control-Allow-Origin} exists for the extension's benefit but is readable by
     * any webpage's own fetch too, and {@code host_permissions} already exempts the extension's
     * background service worker from CORS regardless of what this server sends back — so CORS
     * headers were never actually doing access control here, just enabling it for everyone.
     *
     * <p>Compiled into both this engine and the extension's {@code background.js} (kept in sync
     * by hand — the two live in separate repos/release pipelines) rather than generated
     * per-machine: no pairing step, no options-page UI, nothing for a user to copy. This is
     * deliberately NOT a strong secret — anyone who downloads the public jar or unpacks the
     * public extension source can read it out just as easily as this comment. It filters out
     * opportunistic driveby scripts (a page probing common localhost ports for anything that
     * answers), which is the actual threat this endpoint faces; it does nothing against a
     * targeted attacker willing to go find this constant, and must never be treated as
     * protecting anything more sensitive than the read-only validation history it gates here.
     */
    private static final String REPORTS_KEY = "aa-reports-9f3c1e7b2a48d0c6";

    /** Starts the listener once; silent no-op when the port is busy. */
    static synchronized void start(PrintStream log) {
        if (server != null) {
            return;
        }
        int p = DEFAULT_PORT;
        String prop = System.getProperty("askamerica.claims.port");
        if (prop != null && !prop.trim().isEmpty()) {
            p = Integer.parseInt(prop.trim());
        }
        try {
            server = HttpServer.create(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), p), 0);
            server.createContext("/claims", ClaimsServer::handleClaims);
            server.createContext("/status", ClaimsServer::handleStatus);
            server.createContext("/reports", ClaimsServer::handleReports);
            server.setExecutor(null);
            server.start();
            port = p;
            log.println("[askamerica-mcp] claims endpoint http://127.0.0.1:" + p + "/claims");
        } catch (IOException e) {
            server = null;
            log.println("[askamerica-mcp] claims endpoint not started on port " + p + ": "
                + e.getMessage());
        }
    }

    private static Connection connection() throws Exception {
        // "ref" is an arbitrary, always-mounted default schema name -- irrelevant when
        // PgwireGovDataConnector is enabled (the default), since one shared connection answers
        // for every schema; only used as a cache key on the rare embedded-mode opt-out path.
        return McpServer.getSchemaConnection("ref");
    }

    /** Idempotent; cheap to call every time ({@code CREATE TABLE IF NOT EXISTS}) rather than
     *  tracked per-connection, since the shared connection can be replaced underneath this
     *  class (a schema-connection TTL refresh, or pgwire-govdata itself restarting) without
     *  this class finding out. */
    private static void ensureTable(Connection c) throws Exception {
        if (tableEnsured) {
            return;
        }
        try (Statement st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS askamerica_reports ("
                + "normalized_url VARCHAR PRIMARY KEY, url VARCHAR, title VARCHAR, "
                + "report_url VARCHAR, published_at TIMESTAMP, tally_json VARCHAR, "
                + "claims_json VARCHAR)");
        }
        tableEnsured = true;
    }

    /** Records a validation for the extension to find by article URL. Best-effort: a storage
     *  failure is logged, not thrown -- publish_report's own success already happened, and a
     *  reader losing the extension overlay is a lesser failure than the whole publish erroring
     *  out over a problem in a side table. */
    static void record(String sourceUrl, String title, String reportUrl, JsonNode claims) {
        String key = normalize(sourceUrl);
        if (key == null || claims == null || !claims.isArray() || claims.size() == 0) {
            return;
        }
        ObjectNode tally = MAPPER.createObjectNode();
        for (JsonNode c : claims) {
            String verdict = c.path("verdict").asText("").trim().toLowerCase(Locale.ROOT);
            if (!verdict.isEmpty()) {
                tally.put(verdict, tally.path(verdict).asInt(0) + 1);
            }
        }
        synchronized (McpServer.DB_LOCK) {
            try {
                Connection c = connection();
                ensureTable(c);
                // Opportunistic cleanup on every write keeps the table small over a
                // long-lived shared pgwire-govdata process without needing a separate
                // scheduled job -- report publishing is infrequent enough that piggybacking
                // here costs nothing noticeable.
                try (Statement st = c.createStatement()) {
                    st.execute("DELETE FROM askamerica_reports WHERE published_at <= "
                        + "now() - INTERVAL '" + WINDOW_HOURS + " hours'");
                }
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO askamerica_reports (normalized_url, url, title, "
                        + "report_url, published_at, tally_json, claims_json) "
                        + "VALUES (?, ?, ?, ?, now(), ?, ?) "
                        + "ON CONFLICT (normalized_url) DO UPDATE SET url = excluded.url, "
                        + "title = excluded.title, report_url = excluded.report_url, "
                        + "published_at = excluded.published_at, "
                        + "tally_json = excluded.tally_json, "
                        + "claims_json = excluded.claims_json")) {
                    ps.setString(1, key);
                    ps.setString(2, sourceUrl);
                    ps.setString(3, title == null ? "" : title);
                    ps.setString(4, reportUrl == null ? "" : reportUrl);
                    ps.setString(5, tally.toString());
                    ps.setString(6, claims.toString());
                    ps.executeUpdate();
                }
            } catch (Exception e) {
                McpServer.logLine("[askamerica-mcp] ClaimsServer.record failed: "
                    + e.getMessage());
            }
        }
    }

    static ObjectNode lookup(String url) {
        String key = normalize(url);
        if (key == null) {
            return null;
        }
        synchronized (McpServer.DB_LOCK) {
            try {
                Connection c = connection();
                ensureTable(c);
                try (PreparedStatement ps = c.prepareStatement(
                        "SELECT url, title, report_url, published_at, tally_json, claims_json "
                        + "FROM askamerica_reports WHERE normalized_url = ? "
                        + "AND published_at > now() - INTERVAL '" + WINDOW_HOURS + " hours'")) {
                    ps.setString(1, key);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            return null;
                        }
                        ObjectNode v = MAPPER.createObjectNode();
                        v.put("url", rs.getString("url"));
                        v.put("normalized_url", key);
                        v.put("title", rs.getString("title"));
                        v.put("report_url", rs.getString("report_url"));
                        v.put("published_at", String.valueOf(rs.getTimestamp("published_at")));
                        v.set("tally", MAPPER.readTree(rs.getString("tally_json")));
                        v.set("claims", MAPPER.readTree(rs.getString("claims_json")));
                        return v;
                    }
                }
            } catch (Exception e) {
                McpServer.logLine("[askamerica-mcp] ClaimsServer.lookup failed: "
                    + e.getMessage());
                return null;
            }
        }
    }

    /**
     * Every URL-based validation published in the last {@value #WINDOW_HOURS} hours, across
     * every conversation sharing this machine's pgwire-govdata connection, newest first. Backs
     * the {@code list_reports} MCP tool; see the class javadoc for why this is safe to expose
     * here but not as an HTTP endpoint without {@link #REPORTS_KEY}. Each entry omits the full
     * {@code claims} array (present in {@link #lookup}'s single-URL result) and keeps only the
     * tally, since a list of many reports is for finding the right one, not re-reading every
     * claim inline.
     */
    static List<ObjectNode> listAll() {
        List<ObjectNode> out = new ArrayList<>();
        synchronized (McpServer.DB_LOCK) {
            try {
                Connection c = connection();
                ensureTable(c);
                try (PreparedStatement ps = c.prepareStatement(
                        "SELECT url, title, report_url, published_at, tally_json "
                        + "FROM askamerica_reports "
                        + "WHERE published_at > now() - INTERVAL '" + WINDOW_HOURS + " hours' "
                        + "ORDER BY published_at DESC")) {
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            ObjectNode summary = MAPPER.createObjectNode();
                            summary.put("url", rs.getString("url"));
                            summary.put("title", rs.getString("title"));
                            summary.put("report_url", rs.getString("report_url"));
                            summary.put("published_at",
                                String.valueOf(rs.getTimestamp("published_at")));
                            summary.set("tally", MAPPER.readTree(rs.getString("tally_json")));
                            out.add(summary);
                        }
                    }
                }
            } catch (Exception e) {
                McpServer.logLine("[askamerica-mcp] ClaimsServer.listAll failed: "
                    + e.getMessage());
            }
        }
        return out;
    }

    static int size() {
        synchronized (McpServer.DB_LOCK) {
            try {
                Connection c = connection();
                ensureTable(c);
                try (Statement st = c.createStatement();
                     ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM askamerica_reports "
                         + "WHERE published_at > now() - INTERVAL '" + WINDOW_HOURS + " hours'")) {
                    return rs.next() ? rs.getInt(1) : 0;
                }
            } catch (Exception e) {
                McpServer.logLine("[askamerica-mcp] ClaimsServer.size failed: "
                    + e.getMessage());
                return 0;
            }
        }
    }

    static int port() {
        return port;
    }

    /** Scheme and host lower-cased, fragment dropped, tracking parameters dropped, trailing
     *  slash dropped: the same article reached two ways is one validation. */
    static String normalize(String url) {
        if (url == null) {
            return null;
        }
        String u = url.trim();
        if (u.isEmpty()) {
            return null;
        }
        try {
            URI uri = new URI(u);
            String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
            String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
            if (host.startsWith("www.")) {
                host = host.substring(4);
            }
            String path = uri.getRawPath() == null ? "" : uri.getRawPath();
            while (path.endsWith("/") && path.length() > 1) {
                path = path.substring(0, path.length() - 1);
            }
            List<String> keep = new ArrayList<>();
            String q = uri.getRawQuery();
            if (q != null && !q.isEmpty()) {
                for (String kv : q.split("&")) {
                    String k = kv.contains("=") ? kv.substring(0, kv.indexOf('=')) : kv;
                    String kl = k.toLowerCase(Locale.ROOT);
                    if (kl.startsWith("utm_") || kl.equals("fbclid") || kl.equals("gclid")
                        || kl.equals("ref") || kl.equals("source")) {
                        continue;
                    }
                    keep.add(kv);
                }
            }
            String query = keep.isEmpty() ? "" : "?" + String.join("&", keep);
            int portPart = uri.getPort();
            String hp = host + (portPart > 0 ? ":" + portPart : "");
            return scheme + "://" + hp + path + query;
        } catch (Exception e) {
            return u;
        }
    }

    private static void handleClaims(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        if (!"GET".equals(method) && !"HEAD".equals(method) && !"OPTIONS".equals(method)) {
            ex.sendResponseHeaders(405, -1);
            ex.close();
            return;
        }
        cors(ex);
        if ("OPTIONS".equals(method)) {
            ex.sendResponseHeaders(204, -1);
            ex.close();
            return;
        }
        String query = ex.getRequestURI().getRawQuery();
        String target = null;
        if (query != null) {
            for (String kv : query.split("&")) {
                if (kv.startsWith("url=")) {
                    target = URLDecoder.decode(kv.substring(4), StandardCharsets.UTF_8.name());
                }
            }
        }
        ObjectNode v = target == null ? null : lookup(target);
        if (v == null) {
            ObjectNode miss = MAPPER.createObjectNode();
            miss.put("found", false);
            miss.put("url", target == null ? "" : target);
            send(ex, 404, miss.toString(), "HEAD".equals(method));
            return;
        }
        ObjectNode hit = v.deepCopy();
        hit.put("found", true);
        send(ex, 200, hit.toString(), "HEAD".equals(method));
    }

    private static void handleStatus(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        cors(ex);
        if ("OPTIONS".equals(method)) {
            ex.sendResponseHeaders(204, -1);
            ex.close();
            return;
        }
        if (!"GET".equals(method) && !"HEAD".equals(method)) {
            ex.sendResponseHeaders(405, -1);
            ex.close();
            return;
        }
        ObjectNode s = MAPPER.createObjectNode();
        s.put("ok", true);
        s.put("service", "askamerica-engine");
        s.put("validations", size());
        ArrayNode v = s.putArray("verdicts");
        for (String verdict : McpServer.VERDICTS) {
            v.add(verdict);
        }
        send(ex, 200, s.toString(), "HEAD".equals(method));
    }

    /**
     * The engine/extension handshake: a query-string key (not a header, so no CORS preflight
     * complexity — the extension's own fetch is exempt from CORS via {@code host_permissions}
     * anyway) checked against {@link #REPORTS_KEY} with a constant-time comparison. No
     * {@code cors()} call here — see {@link #REPORTS_KEY}'s javadoc for why this endpoint
     * deliberately doesn't advertise cross-origin readability the way {@code /claims} and
     * {@code /status} do.
     */
    private static void handleReports(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        if (!"GET".equals(method) && !"HEAD".equals(method)) {
            ex.sendResponseHeaders(405, -1);
            ex.close();
            return;
        }
        String query = ex.getRequestURI().getRawQuery();
        String key = null;
        if (query != null) {
            for (String kv : query.split("&")) {
                if (kv.startsWith("key=")) {
                    key = URLDecoder.decode(kv.substring(4), StandardCharsets.UTF_8.name());
                }
            }
        }
        if (key == null || !java.security.MessageDigest.isEqual(
                key.getBytes(StandardCharsets.UTF_8), REPORTS_KEY.getBytes(StandardCharsets.UTF_8))) {
            ex.sendResponseHeaders(403, -1);
            ex.close();
            return;
        }
        ArrayNode arr = MAPPER.createArrayNode();
        for (ObjectNode r : listAll()) {
            arr.add(r);
        }
        ObjectNode body = MAPPER.createObjectNode();
        body.set("reports", arr);
        send(ex, 200, body.toString(), "HEAD".equals(method));
    }

    private static void cors(HttpExchange ex) {
        // Loopback only, no enumeration; the extension's origin is chrome-extension://<id>,
        // which is not a web origin, so a wildcard is the only workable allow-list.
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
        ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
        ex.getResponseHeaders().set("Cache-Control", "no-store");
    }

    private static void send(HttpExchange ex, int status, String json, boolean headOnly)
        throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        if (headOnly) {
            ex.sendResponseHeaders(status, -1);
            ex.close();
            return;
        }
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }
}
