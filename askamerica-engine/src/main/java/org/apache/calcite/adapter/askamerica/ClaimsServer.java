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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Serves the claim-by-claim verdicts of published validations over loopback, keyed by the
 * article URL, so a browser extension can overlay them on the page a reader is looking at.
 *
 * <p>Unlike {@link ArtifactServer} this listens on a <b>fixed</b> port (default
 * {@value #DEFAULT_PORT}, overridable with {@code -Daskamerica.claims.port}): a client that has
 * no way to discover an ephemeral port has to know where to knock. What it gives up in
 * obscurity it keeps elsewhere:
 *
 * <ul>
 *   <li>bound to <b>127.0.0.1</b> only;</li>
 *   <li><b>no enumeration</b>: there is no listing endpoint. A caller gets a validation back only
 *       by presenting the exact article URL it was published for, and {@code /status} reports a
 *       count, never the URLs;</li>
 *   <li><b>GET and HEAD only</b>, read-only, the newest {@value #CAPACITY} validations kept;</li>
 *   <li><b>in memory</b>, gone with the process, like the reports themselves.</li>
 * </ul>
 *
 * <p>If the port is already taken (a second engine on the same machine) this server simply does
 * not start; publishing still works and the extension talks to whichever instance got the port.
 */
final class ClaimsServer {

    private ClaimsServer() {}

    static final int DEFAULT_PORT = 45123;
    private static final int CAPACITY = 64;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, ObjectNode> VALIDATIONS =
        new LinkedHashMap<String, ObjectNode>(16, 0.75f, false) {
            @Override protected boolean removeEldestEntry(Map.Entry<String, ObjectNode> e) {
                return size() > CAPACITY;
            }
        };

    private static HttpServer server;
    private static int port = -1;

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

    /** Records a validation for the extension to find by article URL. */
    static synchronized void record(String sourceUrl, String title, String reportUrl,
        JsonNode claims) {
        String key = normalize(sourceUrl);
        if (key == null || claims == null || !claims.isArray() || claims.size() == 0) {
            return;
        }
        ObjectNode v = MAPPER.createObjectNode();
        v.put("url", sourceUrl);
        v.put("normalized_url", key);
        v.put("title", title == null ? "" : title);
        v.put("report_url", reportUrl == null ? "" : reportUrl);
        v.put("published_at", Instant.now().toString());
        ObjectNode tally = MAPPER.createObjectNode();
        for (JsonNode c : claims) {
            String verdict = c.path("verdict").asText("").trim().toLowerCase(Locale.ROOT);
            if (!verdict.isEmpty()) {
                tally.put(verdict, tally.path(verdict).asInt(0) + 1);
            }
        }
        v.set("tally", tally);
        v.set("claims", claims.deepCopy());
        VALIDATIONS.remove(key);
        VALIDATIONS.put(key, v);
    }

    static synchronized ObjectNode lookup(String url) {
        String key = normalize(url);
        return key == null ? null : VALIDATIONS.get(key);
    }

    static synchronized int size() {
        return VALIDATIONS.size();
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
