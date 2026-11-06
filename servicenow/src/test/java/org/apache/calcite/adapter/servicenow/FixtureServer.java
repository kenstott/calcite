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
package org.apache.calcite.adapter.servicenow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * A local HTTP server that plays a ServiceNow instance's Table API, so that the real client code
 * runs end to end without a network.
 *
 * <p>It serves the documentation-derived fixtures under {@code servicenow/doc-derived/tables}
 * (see the README there: they are NOT captured from a real instance). It implements only what
 * the adapter is expected to send:
 * <ul>
 *   <li>HTTP Basic authentication;
 *   <li>{@code sysparm_fields}, {@code sysparm_limit}, {@code sysparm_display_value} false and
 *   all, {@code sysparm_exclude_reference_link};
 *   <li>{@code sysparm_query} made only of {@code sys_id><key>}, {@code ORDERBYsys_id} and, on
 *   {@code sys_dictionary}, {@code nameIN<a>,<b>} (the adapter's per-table column read; checked on
 *   dev453859 2026-10-12), and on {@code sys_transform_map} / {@code sys_transform_entry},
 *   {@code <field>=<value>} terms (the import binding's lookups). Any other term is answered
 *   with HTTP 400, so a filter that starts being pushed down fails the tests instead of being
 *   silently accepted.
 * </ul>
 * Writes: POST to {@code /api/now/table/<table>} adds a row with a new sys_id (201), PATCH and
 * DELETE of {@code /api/now/table/<table>/<sys_id>} change or remove it (200, 204; 404 if there is
 * none). Every write is recorded with its method and JSON body. The Import Set API
 * ({@code /api/now/import/<staging>}) has no default answer: a test supplies it with
 * {@link #override}.
 *
 * <p>A test can take over any request with {@link #override}.
 */
final class FixtureServer implements AutoCloseable {

  static final String USER = "svc_user";
  static final String PASSWORD = "s3cret";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** What the server answers. */
  static final class Reply {
    int status;
    final String contentType;
    final String body;
    final Map<String, String> headers = new LinkedHashMap<>();

    Reply(int status, String contentType, String body) {
      this.status = status;
      this.contentType = contentType;
      this.body = body;
    }

    static Reply json(String body) {
      return new Reply(200, "application/json;charset=UTF-8", body);
    }

    Reply withStatus(int newStatus) {
      this.status = newStatus;
      return this;
    }

    Reply header(String name, String value) {
      headers.put(name, value);
      return this;
    }
  }

  /** A request as the server saw it. */
  static final class Seen {
    final String method;
    /** "table" or "import". */
    final String api;
    final String table;
    /** The record's sys_id for a PATCH or DELETE; null otherwise. */
    final String sysId;
    final Map<String, String> params;
    final String authorization;
    /** The request's JSON body, or null. */
    final JsonNode body;

    Seen(String method, String api, String table, String sysId, Map<String, String> params,
        String authorization, JsonNode body) {
      this.method = method;
      this.api = api;
      this.table = table;
      this.sysId = sysId;
      this.params = params;
      this.authorization = authorization;
      this.body = body;
    }

    @Override public String toString() {
      return method + " " + api + " " + table + (sysId == null ? "" : "/" + sysId) + " "
          + params + (body == null ? "" : " " + body);
    }
  }

  private final HttpServer server;
  private final Map<String, ArrayNode> tables = new LinkedHashMap<>();
  private final List<Seen> requests = new CopyOnWriteArrayList<>();
  private volatile Function<Seen, Reply> override = seen -> null;
  private volatile boolean lenientTerms;
  private int written;

  FixtureServer() throws IOException {
    for (String table : new String[] {"sys_db_object", "sys_dictionary", "sys_glide_object",
        "sys_user", "incident", "bad_table"}) {
      try (InputStream in = FixtureServer.class.getResourceAsStream(
          "/servicenow/doc-derived/tables/" + table + ".json")) {
        if (in == null) {
          throw new IllegalStateException("Missing fixture for " + table);
        }
        tables.put(table, (ArrayNode) MAPPER.readTree(in).get("result"));
      }
    }
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/api/now/table/", this::handle);
    server.createContext("/api/now/import/", this::handle);
    server.start();
  }

  String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  /** Requests answered so far. */
  List<Seen> requests() {
    return requests;
  }

  /** Requests for one table. */
  List<Seen> requests(String table) {
    final List<Seen> result = new ArrayList<>();
    for (Seen seen : requests) {
      if (seen.table.equals(table)) {
        result.add(seen);
      }
    }
    return result;
  }

  /** Write requests (anything but GET), in order. */
  List<Seen> writes() {
    final List<Seen> result = new ArrayList<>();
    for (Seen seen : requests) {
      if (!seen.method.equals("GET")) {
        result.add(seen);
      }
    }
    return result;
  }

  /** Adds rows to a table, creating it if the fixtures have none. */
  void addRows(String table, String json) throws IOException {
    final ArrayNode rows = tables.computeIfAbsent(table, t -> MAPPER.createArrayNode());
    rows.addAll((ArrayNode) MAPPER.readTree(json));
  }

  /** The rows a table holds now. */
  ArrayNode rows(String table) {
    return tables.get(table);
  }

  /**
   * Makes the server ignore query terms it does not understand instead of answering 400. For tests
   * of the translator's output text and of plumbing only: the rows returned are then NOT filtered
   * by those terms, so such a test must not draw conclusions from the rows.
   */
  void lenientTerms() {
    lenientTerms = true;
  }

  /** Lets a test answer requests itself; return null to let the fixtures answer. */
  void override(Function<Seen, Reply> override) {
    this.override = override;
  }

  @Override public void close() {
    server.stop(0);
  }

  private void handle(HttpExchange exchange) throws IOException {
    final String path = exchange.getRequestURI().getPath();
    final String api = path.startsWith("/api/now/import/") ? "import" : "table";
    final String rest = path.substring(("/api/now/" + api + "/").length());
    final int slash = rest.indexOf('/');
    final String table = slash < 0 ? rest : rest.substring(0, slash);
    final String sysId = slash < 0 ? null : rest.substring(slash + 1);
    final Map<String, String> params = parseQuery(exchange.getRequestURI().getRawQuery());
    final String authorization = exchange.getRequestHeaders().getFirst("Authorization");
    final String method = exchange.getRequestMethod();
    final byte[] in = exchange.getRequestBody().readAllBytes();
    final JsonNode body = in.length == 0 ? null : MAPPER.readTree(in);
    final Seen seen = new Seen(method, api, table, sysId, params, authorization, body);
    requests.add(seen);

    Reply reply = override.apply(seen);
    if (reply == null) {
      reply = answer(seen);
    }
    final byte[] bytes = reply.body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", reply.contentType);
    reply.headers.forEach((k, v) -> exchange.getResponseHeaders().set(k, v));
    exchange.sendResponseHeaders(reply.status, bytes.length == 0 ? -1 : bytes.length);
    if (bytes.length > 0) {
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    }
    exchange.close();
  }

  private Reply answer(Seen seen) {
    final String expected = "Basic " + Base64.getEncoder()
        .encodeToString((USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
    if (!expected.equals(seen.authorization)) {
      return error(401, "User Not Authenticated", "Required to provide Auth information");
    }
    if (seen.api.equals("import")) {
      return error(400, "FixtureServer has no default answer for the Import Set API; "
          + "the test must override it", "");
    }
    final ArrayNode rows = tables.get(seen.table);
    if (rows == null) {
      return error(400, "Invalid table " + seen.table, "");
    }
    if (!seen.method.equals("GET")) {
      return write(seen, rows);
    }
    String after = null;
    Set<String> names = null;
    final Map<String, String> conditions = new LinkedHashMap<>();
    final String query = seen.params.getOrDefault("sysparm_query", "");
    for (String term : query.isEmpty() ? new String[0] : query.split("\\^")) {
      if (term.startsWith("sys_id>")) {
        after = term.substring("sys_id>".length());
      } else if (term.startsWith("nameIN") && seen.table.equals("sys_dictionary")) {
        names = new HashSet<>(Arrays.asList(term.substring("nameIN".length()).split(",")));
      } else if (seen.table.startsWith("sys_transform_") && term.contains("=")) {
        // The import binding's metadata conditions: field=value, applied below
        conditions.put(term.substring(0, term.indexOf('=')), term.substring(term.indexOf('=') + 1));
      } else if (!term.equals("ORDERBYsys_id") && !lenientTerms) {
        return error(400, "FixtureServer does not accept the query term '" + term + "'", "");
      }
    }
    final int limit = Integer.parseInt(seen.params.getOrDefault("sysparm_limit", "10000"));
    final Set<String> fields = seen.params.containsKey("sysparm_fields")
        ? new HashSet<>(Arrays.asList(seen.params.get("sysparm_fields").split(",")))
        : null;
    final boolean all = "all".equals(seen.params.get("sysparm_display_value"));

    final List<JsonNode> sorted = new ArrayList<>();
    rows.forEach(sorted::add);
    sorted.sort((a, b) -> a.get("sys_id").asText().compareTo(b.get("sys_id").asText()));

    final ArrayNode result = MAPPER.createArrayNode();
    for (JsonNode row : sorted) {
      if (after != null && row.get("sys_id").asText().compareTo(after) <= 0) {
        continue;
      }
      if (names != null && !names.contains(row.get("name").asText())) {
        continue;
      }
      if (!matches(row, conditions)) {
        continue;
      }
      if (result.size() == limit) {
        break;
      }
      final ObjectNode out = result.addObject();
      row.fields().forEachRemaining(e -> {
        if (fields != null && !fields.contains(e.getKey())) {
          return;
        }
        final JsonNode value = e.getValue();
        // Fixtures store a reference as {value, display_value}; every other field as a string
        final String stored = value.isObject() ? value.get("value").asText() : value.asText();
        final String display = value.isObject() ? value.get("display_value").asText() : stored;
        if (all) {
          out.putObject(e.getKey()).put("value", stored).put("display_value", display);
        } else {
          out.put(e.getKey(), stored);
        }
      });
    }
    final ObjectNode body = MAPPER.createObjectNode();
    body.set("result", result);
    return Reply.json(body.toString());
  }

  private Reply write(Seen seen, ArrayNode rows) {
    switch (seen.method) {
    case "POST": {
      final ObjectNode row = ((ObjectNode) seen.body).deepCopy();
      row.put("sys_id", String.format(java.util.Locale.ROOT, "%032d", ++written));
      rows.add(row);
      final ObjectNode reply = MAPPER.createObjectNode();
      reply.set("result", row);
      return Reply.json(reply.toString()).withStatus(201);
    }
    case "PATCH":
    case "DELETE":
      for (int i = 0; i < rows.size(); i++) {
        if (rows.get(i).path("sys_id").asText().equals(seen.sysId)) {
          if (seen.method.equals("DELETE")) {
            rows.remove(i);
            return new Reply(204, "application/json", "");
          }
          final ObjectNode row = (ObjectNode) rows.get(i);
          row.setAll((ObjectNode) seen.body);
          final ObjectNode reply = MAPPER.createObjectNode();
          reply.set("result", row);
          return Reply.json(reply.toString());
        }
      }
      return error(404, "No Record found", "Record doesn't exist or ACL restricts the record "
          + "retrieval");
    default:
      return error(405, "Method not allowed", seen.method);
    }
  }

  private static boolean matches(JsonNode row, Map<String, String> conditions) {
    for (Map.Entry<String, String> c : conditions.entrySet()) {
      final JsonNode value = row.path(c.getKey());
      final String stored = value.isObject() ? value.path("value").asText() : value.asText();
      if (!stored.equals(c.getValue())) {
        return false;
      }
    }
    return true;
  }

  static Reply error(int status, String message, String detail) {
    return Reply.json("{\"error\":{\"message\":\"" + message + "\",\"detail\":\"" + detail
        + "\"},\"status\":\"failure\"}").withStatus(status);
  }

  private static Map<String, String> parseQuery(String raw) {
    final Map<String, String> params = new LinkedHashMap<>();
    if (raw == null) {
      return params;
    }
    for (String pair : raw.split("&")) {
      final int eq = pair.indexOf('=');
      params.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
          URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
    }
    return params;
  }
}
