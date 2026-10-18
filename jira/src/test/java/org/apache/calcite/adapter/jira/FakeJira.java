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
package org.apache.calcite.adapter.jira;

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
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A local HTTP server that plays a Jira Cloud site's REST v3, so the adapter's real client code
 * runs end to end without a network. The shapes follow Atlassian's REST v3 reference; they are
 * not captured from a live site.
 *
 * <p>One project {@code ABC} with issue types Task ({@code 10001}) and Sub-task
 * ({@code 10002}), project {@code XYZ} with Task ({@code 10001}). Issues are kept in memory.
 * A search is answered from the {@code project = "<key>" AND issuetype = <id>} pairs of its JQL
 * only (any other clause is recorded, not applied); every request is recorded. A test can take
 * over any request with {@link #override}.
 */
final class FakeJira implements AutoCloseable {

  static final String EMAIL = "bot@example.com";
  static final String TOKEN = "t0ken";

  static final ObjectMapper MAPPER = new ObjectMapper();

  /** A request as received. */
  static final class Request {
    final String method;
    final String path;
    final String query;
    final JsonNode body;

    Request(String method, String path, String query, JsonNode body) {
      this.method = method;
      this.path = path;
      this.query = query;
      this.body = body;
    }

    @Override public String toString() {
      return method + " " + path;
    }
  }

  /** What the server answers. */
  static final class Reply {
    final int status;
    final String body;
    final Map<String, String> headers = new LinkedHashMap<>();

    Reply(int status, String body) {
      this.status = status;
      this.body = body;
    }
  }

  final List<Request> requests = new CopyOnWriteArrayList<>();
  final Map<String, ObjectNode> issues = new LinkedHashMap<>();
  /** Status name to the transitions out of it: transition id to target status. */
  final Map<String, Map<String, String>> workflow = new LinkedHashMap<>();
  private final List<Function<Request, Reply>> overrides = new CopyOnWriteArrayList<>();
  private final HttpServer server;
  private int nextId = 10000;
  private int nextLink = 20000;

  FakeJira() throws IOException {
    workflow.put("To Do", transitions("11", "In Progress", "31", "Done"));
    workflow.put("In Progress", transitions("21", "To Do", "31", "Done"));
    workflow.put("Done", transitions("41", "To Do"));
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/", this::handle);
    server.start();
  }

  private static Map<String, String> transitions(String... idsAndTargets) {
    final Map<String, String> transitions = new LinkedHashMap<>();
    for (int i = 0; i < idsAndTargets.length; i += 2) {
      transitions.put(idsAndTargets[i], idsAndTargets[i + 1]);
    }
    return transitions;
  }

  String baseUrl() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  void override(Function<Request, Reply> handler) {
    overrides.add(handler);
  }

  @Override public void close() {
    server.stop(0);
  }

  /** Adds an issue of a project and issue type with the given fields. */
  synchronized ObjectNode add(String project, String issueTypeId, ObjectNode fields) {
    final String key = project + "-" + (count(project) + 1);
    final ObjectNode issue = MAPPER.createObjectNode();
    issue.put("id", Integer.toString(nextId++));
    issue.put("key", key);
    final ObjectNode stored = issue.putObject("fields");
    stored.putObject("project").put("key", project);
    stored.putObject("issuetype").put("id", issueTypeId)
        .put("name", issueTypeId.equals("10002") ? "Sub-task" : "Task");
    stored.putObject("status").put("name", "To Do");
    stored.put("created", "2026-10-01T09:00:00.000+0000");
    stored.put("updated", "2026-10-02T10:30:00.000+0000");
    stored.putArray("issuelinks");
    stored.putArray("subtasks");
    stored.setAll(fields);
    issues.put(key, issue);
    return issue;
  }

  private int count(String project) {
    int n = 0;
    for (String key : issues.keySet()) {
      if (key.startsWith(project + "-")) {
        n++;
      }
    }
    return n;
  }

  /** Links two issues, as {@code POST /issueLink} would. */
  synchronized String link(String type, String outward, String inward) {
    final String id = Integer.toString(nextLink++);
    final ObjectNode onOutward = MAPPER.createObjectNode().put("id", id);
    onOutward.putObject("type").put("name", type);
    onOutward.putObject("inwardIssue").put("key", inward);
    ((ArrayNode) issues.get(outward).path("fields").path("issuelinks")).add(onOutward);
    final ObjectNode onInward = MAPPER.createObjectNode().put("id", id);
    onInward.putObject("type").put("name", type);
    onInward.putObject("outwardIssue").put("key", outward);
    ((ArrayNode) issues.get(inward).path("fields").path("issuelinks")).add(onInward);
    return id;
  }

  /** The requests whose method and path are these. */
  List<Request> requests(String method, String path) {
    final List<Request> found = new ArrayList<>();
    for (Request request : requests) {
      if (request.method.equals(method) && request.path.equals(path)) {
        found.add(request);
      }
    }
    return found;
  }

  private void handle(HttpExchange exchange) throws IOException {
    final String method = exchange.getRequestMethod();
    final String path = exchange.getRequestURI().getRawPath();
    final String query = exchange.getRequestURI().getRawQuery() == null
        ? "" : URLDecoder.decode(exchange.getRequestURI().getRawQuery(), StandardCharsets.UTF_8);
    final JsonNode body;
    try (InputStream in = exchange.getRequestBody()) {
      final byte[] bytes = in.readAllBytes();
      body = bytes.length == 0 ? MAPPER.createObjectNode() : MAPPER.readTree(bytes);
    }
    final Request request = new Request(method,
        URLDecoder.decode(path, StandardCharsets.UTF_8), query, body);
    requests.add(request);
    Reply reply = null;
    final String expected = "Basic " + Base64.getEncoder()
        .encodeToString((EMAIL + ":" + TOKEN).getBytes(StandardCharsets.UTF_8));
    if (!expected.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
      reply = new Reply(401, "{\"errorMessages\":[\"Client must be authenticated\"]}");
    }
    for (Function<Request, Reply> override : overrides) {
      if (reply == null) {
        reply = override.apply(request);
      }
    }
    if (reply == null) {
      synchronized (this) {
        reply = answer(request);
      }
    }
    final byte[] out = reply.body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    reply.headers.forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
    if (out.length == 0) {
      exchange.sendResponseHeaders(reply.status, -1);
    } else {
      exchange.sendResponseHeaders(reply.status, out.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(out);
      }
    }
    exchange.close();
  }

  private static final String API = "/rest/api/3";

  private static final Pattern FORM_IN_JQL =
      Pattern.compile("project = \"(\\w+)\" AND issuetype = (\\d+)");

  private Reply answer(Request r) {
    final String p = r.path.startsWith(API) ? r.path.substring(API.length()) : r.path;
    if (r.method.equals("GET") && p.equals("/myself")) {
      return json(MAPPER.createObjectNode().put("accountId", "bot").put("emailAddress", EMAIL));
    }
    if (r.method.equals("GET") && p.equals("/project/search")) {
      return json("{\"isLast\":true,\"values\":[{\"key\":\"ABC\",\"name\":\"Alpha\"},"
          + "{\"key\":\"XYZ\",\"name\":\"Xylo\"}]}");
    }
    if (r.method.equals("GET") && p.matches("/issue/createmeta/\\w+/issuetypes")) {
      return json("{\"startAt\":0,\"maxResults\":50,\"total\":2,\"issueTypes\":["
          + "{\"id\":\"10001\",\"name\":\"Task\",\"subtask\":false},"
          + "{\"id\":\"10002\",\"name\":\"Sub-task\",\"subtask\":true}]}");
    }
    if (r.method.equals("GET") && p.matches("/issue/createmeta/\\w+/issuetypes/\\d+")) {
      return json("{\"startAt\":0,\"maxResults\":50,\"total\":11,\"fields\":["
          + field("summary", "Summary", "{\"type\":\"string\",\"system\":\"summary\"}", true)
          + "," + field("issuetype", "Issue Type", "{\"type\":\"issuetype\"}", true)
          + "," + field("project", "Project", "{\"type\":\"project\"}", true)
          + "," + field("description", "Description", "{\"type\":\"string\"}", false)
          + "," + field("priority", "Priority", "{\"type\":\"priority\"}", false)
          + "," + field("labels", "Labels", "{\"type\":\"array\",\"items\":\"string\"}", false)
          + "," + field("customfield_10016", "Story Points",
              "{\"type\":\"number\",\"custom\":\"x:float\"}", false)
          + "," + field("customfield_10020", "Severity",
              "{\"type\":\"option\",\"custom\":\"x:select\"}", false)
          + "," + field("assignee", "Assignee", "{\"type\":\"user\"}", false)
          + "," + field("duedate", "Due date", "{\"type\":\"date\"}", false)
          + "," + field("customfield_10030", "Team", "{\"type\":\"any\"}", false)
          + "]}");
    }
    if (r.method.equals("GET") && p.equals("/field")) {
      return json("[{\"id\":\"status\",\"name\":\"Status\",\"schema\":{\"type\":\"status\"}},"
          + "{\"id\":\"resolution\",\"name\":\"Resolution\","
          + "\"schema\":{\"type\":\"resolution\"}},"
          + "{\"id\":\"customfield_10040\",\"name\":\"Owner\",\"schema\":{\"type\":\"user\"}},"
          + "{\"id\":\"customfield_10041\",\"name\":\"Owner\",\"schema\":{\"type\":\"user\"}},"
          + "{\"id\":\"customfield_10050\",\"name\":\"Severity\","
          + "\"schema\":{\"type\":\"option\"}}]");
    }
    if (r.method.equals("POST") && p.equals("/search/jql")) {
      return search(r.body);
    }
    if (r.method.equals("POST") && p.equals("/issue/bulkfetch")) {
      final ObjectNode answer = MAPPER.createObjectNode();
      final ArrayNode found = answer.putArray("issues");
      final ArrayNode errors = answer.putArray("issueErrors");
      for (JsonNode key : r.body.path("issueIdsOrKeys")) {
        final ObjectNode issue = issues.get(key.asText());
        if (issue == null) {
          errors.addObject().put("id", key.asText()).put("status", 404);
        } else {
          found.add(issue);
        }
      }
      return json(answer);
    }
    if (r.method.equals("POST") && p.equals("/issue/bulk")) {
      final ObjectNode answer = MAPPER.createObjectNode();
      final ArrayNode created = answer.putArray("issues");
      answer.putArray("errors");
      for (JsonNode update : r.body.path("issueUpdates")) {
        final ObjectNode fields = ((ObjectNode) update.path("fields")).deepCopy();
        final String project = fields.path("project").path("key").asText();
        final String type = fields.path("issuetype").path("id").asText();
        fields.remove("project");
        fields.remove("issuetype");
        final ObjectNode issue = add(project, type, fields);
        created.addObject().put("id", issue.path("id").asText())
            .put("key", issue.path("key").asText());
      }
      return new Reply(201, answer.toString());
    }
    final Matcher issue = Pattern.compile("/issue/([A-Z]+-\\d+)(/transitions)?").matcher(p);
    if (issue.matches()) {
      final ObjectNode found = issues.get(issue.group(1));
      if (found == null) {
        return new Reply(404, "{\"errorMessages\":[\"Issue does not exist\"]}");
      }
      final ObjectNode fields = (ObjectNode) found.path("fields");
      if (issue.group(2) != null) {
        final String status = fields.path("status").path("name").asText();
        if (r.method.equals("GET")) {
          final ObjectNode answer = MAPPER.createObjectNode();
          final ArrayNode transitions = answer.putArray("transitions");
          workflow.get(status).forEach((id, to) -> {
            final ObjectNode t = transitions.addObject().put("id", id).put("name", "Go " + to);
            t.putObject("to").put("name", to);
          });
          return json(answer);
        }
        final String to = workflow.get(status).get(r.body.path("transition").path("id").asText());
        fields.putObject("status").put("name", to);
        return new Reply(204, "");
      }
      switch (r.method) {
      case "GET":
        return json(found);
      case "PUT":
        r.body.path("fields").fields().forEachRemaining(e -> fields.set(e.getKey(), e.getValue()));
        return new Reply(204, "");
      case "DELETE":
        issues.remove(issue.group(1));
        return new Reply(204, "");
      default:
        break;
      }
    }
    if (r.method.equals("POST") && p.equals("/issueLink")) {
      link(r.body.path("type").path("name").asText(),
          r.body.path("outwardIssue").path("key").asText(),
          r.body.path("inwardIssue").path("key").asText());
      return new Reply(201, "");
    }
    if (r.method.equals("DELETE") && p.startsWith("/issueLink/")) {
      return new Reply(204, "");
    }
    return new Reply(404, "{\"errorMessages\":[\"No fake for " + r.method + " " + p + "\"]}");
  }

  private Reply search(JsonNode body) {
    final String jql = body.path("jql").asText();
    final List<String[]> forms = new ArrayList<>();
    final Matcher m = FORM_IN_JQL.matcher(jql);
    while (m.find()) {
      forms.add(new String[] {m.group(1), m.group(2)});
    }
    final List<ObjectNode> matching = new ArrayList<>();
    for (ObjectNode issue : issues.values()) {
      for (String[] form : forms) {
        if (issue.path("fields").path("project").path("key").asText().equals(form[0])
            && issue.path("fields").path("issuetype").path("id").asText().equals(form[1])) {
          matching.add(issue);
        }
      }
    }
    final int start = body.has("nextPageToken")
        ? Integer.parseInt(body.path("nextPageToken").asText()) : 0;
    final int size = body.path("maxResults").asInt(50);
    final ObjectNode answer = MAPPER.createObjectNode();
    final ArrayNode page = answer.putArray("issues");
    for (int i = start; i < Math.min(matching.size(), start + size); i++) {
      page.add(matching.get(i));
    }
    if (start + size < matching.size()) {
      answer.put("nextPageToken", Integer.toString(start + size));
    }
    answer.put("isLast", start + size >= matching.size());
    return json(answer);
  }

  private static String field(String id, String name, String schema, boolean required) {
    return "{\"fieldId\":\"" + id + "\",\"key\":\"" + id + "\",\"name\":\"" + name
        + "\",\"required\":" + required + ",\"schema\":" + schema + "}";
  }

  static Reply json(Object body) {
    return new Reply(200, body.toString());
  }
}
