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

import org.apache.calcite.adapter.servicenow.PushdownHarness.Case;
import org.apache.calcite.adapter.servicenow.PushdownHarness.Mode;
import org.apache.calcite.adapter.servicenow.PushdownHarness.Outcome;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The differential pushdown harness against a live ServiceNow instance: the only thing that can
 * verify a pushdown entry.
 *
 * <p>NEVER RUN so far: no instance was available when it was written. Skipped unless
 * {@code govdata/.env.prod} holds SN_INSTANCE_URL, SN_USERNAME and SN_PASSWORD; run with
 * {@code ./gradlew :servicenow:test -PincludeTags=integration --tests '*PushdownHarnessLive*'}.
 * Use a developer instance, never a production one: it WRITES.
 *
 * <p>What it does, in order:
 * <ol>
 *   <li>Seeds {@code incident} rows whose short_description starts with {@code ZZ_HARNESS_<run>_}
 *   (mixed case text, empty values, values with {@code = , ' % _ ^} and spaces, several impact,
 *   urgency, knowledge, caller and opened_at values). Writes are direct REST calls made here; the
 *   adapter stays read-only. Business rules may change what is stored; the cases are built from
 *   what is read back, so that does not matter.
 *   <li>Builds the cases from the data in the table (the whole table takes part, not just the seed
 *   rows) and runs each twice: unpushed (Calcite evaluates) and pushed (ServiceNow evaluates).
 *   <li>Deletes every row it created, in a finally block. If the run is killed, rows whose
 *   short_description starts with ZZ_HARNESS_ are the harness's and can be deleted by hand.
 *   <li>Writes {@code build/servicenow-probe/pushdown-verification.json} (the record to copy to
 *   {@code servicenow/src/main/resources/servicenow/pushdown-verification.json} to enable the
 *   entries it verified), {@code pushdown-harness.md} (every case) and
 *   {@code pushdown-info-probes.md} (observations that verify nothing: fail-open behaviour,
 *   escaping, dot-walking, display values, precedence, BETWEEN, NQ).
 * </ol>
 */
@Tag("integration")
class PushdownHarnessLiveTest {

  private static final String TABLE = "incident";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir Path catalogCache;

  /** Text values that exercise case, empty, punctuation and wildcard characters. */
  private static final String[] DESCRIPTIONS = {
      "Alpha", "ALPHA", "alpha", "alphabet", "Beta beta", "", "a=b", "p,q", "it's", "100%_done",
      "x_y", "a^b", "a^ORb", "  padded  ", "Gamma"
  };

  @Test void differentialHarness() throws Exception {
    final ServiceNowTestCredentials credentials = ServiceNowTestCredentials.load();
    assumeTrue(credentials != null,
        "no ServiceNow instance configured: SN_INSTANCE_URL, SN_USERNAME, SN_PASSWORD");
    final Path out = Paths.get("build", "servicenow-probe");
    Files.createDirectories(out);

    final Map<String, Object> extra = new HashMap<>();
    extra.put("instanceUrl", credentials.instanceUrl);
    extra.put("username", credentials.username);
    extra.put("password", credentials.password);
    extra.put("catalogCacheDirectory", catalogCache.toString());
    extra.put("tables", TABLE + ",sys_user");
    extra.put("pageSize", 200);
    final PushdownHarness.Source source = (trusted, observer) ->
        PushdownTestSupport.open(credentials.instanceUrl, extra, trusted, observer);

    final ServiceNowConnection connection = new ServiceNowConnection(
        URI.create(credentials.instanceUrl),
        ServiceNowAuth.basic(credentials.username, credentials.password), 1, 3,
        Duration.ofSeconds(60), Duration.ofSeconds(75));
    final RestWriter writer = new RestWriter(credentials);
    final List<String> created = new ArrayList<>();
    try {
      seed(connection, writer, created);
      final PushdownHarness harness = new PushdownHarness(Mode.LIVE, source, TABLE);
      final List<Case> cases;
      try (Connection conn = source.open(Collections.<String>emptySet(),
          new PushdownTestSupport.Recorder())) {
        cases = PushdownCases.build(PushdownCases.profile(conn, TABLE));
      }
      final List<Outcome> outcomes = harness.runAll(cases);
      write(out.resolve("pushdown-harness.md"), harness.report(outcomes));
      harness.writeRecord(out.resolve("pushdown-verification.json"), credentials.instanceUrl,
          outcomes);
      write(out.resolve("pushdown-info-probes.md"), infoProbes(connection, created));
    } finally {
      for (String sysId : created) {
        writer.delete(sysId);
      }
    }
  }

  // ---- seeding (direct REST writes; the adapter never writes) -----------------------------

  private static void seed(ServiceNowConnection connection, RestWriter writer,
      List<String> created) throws IOException, InterruptedException {
    final String run = Long.toString(System.currentTimeMillis() % 1_000_000);
    final List<String> users = new ArrayList<>();
    final Map<String, String> params = new LinkedHashMap<>();
    params.put("sysparm_fields", "sys_id,name");
    params.put("sysparm_limit", "2");
    for (JsonNode row : connection.getTable("sys_user", params).body.path("result")) {
      users.add(Rows.stored(row, "sys_id", "sys_user", false, false));
    }
    final String[] opened = {"2026-03-01 00:00:00", "2026-03-01 23:59:59", "2026-03-02 00:00:00",
        "2026-03-02 12:00:00"};
    for (int i = 0; i < DESCRIPTIONS.length; i++) {
      final ObjectNode body = MAPPER.createObjectNode();
      body.put("short_description", "ZZ_HARNESS_" + run + "_" + i);
      if (!DESCRIPTIONS[i].isEmpty()) {
        body.put("description", DESCRIPTIONS[i]);
      }
      body.put("impact", Integer.toString(1 + i % 3));
      body.put("urgency", Integer.toString(1 + (i / 3) % 3));
      body.put("knowledge", i % 4 == 0);
      body.put("opened_at", opened[i % opened.length]);
      if (i % 2 == 0 && !users.isEmpty()) {
        body.put("caller_id", users.get(i % users.size()));
      }
      created.add(writer.insert(TABLE, body));
    }
  }

  /** Minimal REST writer for the harness; the adapter has no write path. */
  private static final class RestWriter {
    private final HttpClient http = HttpClient.newHttpClient();
    private final ServiceNowTestCredentials credentials;

    RestWriter(ServiceNowTestCredentials credentials) {
      this.credentials = credentials;
    }

    private HttpRequest.Builder request(String path) {
      return HttpRequest.newBuilder(URI.create(credentials.instanceUrl + path))
          .header("Accept", "application/json")
          .header("Content-Type", "application/json")
          .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
              (credentials.username + ":" + credentials.password)
                  .getBytes(StandardCharsets.UTF_8)));
    }

    String insert(String table, ObjectNode body) throws IOException, InterruptedException {
      final HttpResponse<String> response = http.send(
          request("/api/now/table/" + table + "?sysparm_fields=sys_id")
              .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(),
          HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 201) {
        throw new IllegalStateException("Seeding " + table + " failed with HTTP "
            + response.statusCode() + ": " + response.body());
      }
      return MAPPER.readTree(response.body()).path("result").path("sys_id").asText();
    }

    void delete(String sysId) throws IOException, InterruptedException {
      final HttpResponse<String> response = http.send(
          request("/api/now/table/" + TABLE + "/" + sysId).DELETE().build(),
          HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 204) {
        throw new IllegalStateException("Deleting harness row " + sysId + " failed with HTTP "
            + response.statusCode() + ": " + response.body());
      }
    }
  }

  // ---- observations that verify nothing ----------------------------------------------------

  private static String infoProbes(ServiceNowConnection connection, List<String> created) {
    final StringBuilder report = new StringBuilder("# Pushdown information probes\n\n"
        + "Raw encoded queries, one request each (no keyset paging), compared with what the data "
        + "says. Nothing here verifies an entry; it records how the instance behaves.\n\n"
        + "| probe | query | rows | note |\n|---|---|---|---|\n");
    final Set<String> all = ids(connection, "");
    row(report, "total rows", "", all.size(), "");

    // Fail-open: an invalid field name or operator is dropped
    final Set<String> noField = ids(connection, "zz_no_such_field_probe=1");
    row(report, "invalid field", "zz_no_such_field_probe=1", noField.size(),
        noField.size() == all.size() ? "FAIL-OPEN: the term was ignored; all rows returned"
            : noField.isEmpty() ? "no rows (glide.invalid_query.returns_no_rows is probably true)"
            : "something else");
    final Set<String> active = ids(connection, "active=true");
    final Set<String> mixed = ids(connection, "zz_no_such_field_probe=1^active=true");
    row(report, "invalid field AND valid term", "zz_no_such_field_probe=1^active=true",
        mixed.size(), mixed.equals(active) ? "invalid term dropped, valid term kept (fail-open)"
            : mixed.isEmpty() ? "no rows" : "something else");
    row(report, "invalid operator", "short_descriptionBOGUSOPERATORx",
        ids(connection, "short_descriptionBOGUSOPERATORx").size(), "");
    row(report, "invalid value for the type", "impact=notanumber",
        ids(connection, "impact=notanumber").size(), "");
    row(report, "empty IN list", "short_descriptionIN", ids(connection, "short_descriptionIN").size(),
        "the translator never sends this");

    // Escaping the query separator
    row(report, "caret escaped by doubling", "description=a^^b",
        ids(connection, "description=a^^b").size(), "seed row 'a^b' exists once");
    row(report, "caret unescaped (what a bug would send)", "description=a^b",
        ids(connection, "description=a^b").size(), "read as description=a AND field b?");

    // Dot-walking and display values
    final Set<String> hasCaller = ids(connection, "caller_idISNOTEMPTY");
    final Set<String> dotWalk = ids(connection, "caller_id.nameISNOTEMPTY");
    row(report, "dot-walk existence", "caller_id.nameISNOTEMPTY", dotWalk.size(),
        "caller_idISNOTEMPTY matches " + hasCaller.size() + "; the translator never dot-walks");

    // BETWEEN
    final Set<String> between = ids(connection, "impactBETWEEN1@2");
    final Set<String> range = ids(connection, "impact>=1^impact<=2");
    row(report, "BETWEEN encoding", "impactBETWEEN1@2", between.size(),
        between.equals(range) ? "same rows as >=1^<=2" : "DIFFERENT from >=1^<=2 ("
            + range.size() + ")");

    // Precedence and NQ
    final Set<String> a = ids(connection, "impact=1");
    final Set<String> b = ids(connection, "urgency=2");
    final Set<String> c = ids(connection, "knowledge=true");
    final Set<String> r1 = new TreeSet<>(b);
    r1.addAll(c);
    r1.retainAll(a);
    final Set<String> r2 = new TreeSet<>(a);
    r2.retainAll(b);
    r2.addAll(c);
    final Set<String> triple = ids(connection, "impact=1^urgency=2^ORknowledge=true");
    row(report, "a^b^ORc", "impact=1^urgency=2^ORknowledge=true", triple.size(),
        triple.equals(r1) ? "a AND (b OR c)" : triple.equals(r2) ? "(a AND b) OR c"
            : "neither reading (R1 " + r1.size() + ", R2 " + r2.size() + ")");
    final Set<String> union = new TreeSet<>(a);
    union.addAll(b);
    final Set<String> nq = ids(connection, "impact=1^NQurgency=2");
    row(report, "^NQ", "impact=1^NQurgency=2", nq.size(),
        nq.equals(union) ? "union of the two queries" : "not the union (" + union.size() + ")");
    final Set<String> orThenAnd = ids(connection, "impact=1^ORurgency=2^knowledge=true");
    final Set<String> expected = new TreeSet<>(a);
    expected.addAll(b);
    expected.retainAll(c);
    row(report, "(a OR b) AND c", "impact=1^ORurgency=2^knowledge=true", orThenAnd.size(),
        orThenAnd.equals(expected) ? "(a OR b) AND c" : "different from (a OR b) AND c ("
            + expected.size() + ")");
    row(report, "seed rows created", "", created.size(), "deleted after the run");
    return report.toString();
  }

  private static void row(StringBuilder report, String probe, String query, int rows,
      String note) {
    report.append("| ").append(probe).append(" | `").append(query.replace("|", "\\|"))
        .append("` | ").append(rows).append(" | ").append(note.replace("|", "\\|")).append(" |\n");
  }

  /** The sys_ids a raw encoded query returns, in one request. */
  private static Set<String> ids(ServiceNowConnection connection, String query) {
    final Map<String, String> params = new LinkedHashMap<>();
    if (!query.isEmpty()) {
      params.put("sysparm_query", query);
    }
    params.put("sysparm_fields", "sys_id");
    params.put("sysparm_limit", "10000");
    params.put("sysparm_no_count", "true");
    final Set<String> ids = new TreeSet<>();
    for (JsonNode row : connection.getTable(TABLE, params).body.path("result")) {
      ids.add(Rows.stored(row, "sys_id", TABLE, false, false));
    }
    return ids;
  }

  private static void write(Path file, String content) throws IOException {
    Files.write(file, content.getBytes(StandardCharsets.UTF_8));
  }
}
