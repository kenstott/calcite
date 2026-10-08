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

import org.apache.calcite.adapter.servicenow.ServiceNowCatalog.TableDef;
import org.apache.calcite.adapter.servicenow.ServiceNowColumn.Kind;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Probe spike: questions only a live instance can answer, written as code so that it can be run
 * the day credentials arrive. Each probe writes a Markdown report under
 * {@code servicenow/build/servicenow-probe/} and asserts nothing about the answers: the point is
 * to learn them. Read the reports, then (1) fill {@link PushdownCapabilities} with the operators
 * and types that came out EXACT, and write their encoders; (2) decide the schema-per-scope layout.
 *
 * <p>NEVER RUN so far: no instance was available. Skipped unless credentials are configured (see
 * {@link ServiceNowTestCredentials}); runs with
 * {@code ./gradlew :servicenow:test -PincludeTags=integration --tests '*ServiceNowProbe*'}.
 *
 * <p>The probes only read. They send queries that are deliberately invalid; that is the point of
 * {@link #failOpen}.
 */
@Tag("integration")
class ServiceNowProbe {

  /** Table whose rows are the probe data. Must have no more than {@link #MAX_ROWS} rows. */
  private static final String PROBE_TABLE = "incident";
  private static final int MAX_ROWS = 5000;

  private static ServiceNowConnection connection;
  private static Path reportDirectory;

  @BeforeAll static void connect() throws IOException {
    final ServiceNowTestCredentials credentials = ServiceNowTestCredentials.load();
    assumeTrue(credentials != null,
        "no ServiceNow instance configured: SN_INSTANCE_URL, SN_USERNAME, SN_PASSWORD");
    connection = new ServiceNowConnection(URI.create(credentials.instanceUrl),
        ServiceNowAuth.basic(credentials.username, credentials.password), 1, 3,
        Duration.ofSeconds(60), Duration.ofSeconds(75));
    reportDirectory = Paths.get("build", "servicenow-probe");
    Files.createDirectories(reportDirectory);
  }

  // ---- (b) tables per application scope ---------------------------------------------------

  /**
   * Counts tables per application scope, which decides whether to give each scope its own SQL
   * schema. Reads {@code sys_db_object} with display values so the scope's name comes with its
   * sys_id.
   */
  @Test void tablesPerApplicationScope() throws IOException {
    final Map<String, Integer> perScope = new TreeMap<>();
    final TableReader reader = new TableReader(connection, "sys_db_object",
        Arrays.asList("sys_id", "name", "sys_scope"), true, 1000, "");
    int total = 0;
    while (reader.hasNext()) {
      final JsonNode row = reader.next();
      final String scopeId = Rows.stored(row, "sys_scope", "sys_db_object", true, true);
      final String scopeName = Rows.displayText(row, "sys_scope", "sys_db_object");
      perScope.merge(scopeName + " (" + scopeId + ")", 1, Integer::sum);
      total++;
    }
    final List<Map.Entry<String, Integer>> sorted = new ArrayList<>(perScope.entrySet());
    sorted.sort((a, b) -> b.getValue() - a.getValue());
    final StringBuilder report = new StringBuilder("# Tables per application scope\n\n")
        .append(total).append(" tables in sys_db_object, ").append(perScope.size())
        .append(" scopes.\n\n| scope (sys_id) | tables |\n|---|---|\n");
    for (Map.Entry<String, Integer> e : sorted) {
      report.append("| ").append(e.getKey()).append(" | ").append(e.getValue()).append(" |\n");
    }
    write("tables-per-scope.md", report.toString());
  }

  // ---- (a) predicate semantics -------------------------------------------------------------

  private enum Op {
    EQ("=") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && compare(v, a) == 0;
      }
    },
    NE("!=") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && compare(v, a) != 0;
      }
    },
    LT("<") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && compare(v, a) < 0;
      }
    },
    LE("<=") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && compare(v, a) <= 0;
      }
    },
    GT(">") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && compare(v, a) > 0;
      }
    },
    GE(">=") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && compare(v, a) >= 0;
      }
    },
    IN("IN") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && (compare(v, a) == 0 || compare(v, b) == 0);
      }
      @Override String term(String field, String a, String b) {
        return field + "IN" + a + "," + b;
      }
    },
    NOT_IN("NOT IN") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && compare(v, a) != 0 && compare(v, b) != 0;
      }
      @Override String term(String field, String a, String b) {
        return field + "NOT IN" + a + "," + b;
      }
    },
    BETWEEN("BETWEEN") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && compare(v, a) >= 0 && compare(v, b) <= 0;
      }
      @Override String term(String field, String a, String b) {
        return field + "BETWEEN" + a + "@" + b;
      }
    },
    ISEMPTY("IS NULL") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v == null;
      }
      @Override String term(String field, String a, String b) {
        return field + "ISEMPTY";
      }
    },
    ISNOTEMPTY("IS NOT NULL") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null;
      }
      @Override String term(String field, String a, String b) {
        return field + "ISNOTEMPTY";
      }
    },
    STARTSWITH("LIKE 'x%'") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && v.toString().startsWith(a.toString());
      }
      @Override String term(String field, String a, String b) {
        return field + "STARTSWITH" + a;
      }
    },
    ENDSWITH("LIKE '%x'") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && v.toString().endsWith(a.toString());
      }
      @Override String term(String field, String a, String b) {
        return field + "ENDSWITH" + a;
      }
    },
    CONTAINS("LIKE '%x%'") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && v.toString().contains(a.toString());
      }
      @Override String term(String field, String a, String b) {
        return field + "LIKE" + a;
      }
    },
    NOT_CONTAINS("NOT LIKE '%x%'") {
      @Override boolean sql(Object v, Object a, Object b) {
        return v != null && !v.toString().contains(a.toString());
      }
      @Override String term(String field, String a, String b) {
        return field + "NOT LIKE" + a;
      }
    };

    final String sqlForm;

    Op(String sqlForm) {
      this.sqlForm = sqlForm;
    }

    /** SQL semantics: true if a row with value {@code v} (null if empty) is selected. */
    abstract boolean sql(Object v, Object a, Object b);

    /** The encoded-query term under test. */
    String term(String field, String a, String b) {
      return field + sqlForm + a;
    }

    boolean applies(Kind kind) {
      switch (this) {
      case LT:
      case LE:
      case GT:
      case GE:
      case BETWEEN:
        return kind != Kind.BOOLEAN && kind != Kind.REFERENCE && kind != Kind.GUID;
      case STARTSWITH:
      case ENDSWITH:
      case CONTAINS:
      case NOT_CONTAINS:
        return kind == Kind.TEXT;
      default:
        return true;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static int compare(Object a, Object b) {
    return ((Comparable<Object>) a).compareTo(b);
  }

  /** One row of the probe table: sys_id to the typed value of each probed column. */
  private static final class Sample {
    final Map<String, Map<String, Object>> rows = new LinkedHashMap<>();
    final Map<String, ServiceNowColumn> columns = new LinkedHashMap<>();
  }

  /**
   * For every operator and every column type present in the probe table, asks the instance for the
   * rows matching a term and compares with what SQL would select over the same rows. Each result
   * is EXACT (same rows), SUPERSET (extra rows: Calcite must re-check), SUBSET (rows missing:
   * pushing is wrong), or DIFFERENT. Then the same for AND, OR and mixed combinations, and for
   * deliberately invalid terms.
   */
  @Test void predicateSemantics() throws IOException {
    final TableDef table = loadTable();
    final Sample sample = sample(table);
    final StringBuilder report = new StringBuilder("# Predicate semantics on ")
        .append(PROBE_TABLE).append(" (").append(sample.rows.size()).append(" rows)\n\n")
        .append("Columns probed, one per field type: ");
    sample.columns.values().forEach(c -> report.append(c.name).append(" (").append(c.kind)
        .append("), "));
    report.append("\n\nSQL means: NULL never matches a comparison, strings compare case "
        + "sensitively. EXACT = same rows as SQL; SUPERSET = extra rows (Calcite would have to "
        + "re-check); SUBSET = rows missing; DIFFERENT = both.\n\n"
        + "| column | type | operator | term | server rows | SQL rows | verdict |\n"
        + "|---|---|---|---|---|---|---|\n");

    for (ServiceNowColumn column : sample.columns.values()) {
      final List<Object> distinct = distinctValues(sample, column);
      if (distinct.size() < 2) {
        report.append("| ").append(column.name).append(" | ").append(column.kind)
            .append(" | all | | | | skipped: fewer than two distinct values |\n");
        continue;
      }
      final Object a = distinct.get(distinct.size() / 2);
      final Object b = distinct.get(distinct.size() / 2 - 1);
      final String lowText = text(sample, column, a);
      final String otherText = text(sample, column, b);
      for (Op op : Op.values()) {
        if (!op.applies(column.kind)) {
          continue;
        }
        // For text, derive substrings from a real value; for BETWEEN order the bounds
        String left = lowText;
        String right = otherText;
        Object leftValue = a;
        Object rightValue = b;
        if (op == Op.BETWEEN) {
          // b sorts below a
          left = otherText;
          right = lowText;
          leftValue = b;
          rightValue = a;
        }
        if (column.kind == Kind.TEXT && (op == Op.STARTSWITH || op == Op.ENDSWITH
            || op == Op.CONTAINS || op == Op.NOT_CONTAINS)) {
          final String full = lowText.isEmpty() ? "a" : lowText;
          final String piece = op == Op.STARTSWITH ? full.substring(0, Math.min(3, full.length()))
              : op == Op.ENDSWITH ? full.substring(Math.max(0, full.length() - 3))
              : full.substring(full.length() / 3, Math.min(full.length(), full.length() / 3 + 3));
          left = piece;
          leftValue = piece;
        }
        final String term = op.term(column.field, left, right);
        verdict(report, sample, column, op, term, leftValue, rightValue);
      }
      // Case sensitivity of text equality: the same value with its case swapped
      if (column.kind == Kind.TEXT) {
        final String swapped = swapCase(lowText);
        if (!swapped.equals(lowText)) {
          verdict(report, sample, column, Op.EQ, column.field + "=" + swapped, swapped, null);
        }
      }
    }

    combinations(report, sample);
    failOpen(report, sample);
    write("predicate-semantics.md", report.toString());
  }

  private void verdict(StringBuilder report, Sample sample, ServiceNowColumn column, Op op,
      String term, Object a, Object b) throws IOException {
    final Set<String> expected = new TreeSet<>();
    for (Map.Entry<String, Map<String, Object>> row : sample.rows.entrySet()) {
      if (op.sql(row.getValue().get(column.field), a, b)) {
        expected.add(row.getKey());
      }
    }
    final String outcome;
    String serverCount = "";
    try {
      final Set<String> server = ids(term, sample);
      serverCount = Integer.toString(server.size());
      outcome = classify(expected, server);
    } catch (ServiceNowException e) {
      row(report, column, op, term, "error", Integer.toString(expected.size()),
          "ERROR HTTP " + e.getStatus() + ": " + e.getMessage());
      return;
    }
    row(report, column, op, term, serverCount, Integer.toString(expected.size()), outcome);
  }

  private static void row(StringBuilder report, ServiceNowColumn column, Op op, String term,
      String serverCount, String sqlCount, String outcome) {
    report.append("| ").append(column.name).append(" | ").append(column.kind).append(" | ")
        .append(op.sqlForm).append(" | `").append(term.replace("|", "\\|")).append("` | ")
        .append(serverCount).append(" | ").append(sqlCount).append(" | ")
        .append(outcome.replace("|", "\\|")).append(" |\n");
  }

  private static String classify(Set<String> expected, Set<String> server) {
    final Set<String> extra = new TreeSet<>(server);
    extra.removeAll(expected);
    final Set<String> missing = new TreeSet<>(expected);
    missing.removeAll(server);
    if (extra.isEmpty() && missing.isEmpty()) {
      return "EXACT";
    }
    if (missing.isEmpty()) {
      return "SUPERSET (+" + extra.size() + ")";
    }
    if (extra.isEmpty()) {
      return "SUBSET (-" + missing.size() + ")";
    }
    return "DIFFERENT (+" + extra.size() + " -" + missing.size() + ")";
  }

  /**
   * Which of "AND binds tighter than ^OR" or "^OR attaches to the term before it" is true, and
   * what ^NQ does. Picks three equality terms whose two readings of {@code a^b^ORc} differ on
   * this data.
   */
  private void combinations(StringBuilder report, Sample sample) throws IOException {
    report.append("\n## Combining terms\n\nTerms are equalities picked from this table's data. "
        + "Readings of `a^b^ORc`: R1 = a AND (b OR c); R2 = (a AND b) OR c.\n\n"
        + "| query | server rows | matches |\n|---|---|---|\n");
    final List<String[]> terms = new ArrayList<>();      // field, text
    final List<Set<String>> sets = new ArrayList<>();
    for (ServiceNowColumn column : sample.columns.values()) {
      if (column.kind == Kind.BOOLEAN || column.kind == Kind.INTEGER
          || column.kind == Kind.REFERENCE || column.kind == Kind.TEXT) {
        for (Object value : distinctValues(sample, column)) {
          final Set<String> ids = new TreeSet<>();
          for (Map.Entry<String, Map<String, Object>> row : sample.rows.entrySet()) {
            if (value.equals(row.getValue().get(column.field))) {
              ids.add(row.getKey());
            }
          }
          if (!ids.isEmpty() && ids.size() < sample.rows.size()) {
            terms.add(new String[] {column.field, text(sample, column, value)});
            sets.add(ids);
          }
          if (terms.size() >= 40) {
            break;
          }
        }
      }
    }
    for (int i = 0; i < terms.size(); i++) {
      for (int j = 0; j < terms.size(); j++) {
        for (int k = 0; k < terms.size(); k++) {
          if (i == j || j == k || i == k) {
            continue;
          }
          final Set<String> r1 = new TreeSet<>(sets.get(j));
          r1.addAll(sets.get(k));
          r1.retainAll(sets.get(i));
          final Set<String> r2 = new TreeSet<>(sets.get(i));
          r2.retainAll(sets.get(j));
          r2.addAll(sets.get(k));
          if (r1.equals(r2) || r1.isEmpty() || r2.isEmpty()) {
            continue;
          }
          final String a = eq(terms.get(i));
          final String b = eq(terms.get(j));
          final String c = eq(terms.get(k));
          final Set<String> andAll = new TreeSet<>(sets.get(i));
          andAll.retainAll(sets.get(j));
          final Set<String> orAll = new TreeSet<>(sets.get(i));
          orAll.addAll(sets.get(j));
          final Set<String> union = new TreeSet<>(sets.get(i));
          union.addAll(sets.get(j));
          combo(report, sample, a + "^" + b, "AND", andAll);
          combo(report, sample, a + "^OR" + b, "OR", orAll);
          combo(report, sample, a + "^NQ" + b, "NQ (union)", union);
          final String triple = a + "^" + b + "^OR" + c;
          final Set<String> server = ids(triple, sample);
          report.append("| `").append(triple).append("` | ").append(server.size()).append(" | ")
              .append(server.equals(r1) ? "R1: a AND (b OR c)"
                  : server.equals(r2) ? "R2: (a AND b) OR c" : "NEITHER").append(" |\n");
          return;
        }
      }
    }
    report.append("| (no triple of terms on this data distinguishes the two readings; "
        + "use a table with more varied rows) | | |\n");
  }

  private void combo(StringBuilder report, Sample sample, String query, String label,
      Set<String> expected) throws IOException {
    final Set<String> server = ids(query, sample);
    report.append("| `").append(query).append("` | ").append(server.size()).append(" | ")
        .append(label).append(": ").append(classify(expected, server)).append(" |\n");
  }

  private static String eq(String[] term) {
    return term[0] + "=" + term[1];
  }

  /**
   * The fail-open check: an invalid term should be dropped by the instance, not rejected, so the
   * rows come back unfiltered. This is why filters are not pushed in this release.
   */
  private void failOpen(StringBuilder report, Sample sample) throws IOException {
    report.append("\n## Invalid terms (fail-open check)\n\n"
        + "Total rows in the sample: ").append(sample.rows.size()).append(".\n\n"
        + "| query | outcome |\n|---|---|\n");
    final ServiceNowColumn any = sample.columns.values().iterator().next();
    final String[] queries = {
        "zz_no_such_field_probe=1",
        any.field + "BOGUSOPERATORx",
        "priority=notanumber",
        "zz_no_such_field_probe=1^" + any.field + "ISNOTEMPTY",
    };
    for (String query : queries) {
      String outcome;
      try {
        final Set<String> server = ids(query, sample);
        outcome = server.size() == sample.rows.size()
            ? "FAIL-OPEN: returned all " + server.size() + " rows"
            : "returned " + server.size() + " of " + sample.rows.size() + " rows";
      } catch (ServiceNowException e) {
        outcome = "REJECTED: HTTP " + e.getStatus() + " " + e.getMessage();
      }
      report.append("| `").append(query).append("` | ").append(outcome.replace("|", "\\|"))
          .append(" |\n");
    }
  }

  // ---- helpers ---------------------------------------------------------------------------

  private TableDef loadTable() {
    final ServiceNowCatalog catalog = new ServiceNowCatalog(
        ServiceNowCatalog.load(connection, 1000), Collections.<String>emptySet());
    return catalog.table(PROBE_TABLE);
  }

  /** Reads the probe table in full, with one column of each field type that has data. */
  private Sample sample(TableDef table) {
    final Sample sample = new Sample();
    final Set<Kind> kinds = new HashSet<>();
    final List<String> fields = new ArrayList<>(Collections.singletonList("sys_id"));
    for (ServiceNowColumn column : table.columns) {
      if (!column.display && column.kind != Kind.GUID && kinds.add(column.kind)) {
        sample.columns.put(column.name, column);
        fields.add(column.field);
      }
    }
    final TableReader reader =
        new TableReader(connection, PROBE_TABLE, fields, false, 1000, "");
    while (reader.hasNext()) {
      final JsonNode row = reader.next();
      final String sysId = Rows.stored(row, "sys_id", PROBE_TABLE, false, false);
      final Map<String, Object> values = new LinkedHashMap<>();
      for (ServiceNowColumn column : sample.columns.values()) {
        values.put(column.field, ValueConverter.convert(column,
            Rows.stored(row, column.field, PROBE_TABLE, false, column.kind == Kind.REFERENCE),
            PROBE_TABLE, sysId));
      }
      sample.rows.put(sysId, values);
      if (sample.rows.size() > MAX_ROWS) {
        throw new IllegalStateException(PROBE_TABLE + " has more than " + MAX_ROWS + " rows; the "
            + "probe compares whole result sets and needs a table that fits in one response");
      }
    }
    return sample;
  }

  private static List<Object> distinctValues(Sample sample, ServiceNowColumn column) {
    final Set<Object> values = new HashSet<>();
    for (Map<String, Object> row : sample.rows.values()) {
      final Object value = row.get(column.field);
      if (value != null) {
        values.add(value);
      }
    }
    final List<Object> sorted = new ArrayList<>(values);
    sorted.sort((a, b) -> compare(a, b));
    return sorted;
  }

  /** The wire text of a typed value, as it would appear in a query term. */
  private static String text(Sample sample, ServiceNowColumn column, Object value) {
    switch (column.kind) {
    case TIMESTAMP:
      return java.time.Instant.ofEpochMilli((Long) value).atZone(java.time.ZoneOffset.UTC)
          .toLocalDateTime().format(
              java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    case DATE:
      return java.time.LocalDate.ofEpochDay((Integer) value).toString();
    case DECIMAL:
    case CURRENCY:
      return ((BigDecimal) value).toPlainString();
    default:
      return value.toString();
    }
  }

  private static String swapCase(String text) {
    final StringBuilder out = new StringBuilder();
    for (char c : text.toCharArray()) {
      out.append(Character.isUpperCase(c) ? Character.toLowerCase(c)
          : Character.isLowerCase(c) ? Character.toUpperCase(c) : c);
    }
    return out.toString();
  }

  /**
   * The sys_ids of rows the instance returns for an encoded query, in one request. No keyset
   * paging: a paging term appended to an OR or NQ query would change its meaning, which is one of
   * the things being probed. Only ids that are in the sample are returned, so rows created since
   * the sample was read do not count.
   */
  private Set<String> ids(String query, Sample sample) {
    final Map<String, String> params = new LinkedHashMap<>();
    params.put("sysparm_query", query);
    params.put("sysparm_fields", "sys_id");
    params.put("sysparm_limit", Integer.toString(MAX_ROWS + 1));
    params.put("sysparm_no_count", "true");
    final JsonNode result = connection.getTable(PROBE_TABLE, params).body.path("result");
    if (!result.isArray()) {
      throw new ServiceNowException("No result array for query " + query);
    }
    final Set<String> ids = new TreeSet<>();
    for (JsonNode row : result) {
      final String id = Rows.stored(row, "sys_id", PROBE_TABLE, false, false);
      if (sample.rows.containsKey(id)) {
        ids.add(id);
      }
    }
    return ids;
  }

  private static void write(String name, String content) throws IOException {
    Files.write(reportDirectory.resolve(name), content.getBytes(StandardCharsets.UTF_8));
  }
}
