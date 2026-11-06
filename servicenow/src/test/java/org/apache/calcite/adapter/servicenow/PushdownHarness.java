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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The differential test harness for predicate pushdown.
 *
 * <p>For each {@link Case} it runs the same SQL twice over the same table: once with no pushdown
 * entry trusted, so that ServiceNow returns every row and Calcite evaluates the predicate with SQL
 * semantics (an empty value is NULL; three-valued logic), and once with the entries under test
 * trusted, so that the translator sends the predicate to ServiceNow as an encoded query. The sets
 * of {@code sys_id}s are compared. Equal sets are a MATCH; any difference is a MISMATCH and the
 * rows that differ are recorded.
 *
 * <p>An entry is verified only if every case that lists it as a subject MATCHed, at least one did,
 * and the entries its cases depend on (prerequisites) are verified too. A mismatch leaves it
 * unverified, so it stays unpushed. Verification is only possible in {@link Mode#LIVE}: in
 * {@link Mode#OFFLINE} the "server" is a local stub that is this project's own reading of the
 * documentation, so the same code runs (and checks that the translator pushes or declines each
 * case) but {@link #verdicts} marks nothing verified and {@link #writeRecord} refuses.
 */
final class PushdownHarness {

  enum Mode { LIVE, OFFLINE }

  /** Opens a connection to the table under test with the given entries trusted. */
  interface Source {
    Connection open(Collection<String> trusted, PushdownObserver observer) throws SQLException;
  }

  /** One predicate to test. */
  static final class Case {
    final String id;
    final String where;
    /** Entries trusted for the pushed run. */
    final Set<String> trust;
    /** Entries this case is evidence for. */
    final Set<String> subjects;
    /** Entries the case relies on being right; the subjects are verified only if these are. */
    final Set<String> prerequisites;
    /** True if the translator is expected to push the whole predicate, false if to decline. */
    final boolean expectPushed;
    final String note;

    Case(String id, String where, Set<String> trust, Set<String> subjects,
        Set<String> prerequisites, boolean expectPushed, String note) {
      this.id = id;
      this.where = where;
      this.trust = trust;
      this.subjects = subjects;
      this.prerequisites = prerequisites;
      this.expectPushed = expectPushed;
      this.note = note;
    }

    String sql(String table) {
      return "SELECT sys_id FROM " + table + " WHERE " + where;
    }
  }

  enum Status {
    /** Pushed, and the two sets are equal. */
    MATCH,
    /** Pushed, and the sets differ. */
    MISMATCH,
    /** The case expected a push and the translator declined (or pushed only part of it). */
    NOT_PUSHED,
    /** The case expected the translator to decline and it pushed. */
    UNEXPECTEDLY_PUSHED,
    /** The case expected a decline and the translator declined; nothing to compare. */
    DECLINED
  }

  /** What happened to one case. */
  static final class Outcome {
    final Case testCase;
    final Status status;
    final String query;
    final Set<String> entriesUsed;
    final Set<String> onlyPushed;
    final Set<String> onlyCalcite;
    final int calciteRows;
    final int pushedRows;

    Outcome(Case testCase, Status status, String query, Set<String> entriesUsed,
        Set<String> onlyPushed, Set<String> onlyCalcite, int calciteRows, int pushedRows) {
      this.testCase = testCase;
      this.status = status;
      this.query = query;
      this.entriesUsed = entriesUsed;
      this.onlyPushed = onlyPushed;
      this.onlyCalcite = onlyCalcite;
      this.calciteRows = calciteRows;
      this.pushedRows = pushedRows;
    }
  }

  private final Mode mode;
  private final Source source;
  private final String table;

  PushdownHarness(Mode mode, Source source, String table) {
    this.mode = mode;
    this.source = source;
    this.table = table;
  }

  Mode mode() {
    return mode;
  }

  /** Runs one case. */
  Outcome run(Case testCase) throws SQLException {
    final PushdownTestSupport.Recorder calciteScans = new PushdownTestSupport.Recorder();
    final List<String> calcite;
    try (Connection conn = source.open(Collections.<String>emptySet(), calciteScans)) {
      calcite = PushdownTestSupport.column(conn, testCase.sql(table));
    }
    if (calciteScans.scans.isEmpty()) {
      throw new IllegalStateException("Case " + testCase.id + " never reached the table scan "
          + "(the planner folded the predicate): " + testCase.where);
    }
    if (!calciteScans.last().query.isEmpty()) {
      throw new IllegalStateException("The Calcite-side run pushed a query: " + calciteScans.last());
    }
    final PushdownTestSupport.Recorder pushedScans = new PushdownTestSupport.Recorder();
    final List<String> pushed;
    try (Connection conn = source.open(testCase.trust, pushedScans)) {
      pushed = PushdownTestSupport.column(conn, testCase.sql(table));
    }
    final PushdownTestSupport.Scan scan = pushedScans.last();
    final boolean fullyPushed = !scan.query.isEmpty() && scan.remaining == 0;
    if (testCase.expectPushed && !fullyPushed) {
      return outcome(testCase, Status.NOT_PUSHED, scan, calcite, pushed);
    }
    if (!testCase.expectPushed) {
      return outcome(testCase, scan.query.isEmpty() ? Status.DECLINED : Status.UNEXPECTEDLY_PUSHED,
          scan, calcite, pushed);
    }
    return outcome(testCase, Status.MATCH, scan, calcite, pushed);
  }

  /** Compares the two row sets of a pushed case; MATCH or MISMATCH. */
  private static Outcome outcome(Case testCase, Status status, PushdownTestSupport.Scan scan,
      List<String> calcite, List<String> pushed) {
    final Set<String> calciteSet = new TreeSet<>(calcite);
    final Set<String> pushedSet = new TreeSet<>(pushed);
    final Set<String> onlyPushed = new TreeSet<>(pushedSet);
    onlyPushed.removeAll(calciteSet);
    final Set<String> onlyCalcite = new TreeSet<>(calciteSet);
    onlyCalcite.removeAll(pushedSet);
    Status result = status;
    if (status == Status.MATCH
        && (!onlyPushed.isEmpty() || !onlyCalcite.isEmpty() || calcite.size() != pushed.size())) {
      result = Status.MISMATCH;
    }
    return new Outcome(testCase, result, scan.query, scan.entries, onlyPushed, onlyCalcite,
        calcite.size(), pushed.size());
  }

  /** Runs every case. */
  List<Outcome> runAll(List<Case> cases) throws SQLException {
    final List<Outcome> outcomes = new ArrayList<>();
    for (Case testCase : cases) {
      outcomes.add(run(testCase));
    }
    return outcomes;
  }

  // ---- verdicts -----------------------------------------------------------------------------

  /**
   * Decides, per entry that some case is evidence for, whether it is verified. Returns for each
   * such entry {status ("verified" or "mismatch"), number of cases, detail}. In OFFLINE mode
   * nothing is verified.
   */
  Map<String, String[]> verdicts(List<Outcome> outcomes) {
    final Map<String, List<Outcome>> bySubject = new LinkedHashMap<>();
    for (Outcome outcome : outcomes) {
      for (String subject : outcome.testCase.subjects) {
        bySubject.computeIfAbsent(subject, k -> new ArrayList<>()).add(outcome);
      }
    }
    final Set<String> proven = new LinkedHashSet<>();
    final Map<String, String> reason = new LinkedHashMap<>();
    for (Map.Entry<String, List<Outcome>> e : bySubject.entrySet()) {
      final StringBuilder why = new StringBuilder();
      boolean ok = mode == Mode.LIVE;
      if (!ok) {
        why.append("offline run: the stub is not evidence");
      }
      for (Outcome outcome : e.getValue()) {
        if (outcome.status != Status.MATCH) {
          ok = false;
          why.append(outcome.testCase.id).append(": ").append(outcome.status);
          if (outcome.status == Status.MISMATCH) {
            why.append(" (+").append(outcome.onlyPushed).append(" -")
                .append(outcome.onlyCalcite).append(")");
          }
          why.append("; ");
        }
      }
      if (ok) {
        proven.add(e.getKey());
      }
      reason.put(e.getKey(), why.toString());
    }
    // An entry stands only if the entries its cases depend on stand
    boolean changed = true;
    while (changed) {
      changed = false;
      for (String entry : new ArrayList<>(proven)) {
        for (Outcome outcome : bySubject.get(entry)) {
          for (String prerequisite : outcome.testCase.prerequisites) {
            if (!prerequisite.equals(entry) && !proven.contains(prerequisite)) {
              proven.remove(entry);
              reason.put(entry, "prerequisite " + prerequisite + " is not verified");
              changed = true;
              break;
            }
          }
          if (!proven.contains(entry)) {
            break;
          }
        }
      }
    }
    final Map<String, String[]> verdicts = new LinkedHashMap<>();
    for (Map.Entry<String, List<Outcome>> e : bySubject.entrySet()) {
      final boolean ok = proven.contains(e.getKey());
      verdicts.put(e.getKey(), new String[] {ok ? "verified" : "mismatch",
          Integer.toString(e.getValue().size()), ok ? "all cases matched" : reason.get(e.getKey())});
    }
    return verdicts;
  }

  /** Writes the verification record. Refuses unless the run was live. */
  void writeRecord(Path file, String instance, List<Outcome> outcomes) throws IOException {
    if (mode != Mode.LIVE) {
      throw new IllegalStateException(
          "An offline run cannot write a verification record: the stub is not evidence");
    }
    Files.write(file, PushdownVerification.toJson(instance, "live", verdicts(outcomes))
        .getBytes(StandardCharsets.UTF_8));
  }

  /** A readable report of every case. */
  String report(List<Outcome> outcomes) {
    final StringBuilder report = new StringBuilder("# Pushdown differential harness (")
        .append(mode).append(")\n\n| case | predicate | pushed query | result | calcite rows | "
            + "pushed rows | rows only pushed | rows only calcite |\n|---|---|---|---|---|---|---|---|\n");
    for (Outcome o : outcomes) {
      report.append("| ").append(o.testCase.id).append(" | `").append(cell(o.testCase.where))
          .append("` | `").append(cell(o.query)).append("` | ").append(o.status).append(" | ")
          .append(o.calciteRows).append(" | ").append(o.pushedRows).append(" | ")
          .append(o.onlyPushed).append(" | ").append(o.onlyCalcite).append(" |\n");
    }
    report.append("\n## Verdicts\n\n| entry | status | cases | detail |\n|---|---|---|---|\n");
    verdicts(outcomes).forEach((id, v) -> report.append("| ").append(id).append(" | ")
        .append(v[0]).append(" | ").append(v[1]).append(" | ").append(cell(v[2])).append(" |\n"));
    final Set<String> unexercised = new TreeSet<>(PushdownCapabilities.candidates());
    unexercised.removeAll(verdicts(outcomes).keySet());
    report.append("\nEntries no case exercised (stay unverified): ").append(unexercised)
        .append("\n");
    return report.toString();
  }

  private static String cell(String text) {
    return text.replace("|", "\\|");
  }
}
