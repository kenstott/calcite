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
import org.apache.calcite.adapter.servicenow.PushdownHarness.Status;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The harness, offline: against the local stub, which is this project's own reading of the
 * documentation and is NOT evidence about ServiceNow. It checks that every case's translator
 * decision (pushed in full, or declined) is what the case expects, and that the plumbing (row-set
 * comparison, verdicts, record writing) behaves; it can never mark an entry verified. The
 * server ignores the pushed terms, so many cases MISMATCH here; that is expected and says nothing.
 */
class PushdownHarnessTest {

  @TempDir Path temp;

  private PushdownHarness.Source source(FixtureServer server) {
    final Map<String, Object> extra = new HashMap<>();
    extra.put("catalogCacheDirectory", temp.resolve("catalog").toString());
    return (trusted, observer) ->
        PushdownTestSupport.open(server.url(), extra, trusted, observer);
  }

  private List<Case> cases(FixtureServer server) throws Exception {
    try (Connection conn = source(server).open(Collections.<String>emptySet(),
        new PushdownTestSupport.Recorder())) {
      return PushdownCases.build(PushdownCases.profile(conn, "incident"));
    }
  }

  @Test void everyCaseIsPushedOrDeclinedAsItsAuthorExpects() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.lenientTerms();
      final PushdownHarness harness = new PushdownHarness(Mode.OFFLINE, source(server), "incident");
      final List<Case> cases = cases(server);
      assertThat(cases.size() > 60, is(true));
      final List<Outcome> outcomes = harness.runAll(cases);
      final List<String> wrong = new ArrayList<>();
      for (Outcome outcome : outcomes) {
        if (outcome.status == Status.NOT_PUSHED || outcome.status == Status.UNEXPECTEDLY_PUSHED) {
          wrong.add(outcome.testCase.id + " -> " + outcome.status + " query='" + outcome.query
              + "' where " + outcome.testCase.where);
        }
      }
      assertThat(wrong.toString(), wrong.isEmpty(), is(true));
    }
  }

  @Test void anOfflineRunMarksNothingVerifiedAndCannotWriteARecord() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.lenientTerms();
      final PushdownHarness harness = new PushdownHarness(Mode.OFFLINE, source(server), "incident");
      final List<Outcome> outcomes = harness.runAll(cases(server));
      for (Map.Entry<String, String[]> verdict : harness.verdicts(outcomes).entrySet()) {
        assertThat(verdict.getKey(), verdict.getValue()[0], equalTo("mismatch"));
        assertThat(verdict.getValue()[2], containsString("offline"));
      }
      assertThrows(IllegalStateException.class,
          () -> harness.writeRecord(temp.resolve("record.json"), server.url(), outcomes));
      assertThat(Files.exists(temp.resolve("record.json")), is(false));
    }
  }

  @Test void aRunWhereTheServerFilteredNothingRecordsTheDifferingRows() throws Exception {
    // The stub ignores terms, so the pushed run returns every row while Calcite's run returns the
    // matching ones: the harness must report exactly which rows differ
    try (FixtureServer server = new FixtureServer()) {
      server.lenientTerms();
      final PushdownHarness harness = new PushdownHarness(Mode.OFFLINE, source(server), "incident");
      final Case eq = new Case("t:eq", "priority = 3",
          set(PushdownCapabilities.SHAPE_AND, "EQ:NUMERIC:AND"), set("EQ:NUMERIC:AND"),
          set(PushdownCapabilities.SHAPE_AND), true, "");
      final Outcome outcome = harness.run(eq);
      assertThat(outcome.status, is(Status.MISMATCH));
      assertThat(outcome.query, equalTo("priority=3"));
      assertThat(outcome.calciteRows, is(2));
      assertThat(outcome.pushedRows, is(5));
      assertThat(outcome.onlyPushed.size(), is(3));
      assertThat(outcome.onlyCalcite.isEmpty(), is(true));
    }
  }

  @Test void aRunWhereTheStubHappensToAgreeIsStillNotVerifiedOffline() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.lenientTerms();
      final PushdownHarness harness = new PushdownHarness(Mode.OFFLINE, source(server), "incident");
      // True of every row, so ignoring the term gives the same set
      final Case all = new Case("t:all", "sys_id IS NOT NULL",
          set(PushdownCapabilities.SHAPE_AND, "IS_NOT_NULL:GUID:AND"),
          set("IS_NOT_NULL:GUID:AND"), set(PushdownCapabilities.SHAPE_AND), true, "");
      final Outcome outcome = harness.run(all);
      assertThat(outcome.status, is(Status.MATCH));
      assertThat(harness.verdicts(Collections.singletonList(outcome)).get("IS_NOT_NULL:GUID:AND")[0],
          equalTo("mismatch"));
    }
  }

  // ---- verdict logic with synthetic outcomes (no server involved) --------------------------

  private static Set<String> set(String... entries) {
    return new TreeSet<>(Arrays.asList(entries));
  }

  private static Outcome outcome(String id, Set<String> subjects, Set<String> prerequisites,
      Status status, Set<String> onlyPushed) {
    final Case testCase = new Case(id, "x", subjects, subjects, prerequisites, true, "");
    return new Outcome(testCase, status, "q", subjects, onlyPushed,
        Collections.<String>emptySet(), 1, 1);
  }

  @Test void liveVerdictsFollowMatchesMismatchesAndPrerequisites() throws Exception {
    final PushdownHarness live = new PushdownHarness(Mode.LIVE, null, "incident");
    final String shape = PushdownCapabilities.SHAPE_AND;
    final List<Outcome> outcomes = Arrays.asList(
        outcome("shape", set(shape), Collections.<String>emptySet(), Status.MATCH,
            Collections.<String>emptySet()),
        outcome("eq1", set("EQ:TEXT:AND"), set(shape), Status.MATCH, Collections.<String>emptySet()),
        outcome("eq2", set("EQ:TEXT:AND"), set(shape), Status.MATCH, Collections.<String>emptySet()),
        outcome("lt1", set("LT:TEXT:AND"), set(shape), Status.MATCH, Collections.<String>emptySet()),
        outcome("lt2", set("LT:TEXT:AND"), set(shape), Status.MISMATCH, set("row9")),
        outcome("special", set(PushdownCapabilities.VALUE_SPECIAL), set(shape, "LT:TEXT:AND"),
            Status.MATCH, Collections.<String>emptySet()),
        outcome("ne", set("NE:TEXT:AND"), set(shape), Status.NOT_PUSHED,
            Collections.<String>emptySet()));
    final Map<String, String[]> verdicts = live.verdicts(outcomes);
    assertThat(verdicts.get(shape)[0], equalTo("verified"));
    assertThat(verdicts.get("EQ:TEXT:AND")[0], equalTo("verified"));
    assertThat(verdicts.get("EQ:TEXT:AND")[1], equalTo("2"));
    assertThat(verdicts.get("LT:TEXT:AND")[0], equalTo("mismatch"));
    assertThat(verdicts.get("LT:TEXT:AND")[2], containsString("row9"));
    // matched itself, but depends on an entry that did not verify
    assertThat(verdicts.get(PushdownCapabilities.VALUE_SPECIAL)[0], equalTo("mismatch"));
    assertThat(verdicts.get(PushdownCapabilities.VALUE_SPECIAL)[2],
        containsString("prerequisite LT:TEXT:AND"));
    assertThat(verdicts.get("NE:TEXT:AND")[0], equalTo("mismatch"));

    // The written record enables exactly the verified entries
    final Path file = temp.resolve("live-record.json");
    live.writeRecord(file, "https://dev1.service-now.com", outcomes);
    assertThat(PushdownVerification.resolve(file, URI.create("https://dev1.service-now.com"),
        Collections.<String>emptyList()), equalTo(set(shape, "EQ:TEXT:AND")));
  }

  @Test void theMatrixCoversTheEntriesItClaimsAndNamesTheRest() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final Set<String> covered = PushdownCases.covered(cases(server));
      final Set<String> uncovered = new TreeSet<>(PushdownCapabilities.candidates());
      uncovered.removeAll(covered);
      // Entries the matrix has no case for stay unverified forever; they must be a known list
      // The fixtures hold no value with punctuation; the live seed rows do (see the live test)
      assertThat(uncovered.toString(), uncovered, equalTo(set(PushdownCapabilities.VALUE_SPECIAL)));
    }
  }
}
