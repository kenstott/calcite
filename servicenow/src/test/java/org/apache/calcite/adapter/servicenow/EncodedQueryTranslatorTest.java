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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The translator's output text, and the rule that an unverified entry is never pushed. The
 * expected text is what the translator writes; it is NOT evidence that ServiceNow reads the text
 * as SQL would, which only the live differential harness can show. The server here ignores the
 * terms it is sent ({@link FixtureServer#lenientTerms}), so no assertion is about returned rows.
 */
class EncodedQueryTranslatorTest {

  @TempDir Path cache;

  /** Every entry, as if a live harness run had verified all of them. */
  private static final Set<String> ALL = PushdownCapabilities.candidates();

  private PushdownTestSupport.Scan scan(Collection<String> trusted, String sql) throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.lenientTerms();
      final PushdownTestSupport.Recorder recorder = new PushdownTestSupport.Recorder();
      final Map<String, Object> extra = new HashMap<>();
      extra.put("catalogCacheDirectory", cache.toString());
      try (Connection conn = PushdownTestSupport.open(server.url(), extra, trusted, recorder)) {
        PushdownTestSupport.column(conn, sql);
      }
      return recorder.last();
    }
  }

  private String pushed(String where) throws Exception {
    final PushdownTestSupport.Scan scan =
        scan(ALL, "SELECT sys_id FROM incident WHERE " + where);
    assertThat("filters left for Calcite: " + scan, scan.remaining, is(0));
    return scan.query;
  }

  private void declined(String where) throws Exception {
    final PushdownTestSupport.Scan scan =
        scan(ALL, "SELECT sys_id FROM incident WHERE " + where);
    assertThat(scan.toString(), scan.query, equalTo(""));
    assertThat(scan.toString(), scan.remaining, is(1));
  }

  @Test void nothingIsPushedWhenNothingIsVerified() throws Exception {
    for (String where : new String[] {"priority = 2", "number LIKE 'INC%'", "active",
        "closed_at IS NULL", "priority IN (1, 3)"}) {
      final PushdownTestSupport.Scan scan = scan(Collections.<String>emptySet(),
          "SELECT sys_id FROM incident WHERE " + where);
      assertThat(scan.toString(), scan.query, equalTo(""));
      assertThat(scan.toString(), scan.remaining, is(1));
    }
  }

  @Test void comparisons() throws Exception {
    assertThat(pushed("priority = 2"), equalTo("priority=2"));
    assertThat(pushed("priority < 2"), equalTo("priority<2"));
    assertThat(pushed("priority <= 2"), equalTo("priority<=2"));
    assertThat(pushed("priority > 2"), equalTo("priority>2"));
    assertThat(pushed("priority >= 2"), equalTo("priority>=2"));
    assertThat(pushed("2 < priority"), equalTo("priority>2"));
    assertThat(pushed("number = 'INC0000001'"), equalTo("number=INC0000001"));
    assertThat(pushed("opened_at > TIMESTAMP '2026-01-02 00:00:00'"),
        equalTo("opened_at>2026-01-02 00:00:00"));
    assertThat(pushed("start_date = DATE '2026-02-01'"), equalTo("start_date=2026-02-01"));
    assertThat(pushed("caller_id = '" + String.format("%032x", 0xb1) + "'"),
        equalTo("caller_id=" + String.format("%032x", 0xb1)));
  }

  @Test void negatedOperatorsGetTheAddedNotEmptyTermWhenThatFormIsVerified() throws Exception {
    assertThat(pushed("priority <> 2"), equalTo("priority!=2^priorityISNOTEMPTY"));
    assertThat(pushed("NOT (priority = 2)"), equalTo("priority!=2^priorityISNOTEMPTY"));
    assertThat(pushed("priority NOT IN (1, 3)"),
        equalTo("priorityNOT IN1,3^priorityISNOTEMPTY"));
    // NOT of an ordering inverts it; SQL excludes empty rows from both forms
    assertThat(pushed("NOT (priority < 2)"), equalTo("priority>=2"));
  }

  @Test void thePlainNegatedFormIsUsedOnlyWhenTheAddedTermFormIsNotVerified() throws Exception {
    final Set<String> trusted = new TreeSet<>(Arrays.asList(PushdownCapabilities.SHAPE_AND,
        "NE:NUMERIC:AND"));
    final PushdownTestSupport.Scan plain =
        scan(trusted, "SELECT sys_id FROM incident WHERE priority <> 2");
    assertThat(plain.query, equalTo("priority!=2"));
    assertThat(plain.entries, hasItems("NE:NUMERIC:AND", PushdownCapabilities.SHAPE_AND));
    trusted.add("NE_NOT_EMPTY:NUMERIC:AND");
    assertThat(scan(trusted, "SELECT sys_id FROM incident WHERE priority <> 2").query,
        equalTo("priority!=2^priorityISNOTEMPTY"));
  }

  @Test void inLists() throws Exception {
    assertThat(pushed("priority IN (1, 3)"), equalTo("priorityIN1,3"));
    assertThat(pushed("priority IN (2)"), equalTo("priority=2"));
    assertThat(pushed("number IN ('a', 'b')"), equalTo("numberINa,b"));
  }

  @Test void rangesAndBetween() throws Exception {
    assertThat(pushed("priority BETWEEN 2 AND 3"), equalTo("priority>=2^priority<=3"));
    assertThat(pushed("priority > 1 AND priority <= 3"), equalTo("priority>1^priority<=3"));
  }

  @Test void nullTests() throws Exception {
    assertThat(pushed("closed_at IS NULL"), equalTo("closed_atISEMPTY"));
    assertThat(pushed("closed_at IS NOT NULL"), equalTo("closed_atISNOTEMPTY"));
    assertThat(pushed("number IS NULL"), equalTo("numberISEMPTY"));
  }

  @Test void likeFormsThatAreExactlyAPrefixSuffixContainsOrEquality() throws Exception {
    assertThat(pushed("number LIKE 'INC%'"), equalTo("numberSTARTSWITHINC"));
    assertThat(pushed("number LIKE '%7'"), equalTo("numberENDSWITH7"));
    assertThat(pushed("number LIKE '%000%'"), equalTo("numberLIKE000"));
    assertThat(pushed("number LIKE 'INC0000001'"), equalTo("number=INC0000001"));
  }

  @Test void booleans() throws Exception {
    assertThat(pushed("active"), equalTo("active=true"));
    assertThat(pushed("NOT active"), equalTo("active=false"));
    assertThat(pushed("active = false"), equalTo("active=false"));
  }

  @Test void conjunctsAreJoinedWithCaret() throws Exception {
    final String query = pushed("priority = 2 AND number = 'x' AND active");
    assertThat(Arrays.asList(query.split("\\^")),
        hasItems("priority=2", "number=x", "active=true"));
  }

  @Test void orGroupsUseCaretOr() throws Exception {
    assertThat(pushed("priority = 1 OR number = 'x'"), equalTo("priority=1^ORnumber=x"));
    final String mixed = pushed("priority = 1 AND (active OR number = 'a')");
    assertThat(mixed, containsString("active=true^ORnumber=a"));
    assertThat(mixed, containsString("priority=1"));
    final String twoGroups = pushed("(priority = 1 OR active) AND (number = 'a' OR priority = 3)");
    assertThat(twoGroups, containsString("priority=1^ORactive=true"));
    assertThat(twoGroups, containsString("number=a^ORpriority=3"));
  }

  @Test void notOverOrIsPushedAsAnAndOfNegationsBecausePlannerRewritesIt() throws Exception {
    // Calcite applies De Morgan before the scan sees the filter; the translator never handles
    // NOT over OR itself
    assertThat(pushed("NOT (priority = 1 OR active)"),
        equalTo("priority!=1^priorityISNOTEMPTY^active=false"));
  }

  @Test void notOverAndBecomesAnOrGroupWithTheRawNegatedForm() throws Exception {
    // Inside an OR group the added ISNOTEMPTY term cannot be written, so only the plain != form
    // is available; the harness case for NE:*:OR decides whether it may ever be pushed
    assertThat(pushed("NOT (priority = 1 AND active)"), equalTo("priority!=1^ORactive=false"));
  }

  @Test void shapesThatNeedParenthesesOrNqStayInCalcite() throws Exception {
    declined("(priority = 1 AND active) OR number = 'x'");
    declined("(priority > 1 AND priority < 3) OR active");
  }

  @Test void whatCannotBeExactStaysInCalcite() throws Exception {
    declined("number = ''");
    declined("number <> ''");
    declined("number IN ('', 'a')");
    declined("number = 'a^b'");
    declined("number IN ('a,b', 'c')");
    declined("number LIKE 'a_c%'");
    declined("number LIKE '%a%b%'");
    declined("number LIKE 'a%b'");
    declined("number NOT LIKE 'a%'");
    declined("UPPER(number) = 'A'");
    declined("caller_id__display = 'x'");
    declined("priority + 1 = 3");
  }

  @Test void aValueWithPunctuationNeedsTheSpecialValueEntry() throws Exception {
    assertThat(pushed("number = 'a=b'"), equalTo("number=a=b"));
    final Set<String> withoutSpecial = new TreeSet<>(ALL);
    withoutSpecial.remove(PushdownCapabilities.VALUE_SPECIAL);
    final PushdownTestSupport.Scan scan =
        scan(withoutSpecial, "SELECT sys_id FROM incident WHERE number = 'a=b'");
    assertThat(scan.query, equalTo(""));
    assertThat(scan.remaining, is(1));
    assertThat(scan(withoutSpecial, "SELECT sys_id FROM incident WHERE number = 'ab'").query,
        equalTo("number=ab"));
  }

  @Test void aFilterWhoseEntryIsUnverifiedStaysAndTheOthersAreStillPushed() throws Exception {
    final Set<String> trusted = new TreeSet<>(Arrays.asList(PushdownCapabilities.SHAPE_AND,
        "EQ:NUMERIC:AND"));
    final PushdownTestSupport.Scan scan = scan(trusted,
        "SELECT sys_id FROM incident WHERE priority = 2 AND number = 'x'");
    assertThat(scan.query, equalTo("priority=2"));
    assertThat(scan.remaining, is(1));
    assertThat(scan.entries, equalTo((Set<String>) new TreeSet<>(trusted)));
  }

  @Test void withoutTheAndShapeNothingIsPushedAndWithoutOrShapesNoOrGroup() throws Exception {
    final Set<String> noAnd = new TreeSet<>(ALL);
    noAnd.remove(PushdownCapabilities.SHAPE_AND);
    assertThat(scan(noAnd, "SELECT sys_id FROM incident WHERE priority = 2").query, equalTo(""));

    final Set<String> noOr = new TreeSet<>(ALL);
    noOr.remove(PushdownCapabilities.SHAPE_OR_WITH_OTHER_TERMS);
    final PushdownTestSupport.Scan scan =
        scan(noOr, "SELECT sys_id FROM incident WHERE priority = 2 AND (active OR number = 'a')");
    assertThat(scan.query, equalTo("priority=2"));
    assertThat(scan.remaining, is(1));
  }

  @Test void aLimitIsNeverSentAndSurvivesAPushedFilter() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.lenientTerms();
      final PushdownTestSupport.Recorder recorder = new PushdownTestSupport.Recorder();
      final Map<String, Object> extra = new HashMap<>();
      extra.put("catalogCacheDirectory", cache.toString());
      extra.put("pageSize", 100);
      try (Connection conn = PushdownTestSupport.open(server.url(), extra, ALL, recorder)) {
        PushdownTestSupport.column(conn, "SELECT sys_id FROM incident WHERE priority = 2 LIMIT 1");
      }
      for (FixtureServer.Seen seen : server.requests("incident")) {
        assertThat(seen.params.get("sysparm_limit"), equalTo("100"));
        assertThat(seen.params.get("sysparm_query"), containsString("priority=2^sys_id>"
            .substring(0, 10)));
      }
    }
  }

  // ---- the verification record and the trustPushdown operand -----------------------------

  private static Path record(Path dir, String json) throws Exception {
    final Path file = dir.resolve("record.json");
    Files.write(file, json.getBytes(StandardCharsets.UTF_8));
    return file;
  }

  private static final URI INSTANCE = URI.create("https://dev1.service-now.com");

  @Test void theBundledRecordVerifiesNothing() {
    assertThat(PushdownVerification.resolve(null, INSTANCE, Collections.<String>emptyList())
        .isEmpty(), is(true));
  }

  @Test void onlyVerifiedStatusEnablesAnEntry() throws Exception {
    final Path file = record(cache, "{\"version\":1,\"instance\":\"https://dev1.service-now.com\","
        + "\"entries\":{\"EQ:TEXT:AND\":{\"status\":\"verified\"},"
        + "\"LT:TEXT:AND\":{\"status\":\"mismatch\"}}}");
    assertThat(PushdownVerification.resolve(file, INSTANCE, Collections.<String>emptyList()),
        equalTo((Set<String>) new TreeSet<>(Arrays.asList("EQ:TEXT:AND"))));
  }

  @Test void aRecordFromAnotherInstanceIsRejected() throws Exception {
    final Path file = record(cache, "{\"version\":1,\"instance\":\"https://other.service-now.com\","
        + "\"entries\":{}}");
    final ServiceNowException e = assertThrows(ServiceNowException.class,
        () -> PushdownVerification.resolve(file, INSTANCE, Collections.<String>emptyList()));
    assertThat(e.getMessage(), containsString("other.service-now.com"));
  }

  @Test void unknownEntriesAndStatusesAreErrors() throws Exception {
    final Path unknownEntry = record(cache, "{\"version\":1,\"entries\":"
        + "{\"EQ:NOPE:AND\":{\"status\":\"verified\"}}}");
    assertThrows(ServiceNowException.class,
        () -> PushdownVerification.resolve(unknownEntry, INSTANCE,
            Collections.<String>emptyList()));
    final Path unknownStatus = record(cache, "{\"version\":1,\"entries\":"
        + "{\"EQ:TEXT:AND\":{\"status\":\"probably\"}}}");
    assertThrows(ServiceNowException.class,
        () -> PushdownVerification.resolve(unknownStatus, INSTANCE,
            Collections.<String>emptyList()));
    assertThrows(IllegalArgumentException.class,
        () -> PushdownVerification.resolve(null, INSTANCE, Arrays.asList("EQ:NOPE:AND")));
  }

  @Test void trustPushdownNamesEntriesExplicitly() {
    assertThat(PushdownVerification.resolve(null, INSTANCE, Arrays.asList("EQ:TEXT:AND")),
        equalTo((Set<String>) new TreeSet<>(Arrays.asList("EQ:TEXT:AND"))));
  }

  @Test void recordsRoundTrip() throws Exception {
    final Map<String, String[]> entries = new LinkedHashMap<>();
    entries.put("EQ:TEXT:AND", new String[] {"verified", "3", "ok"});
    entries.put("LT:TEXT:AND", new String[] {"mismatch", "2", "rows differ"});
    final Path file = record(cache,
        PushdownVerification.toJson("https://dev1.service-now.com", "live", entries));
    assertThat(PushdownVerification.resolve(file, INSTANCE, Collections.<String>emptyList()),
        equalTo((Set<String>) new TreeSet<>(Arrays.asList("EQ:TEXT:AND"))));
  }
}
