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
package org.apache.calcite.adapter.govdata.officials;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the strict reading of the FEC House results sheet with small synthetic sheets. */
@Tag("unit")
class FecHouseResultsParserTest {

  private static final FecHouseResultsParser.Release REL = FecHouseResultsParser.RELEASES[0];

  private static final List<String[]> LABELS = Arrays.asList(
      new String[] {"A GUIDE TO PARTY LABELS", "", ""},
      new String[] {"D", "=", "Democratic"},
      new String[] {"R", "=", "Republican"},
      new String[] {"W", "=", "Write-In"},
      new String[] {"WF", "=", "Working Families"},
      new String[] {"CRV", "=", "Conservative"},
      new String[] {"IP", "=", "Independent Party"},
      new String[] {"LIB", "=", "Libertarian"},
      new String[] {"APV", "=", "Approval Voting Party"});

  private static final List<String[]> PUBLICATION = Collections.singletonList(
      new String[] {"October 2022"});

  /** A candidate row: state, district, id, first, last, party, primary, general, runoff, win. */
  private static String[] cand(int n, String st, String dist, String id, String first,
      String last, String party, String primary, String general, String runoff, String win) {
    String[] r = new String[24];
    Arrays.fill(r, "");
    r[0] = String.valueOf(n);
    r[1] = st;
    r[2] = st.equals("NY") ? "New York" : st.equals("AK") ? "Alaska" : "Texas";
    r[3] = dist;
    r[4] = id;
    r[6] = first;
    r[7] = last;
    r[10] = party;
    r[11] = primary;
    r[15] = general;
    r[17] = runoff;
    r[22] = win;
    return r;
  }

  private static String[] districtVotes(int n, String st, String dist, String general,
      String runoff) {
    String[] r = cand(n, st, dist, "n/a", "", "", "", "", general, runoff, "");
    r[9] = "District Votes:";
    return r;
  }

  private static List<String[]> sheet(String[]... rows) {
    List<String[]> out = new ArrayList<String[]>();
    out.add(REL.header.clone());
    out.addAll(Arrays.asList(rows));
    return out;
  }

  private static List<Map<String, Object>> parse(List<String[]> house) throws IOException {
    return FecHouseResultsParser.parse(REL, house, LABELS, PUBLICATION);
  }

  @Test void fusionLinesWriteInsAndTotalsParse() throws Exception {
    List<Map<String, Object>> rows = parse(sheet(
        cand(2, "NY", "01", "H1", "Ann", "Lee", "D", "100", "600", "", "W"),
        cand(3, "NY", "01", "H1", "Ann", "Lee", "WF", "Unopposed", "50", "", "W"),
        cand(4, "NY", "01", "H2", "Bob", "Roe", "R", "90", "300", "", ""),
        cand(5, "NY", "01", "n/a", "", "Scattered", "W", "", "10", "", ""),
        cand(6, "NY", "01", "n/a", "Zed", "Quill", "W", "", "5", "", ""),
        // A primary-only candidate and a primary subtotal do not appear.
        cand(7, "NY", "01", "H3", "Cy", "Ng", "D", "40", "", "", ""),
        districtVotes(8, "NY", "01", "965", "")));
    assertEquals(5, rows.size());
    Map<String, Object> a = rows.get(0);
    assertEquals(2020, a.get("year"));
    assertEquals("36", a.get("state_fips"));
    assertEquals("Ann Lee", a.get("candidate"));
    assertEquals("democratic", a.get("party_published"));
    assertEquals("D", a.get("party_major"));
    assertEquals(600L, a.get("candidate_votes"));
    assertEquals(965L, a.get("total_votes"));
    assertEquals("FEC", a.get("source"));
    assertEquals(false, a.get("runoff"));
    assertEquals("OTHER", rows.get(1).get("party_major"));
    assertEquals("working families", rows.get(1).get("party_published"));
    assertEquals(2, rows.get(1).get("line_no"));
    assertNull(rows.get(3).get("candidate"));
    assertEquals("write_in_aggregate", rows.get(3).get("candidate_kind"));
    assertEquals(true, rows.get(3).get("writein"));
    assertEquals("candidate", rows.get(4).get("candidate_kind"));
    assertNull(rows.get(4).get("party_major"));
    assertEquals(true, rows.get(4).get("writein"));
  }

  @Test void combinedLabelNeedsPrimaryEntryAndAliasResolves() throws Exception {
    List<Map<String, Object>> rows = parse(sheet(
        cand(2, "TX", "04", "H1", "Ann", "Lee", "D/WF", "100", "600", "", "W"),
        cand(3, "TX", "04", "H2", "Bob", "Roe", "D/IP", "", "300", "", ""),
        cand(4, "TX", "04", "H3", "Cat", "Kok", "AVP", "", "100", "", ""),
        districtVotes(5, "TX", "04", "1000", "")));
    assertEquals("D", rows.get(0).get("party_major"));
    assertEquals("democratic/working families", rows.get(0).get("party_published"));
    assertEquals("OTHER", rows.get(1).get("party_major"));
    assertEquals("approval voting party", rows.get(2).get("party_published"));
  }

  @Test void unopposedBecomesTheLabsOneOfOneMarker() throws Exception {
    List<Map<String, Object>> rows = parse(sheet(
        cand(2, "TX", "25", "H1", "Mario", "Diaz", "R", "Unopposed", "Unopposed", "", "W"),
        districtVotes(3, "TX", "25", "", "")));
    assertEquals(1, rows.size());
    assertEquals(1L, rows.get(0).get("candidate_votes"));
    assertEquals(1L, rows.get(0).get("total_votes"));
    assertEquals(true, rows.get(0).get("uncontested"));
  }

  @Test void runoffMakesASecondRound() throws Exception {
    List<Map<String, Object>> rows = parse(sheet(
        cand(2, "TX", "05", "H1", "Ann", "Lee", "R", "", "60", "70", "W*"),
        cand(3, "TX", "05", "H2", "Bob", "Roe", "R", "", "40", "30", ""),
        districtVotes(4, "TX", "05", "100", "100")));
    assertEquals(4, rows.size());
    assertEquals(false, rows.get(0).get("runoff"));
    assertEquals(true, rows.get(2).get("runoff"));
    assertEquals(70L, rows.get(2).get("candidate_votes"));
    assertEquals(100L, rows.get(2).get("total_votes"));
  }

  @Test void delegatesAreSkippedAndCaucusRowsAfterTheTotalAreAllowed() throws Exception {
    String[] caucus = cand(5, "TX", "04", "n/a", "", "", "R", "145", "", "", "");
    caucus[9] = "Party Caucus Votes:";
    List<Map<String, Object>> rows = parse(sheet(
        cand(2, "GU", "00", "H9", "Del", "Egate", "D", "", "5", "", "W"),
        cand(3, "TX", "04", "H1", "Ann", "Lee", "R", "", "60", "", "W"),
        districtVotes(4, "TX", "04", "60", ""),
        caucus));
    assertEquals(1, rows.size());
  }

  @Test void subtotalMismatchFails() {
    IOException e = assertThrows(IOException.class, () -> parse(sheet(
        cand(2, "TX", "04", "H1", "Ann", "Lee", "R", "", "60", "", "W"),
        districtVotes(3, "TX", "04", "61", ""))));
    assertTrue(e.getMessage().contains("sum to 60"), e.getMessage());
  }

  @Test void winnerFlagOnTheTrailingCandidateFails() {
    IOException e = assertThrows(IOException.class, () -> parse(sheet(
        cand(2, "TX", "04", "H1", "Ann", "Lee", "R", "", "60", "", ""),
        cand(3, "TX", "04", "H2", "Bob", "Roe", "D", "", "40", "", "W"),
        districtVotes(4, "TX", "04", "100", ""))));
    assertTrue(e.getMessage().contains("winner flags"), e.getMessage());
  }

  @Test void unknownPartyStateAndHeaderFail() {
    IOException e = assertThrows(IOException.class, () -> parse(sheet(
        cand(2, "TX", "04", "H1", "Ann", "Lee", "ZZZ", "", "60", "", "W"),
        districtVotes(3, "TX", "04", "60", ""))));
    assertTrue(e.getMessage().contains("ZZZ"), e.getMessage());
    e = assertThrows(IOException.class, () -> parse(sheet(
        cand(2, "XX", "04", "H1", "Ann", "Lee", "R", "", "60", "", "W"),
        districtVotes(3, "XX", "04", "60", ""))));
    assertTrue(e.getMessage().contains("XX"), e.getMessage());
    List<String[]> bad = sheet(cand(2, "TX", "04", "H1", "Ann", "Lee", "R", "", "60", "", "W"),
        districtVotes(3, "TX", "04", "60", ""));
    bad.get(0)[15] = "VOTES";
    e = assertThrows(IOException.class, () -> parse(bad));
    assertTrue(e.getMessage().contains("header column 15"), e.getMessage());
  }

  @Test void combiningDemocraticAndRepublicanFailsAndReplacedReleaseFails() {
    assertThrows(IOException.class, () -> parse(sheet(
        cand(2, "TX", "04", "H1", "Ann", "Lee", "D/R", "1", "60", "", "W"),
        districtVotes(3, "TX", "04", "60", ""))));
    IOException e = assertThrows(IOException.class, () -> FecHouseResultsParser.parse(REL,
        sheet(cand(2, "TX", "04", "H1", "Ann", "Lee", "R", "", "60", "", "W"),
            districtVotes(3, "TX", "04", "60", "")),
        LABELS, Collections.singletonList(new String[] {"December 2030"})));
    assertTrue(e.getMessage().contains("replaced"), e.getMessage());
    assertFalse(REL.url.isEmpty());
  }
}
