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
package org.apache.calcite.adapter.govdata.law;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parsing of the Senate and House roll-call XML into one row per member. */
@Tag("unit")
class MemberRollCallVotesProviderTest {

  private static final String SENATE =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?><roll_call_vote>"
      + "<congress>118</congress><session>2</session><vote_number>100</vote_number>"
      + "<vote_date>March 21, 2024,  01:59 PM</vote_date>"
      + "<vote_question_text>On the Nomination PN157</vote_question_text>"
      + "<question>On the Nomination</question>"
      + "<vote_result>Nomination Confirmed</vote_result>"
      + "<document><document_name>PN157</document_name></document>"
      + "<members>"
      + "<member><last_name>Baldwin</last_name><first_name>Tammy</first_name>"
      + "<party>D</party><state>WI</state><vote_cast>Yea</vote_cast>"
      + "<lis_member_id>S354</lis_member_id></member>"
      + "<member><last_name>Barrasso</last_name><first_name>John</first_name>"
      + "<party>R</party><state>WY</state><vote_cast>Nay</vote_cast>"
      + "<lis_member_id>S317</lis_member_id></member>"
      + "</members></roll_call_vote>";

  private static final String HOUSE =
      "<?xml version=\"1.0\" encoding=\"utf-8\"?><rollcall-vote><vote-metadata>"
      + "<rollcall-num>100</rollcall-num><legis-num>H R 7023</legis-num>"
      + "<vote-question>On Motion to Recommit</vote-question>"
      + "<vote-result>Failed</vote-result><action-date>21-Mar-2024</action-date>"
      + "</vote-metadata><vote-data>"
      + "<recorded-vote><legislator name-id=\"A000370\" party=\"D\" state=\"NC\" "
      + "role=\"legislator\">Adams</legislator><vote>Yea</vote></recorded-vote>"
      + "<recorded-vote><legislator party=\"R\" state=\"TX\" role=\"legislator\">"
      + "Archer</legislator><vote>Not Voting</vote></recorded-vote>"
      + "</vote-data></rollcall-vote>";

  private static ByteArrayInputStream in(String xml) {
    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  @Test void senateRowsCarryVoteAndMemberFields() throws Exception {
    Deque<Map<String, Object>> out = new ArrayDeque<Map<String, Object>>();
    MemberRollCallVotesProvider.parseSenate(in(SENATE), "u", 2024, 118, 2, 100, out);
    assertEquals(2, out.size());
    Map<String, Object> row = out.removeFirst();
    assertEquals("Senate", row.get("chamber"));
    assertEquals("2024-03-21", row.get("vote_date"));
    assertEquals("On the Nomination", row.get("vote_question"));
    assertEquals("Nomination Confirmed", row.get("vote_result"));
    assertEquals("PN157", row.get("legis_num"));
    assertEquals("Baldwin", row.get("member_last_name"));
    assertEquals("Tammy", row.get("member_first_name"));
    assertEquals("S354", row.get("lis_member_id"));
    assertEquals("Yea", row.get("vote_cast"));
    assertFalse(row.containsKey("bioguide_id"));
  }

  @Test void houseRowsCarryBioguideWhenPresent() throws Exception {
    Deque<Map<String, Object>> out = new ArrayDeque<Map<String, Object>>();
    MemberRollCallVotesProvider.parseHouse(in(HOUSE), "u", 2024, 118, 2, 100, out);
    assertEquals(2, out.size());
    Map<String, Object> first = out.removeFirst();
    assertEquals("House", first.get("chamber"));
    assertEquals("2024-03-21", first.get("vote_date"));
    assertEquals("H R 7023", first.get("legis_num"));
    assertEquals("A000370", first.get("bioguide_id"));
    assertEquals("Adams", first.get("member_last_name"));
    assertEquals("NC", first.get("state"));
    Map<String, Object> second = out.removeFirst();
    assertFalse(second.containsKey("bioguide_id"));
    assertEquals("Not Voting", second.get("vote_cast"));
  }

  @Test void houseDateWithCapitalisedMonthParses() throws Exception {
    Deque<Map<String, Object>> out = new ArrayDeque<Map<String, Object>>();
    MemberRollCallVotesProvider.parseHouse(
        in(HOUSE.replace("21-Mar-2024", "3-JAN-2001")), "u", 2001, 107, 1, 2, out);
    assertEquals("2001-01-03", out.removeFirst().get("vote_date"));
  }

  @Test void fileWithNoMembersThrows() {
    String empty = "<rollcall-vote><vote-metadata><action-date>1-Jan-2000</action-date>"
        + "</vote-metadata><vote-data/></rollcall-vote>";
    assertThrows(java.io.IOException.class, () ->
        MemberRollCallVotesProvider.parseHouse(in(empty), "u", 2000, 106, 2, 1,
            new ArrayDeque<Map<String, Object>>()));
  }

  @Test void vacatedRollContributesNoRows() throws Exception {
    String vacated = "<rollcall-vote><vote-metadata><action-date>4-Jun-2015</action-date>"
        + "<vote-desc>This vote was vacated by unanimous consent on 4-Jun-2015.</vote-desc>"
        + "</vote-metadata><vote-data /></rollcall-vote>";
    Deque<Map<String, Object>> out = new ArrayDeque<Map<String, Object>>();
    MemberRollCallVotesProvider.parseHouse(in(vacated), "u", 2015, 114, 1, 300, out);
    assertTrue(out.isEmpty());
  }

  @Test void congressFollowsCalendarYear() {
    assertEquals(101, MemberRollCallVotesProvider.congressOf(1989));
    assertEquals(101, MemberRollCallVotesProvider.congressOf(1990));
    assertEquals(118, MemberRollCallVotesProvider.congressOf(2024));
    assertEquals(119, MemberRollCallVotesProvider.congressOf(2025));
  }
}
