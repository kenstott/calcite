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

import org.apache.calcite.adapter.govdata.GovDataException;
import org.apache.calcite.adapter.govdata.law.CongressBillStatusProvider.BillRecord;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link CongressBillStatusProvider#parse} and {@link CongressBillStatusProvider#rows}
 * against real 119th-Congress BILLSTATUS files downloaded from govinfo.gov on 2026-09-25. Each
 * fixture was picked as the smallest bill showing one feature: H.R. 22 (19 actions, 110
 * cosponsors), H.R. 100 (no cosponsors), H.R. 9410 (a subcommittee), S. 5340 (a related bill),
 * S. 4138 (enacted; multi-format and undated text versions), H.R. 4944 (a text version with no
 * format), S. 4128 / S. 1199 / H.R. 1689 (amendments, incl. one amending an amendment and one
 * sponsored by a committee), S. 1360 (a withdrawn cosponsor), and, across the 108th-119th, the
 * smallest bill for each remaining section and each legacy spelling (see the test names). The
 * two shapes that occur only in very large files (a bill-level {@code recordedVotes} block, from
 * H.R. 3354 of the 115th, and a {@code notes/item/cdata/text} note, from H.R. 5009 of the 118th)
 * are excerpted verbatim from those files.
 */
@Tag("unit")
class CongressBillStatusProviderTest {

  private static BillRecord load(String name) throws Exception {
    try (InputStream in = CongressBillStatusProviderTest.class
        .getResourceAsStream("/law/billstatus/" + name)) {
      assertTrue(in != null, "missing fixture " + name);
      return CongressBillStatusProvider.parse(in);
    }
  }

  @Test void billRowCarriesStatusSponsorAndCounts() throws Exception {
    BillRecord rec = load("BILLSTATUS-119hr22.xml");
    List<Map<String, Object>> rows = CongressBillStatusProvider.rows("bills", rec);
    assertEquals(1, rows.size());
    Map<String, Object> row = rows.get(0);
    assertEquals(119, row.get("congress"));
    assertEquals(22, row.get("bill_number"));
    assertEquals("House", row.get("origin_chamber"));
    assertEquals("Government Operations and Politics", row.get("policy_area"));
    assertEquals("R000614", row.get("sponsor_bioguide_id"));
    assertEquals("2025-04-10", row.get("latest_action_date"));
    assertEquals("Received in the Senate.", row.get("latest_action_text"));
    assertEquals(19, row.get("action_count"));
    assertEquals(110, row.get("cosponsor_count"));
    assertNull(row.get("law_type"));
    assertNull(row.get("law_number"));
  }

  @Test void actionsAreNumberedOldestFirst() throws Exception {
    BillRecord rec = load("BILLSTATUS-119hr22.xml");
    List<Map<String, Object>> rows = CongressBillStatusProvider.rows("bill_actions", rec);
    assertEquals(19, rows.size());
    // The file lists newest-first: the first row is the newest action, seq = 19.
    assertEquals(19, rows.get(0).get("action_seq"));
    assertEquals("2025-04-10", rows.get(0).get("action_date"));
    Map<String, Object> oldest = rows.get(18);
    assertEquals(1, oldest.get("action_seq"));
    assertEquals("2025-01-03", oldest.get("action_date"));
    assertEquals(22, oldest.get("bill_number"));
    // A field nested under actions/item/sourceSystem is read, one under
    // actions/item/committees/item is not mistaken for an action field.
    assertTrue(rows.stream().anyMatch(r -> "Senate".equals(r.get("source_system"))));
  }

  @Test void cosponsorsReadEveryItemAndParseBooleans() throws Exception {
    BillRecord rec = load("BILLSTATUS-119hr22.xml");
    List<Map<String, Object>> rows = CongressBillStatusProvider.rows("bill_cosponsors", rec);
    assertEquals(110, rows.size());
    Map<String, Object> first = rows.get(0);
    assertEquals("G000597", first.get("bioguide_id"));
    assertEquals("NY", first.get("state"));
    assertEquals("2025-01-03", first.get("sponsorship_date"));
    assertEquals(Boolean.TRUE, first.get("is_original_cosponsor"));
    assertNull(first.get("withdrawn_date"));
    assertEquals(22, first.get("bill_number"));
  }

  @Test void billWithoutCosponsorsYieldsNoCosponsorRows() throws Exception {
    BillRecord rec = load("BILLSTATUS-119hr100.xml");
    assertTrue(CongressBillStatusProvider.rows("bill_cosponsors", rec).isEmpty());
    Map<String, Object> bill = CongressBillStatusProvider.rows("bills", rec).get(0);
    assertEquals(0, bill.get("cosponsor_count"));
    assertEquals(3, bill.get("action_count"));
    assertEquals("Law", bill.get("policy_area"));
  }

  @Test void nestedFieldsWithSameLocalNameDoNotOverwriteBillFields() throws Exception {
    // relatedBills/item, titles/item and subjects/policyArea reuse the local names number,
    // type, title, name and latestAction that the bill itself also has; only the bill's own
    // must land in the bills row.
    BillRecord rec = load("BILLSTATUS-119hr22.xml");
    assertEquals("HR", rec.bill.get("type"));
    assertEquals("22", rec.bill.get("number"));
    assertFalse(rec.bill.get("title").isEmpty());
  }

  @Test void documentMissingBillKeyIsRejected() {
    String xml = "<billStatus><bill><type>HR</type><congress>119</congress></bill></billStatus>";
    GovDataException e = assertThrows(GovDataException.class,
        () -> CongressBillStatusProvider.parse(
            new java.io.ByteArrayInputStream(xml.getBytes(java.nio.charset.StandardCharsets.UTF_8))));
    assertTrue(e.getMessage().contains("bill/number"));
  }

  private static List<Map<String, Object>> rows(String table, String fixture) throws Exception {
    return CongressBillStatusProvider.rows(table, load(fixture));
  }

  @Test void subcommitteeRowsPointAtTheirParentCommittee() throws Exception {
    List<Map<String, Object>> rows = rows("bill_committees", "BILLSTATUS-119hr9410.xml");
    assertEquals(2, rows.size());
    Map<String, Object> parent = rows.get(0);
    assertEquals("hsvr00", parent.get("committee_system_code"));
    assertEquals("House", parent.get("chamber"));
    assertEquals("Standing", parent.get("committee_type"));
    assertNull(parent.get("parent_committee_system_code"));
    Map<String, Object> sub = rows.get(1);
    assertEquals("hsvr10", sub.get("committee_system_code"));
    assertEquals("hsvr00", sub.get("parent_committee_system_code"));
    assertNull(sub.get("chamber"));
    assertEquals(9410, sub.get("bill_number"));
  }

  @Test void committeeActivitiesCoverCommitteesAndSubcommittees() throws Exception {
    List<Map<String, Object>> rows = rows("bill_committee_activities", "BILLSTATUS-119hr9410.xml");
    assertEquals(2, rows.size());
    assertEquals("hsvr00", rows.get(0).get("committee_system_code"));
    assertEquals("Referred To", rows.get(0).get("activity_name"));
    assertEquals("2026-06-23T16:03:35Z", rows.get(0).get("activity_date"));
    assertEquals("hsvr10", rows.get(1).get("committee_system_code"));
    assertEquals("2026-07-06T14:13:05Z", rows.get(1).get("activity_date"));
  }

  @Test void subjectsAreOneRowEach() throws Exception {
    List<Map<String, Object>> rows = rows("bill_subjects", "BILLSTATUS-119s1199.xml");
    assertEquals(4, rows.size());
    assertEquals("Administrative law and regulatory procedures", rows.get(0).get("subject_name"));
    assertTrue(rows.get(0).get("update_date") != null);
  }

  @Test void relatedBillTypeIsLowercasedToMatchTheBillTypePartition() throws Exception {
    List<Map<String, Object>> rows = rows("bill_related_bills", "BILLSTATUS-119s5340.xml");
    assertEquals(1, rows.size());
    Map<String, Object> row = rows.get(0);
    assertEquals(119, row.get("related_congress"));
    assertEquals("s", row.get("related_bill_type"));
    assertEquals(4784, row.get("related_bill_number"));
    assertEquals("Related bill", row.get("relationship_type"));
    assertEquals("CRS", row.get("identified_by"));
    assertEquals("2026-07-27", row.get("related_latest_action_date"));
  }

  @Test void textVersionsHaveOneRowPerFormatAndEmptyDatesAreNull() throws Exception {
    List<Map<String, Object>> rows = rows("bill_text_versions", "BILLSTATUS-119s4138.xml");
    // Enrolled Bill has two formats, the other three versions one each.
    assertEquals(5, rows.size());
    assertEquals("Enrolled Bill", rows.get(0).get("version_type"));
    assertNull(rows.get(0).get("version_date"));
    assertEquals("United States Legislative Markup", rows.get(0).get("format_type"));
    assertNull(rows.get(1).get("format_type"));
    assertEquals("Enrolled Bill", rows.get(1).get("version_type"));
    assertEquals("2026-03-18T04:00:00Z", rows.get(2).get("version_date"));
    Map<String, Object> bill = rows("bills", "BILLSTATUS-119s4138.xml").get(0);
    assertEquals("Public Law", bill.get("law_type"));
    assertEquals("119-80", bill.get("law_number"));
  }

  @Test void textVersionWithNoFormatIsKeptWithNoUrl() throws Exception {
    List<Map<String, Object>> rows = rows("bill_text_versions", "BILLSTATUS-119hr4944.xml");
    assertEquals(1, rows.size());
    assertEquals("Introduced in House", rows.get(0).get("version_type"));
    assertEquals("2025-08-08T04:00:00Z", rows.get(0).get("version_date"));
    assertNull(rows.get(0).get("url"));
  }

  @Test void amendmentRowCountsItsOwnActionsAndCosponsors() throws Exception {
    List<Map<String, Object>> rows = rows("bill_amendments", "BILLSTATUS-119s4128.xml");
    assertEquals(1, rows.size());
    Map<String, Object> row = rows.get(0);
    assertEquals("SAMDT", row.get("amendment_type"));
    assertEquals(6832, row.get("amendment_number"));
    assertEquals("Senate", row.get("chamber"));
    assertEquals("R000608", row.get("sponsor_bioguide_id"));
    assertTrue(((String) row.get("purpose")).startsWith("To apply the prohibition"));
    assertEquals(3, row.get("action_count"));
    assertEquals(0, row.get("cosponsor_count"));
    assertNull(row.get("amended_amendment_number"));
    assertEquals(4128, row.get("bill_number"));
  }

  @Test void amendmentToAnAmendmentRecordsWhatItAmends() throws Exception {
    Map<String, Object> row = null;
    for (Map<String, Object> r : rows("bill_amendments", "BILLSTATUS-119s1199.xml")) {
      if (Integer.valueOf(5441).equals(r.get("amendment_number"))) {
        row = r;
      }
    }
    assertTrue(row != null, "SAMDT 5441 not found");
    assertEquals("SAMDT", row.get("amended_amendment_type"));
    assertEquals(5440, row.get("amended_amendment_number"));
  }

  @Test void committeeSponsoredAmendmentHasNameButNoBioguideId() throws Exception {
    Map<String, Object> row = null;
    for (Map<String, Object> r : rows("bill_amendments", "BILLSTATUS-119hr1689.xml")) {
      if (Integer.valueOf(174).equals(r.get("amendment_number"))) {
        row = r;
      }
    }
    assertTrue(row != null, "HAMDT 174 not found");
    assertEquals("HAMDT", row.get("amendment_type"));
    assertEquals("Rules Committee", row.get("sponsor_name"));
    assertNull(row.get("sponsor_bioguide_id"));
  }

  @Test void withdrawnCosponsorKeepsItsWithdrawalDate() throws Exception {
    List<Map<String, Object>> rows = rows("bill_cosponsors", "BILLSTATUS-119s1360.xml");
    assertEquals(2, rows.size());
    Map<String, Object> withdrawn = null;
    for (Map<String, Object> r : rows) {
      if ("V000128".equals(r.get("bioguide_id"))) {
        withdrawn = r;
      }
    }
    assertTrue(withdrawn != null, "V000128 not found");
    assertEquals("2025-04-09", withdrawn.get("withdrawn_date"));
    assertEquals("2025-04-08", withdrawn.get("sponsorship_date"));
    assertEquals(Boolean.TRUE, withdrawn.get("is_original_cosponsor"));
  }

  private static BillRecord parseXml(String xml) throws Exception {
    return CongressBillStatusProvider.parse(new java.io.ByteArrayInputStream(
        xml.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
  }

  private static Map<String, Object> only(List<Map<String, Object>> rows) {
    assertEquals(1, rows.size());
    return rows.get(0);
  }

  @Test void legacyBillNumberAndBillTypeAreMappedToTheSameFieldsAsNumberAndType()
      throws Exception {
    // BILLSTATUS-117hr9.xml uses <billNumber>/<billType> where every other file uses
    // <number>/<type>.
    BillRecord rec = load("BILLSTATUS-117hr9.xml");
    assertEquals("HR", rec.bill.get("type"));
    Map<String, Object> bill = only(CongressBillStatusProvider.rows("bills", rec));
    assertEquals(117, bill.get("congress"));
    assertEquals(9, bill.get("bill_number"));
    // An empty constitutionalAuthorityStatementText element is an absent value.
    assertNull(bill.get("constitutional_authority_statement"));
  }

  @Test void titlesAreOneRowEach() throws Exception {
    List<Map<String, Object>> rows = rows("bill_titles", "BILLSTATUS-117hr9.xml");
    assertEquals(2, rows.size());
    assertEquals("Official Title as Introduced", rows.get(0).get("title_type"));
    assertEquals("Display Title", rows.get(1).get("title_type"));
    assertNull(rows.get(0).get("parent_title_type"));
  }

  @Test void billActionRecordedVoteCarriesTheActionItBelongsTo() throws Exception {
    Map<String, Object> vote = only(rows("bill_recorded_votes", "BILLSTATUS-118sjres117.xml"));
    assertEquals(295, vote.get("roll_number"));
    assertEquals("Senate", vote.get("vote_chamber"));
    assertEquals(2, vote.get("session_number"));
    assertEquals(118, vote.get("vote_congress"));
    assertEquals("2024-11-21T03:24:18Z", vote.get("vote_date"));
    assertEquals("2024-11-20", vote.get("action_date"));
    assertTrue(((String) vote.get("action_text")).startsWith("Motion to proceed"));
    assertNull(vote.get("amendment_type"));
    assertNull(vote.get("full_action_name"));
  }

  @Test void amendmentActionVotesCarryTheAmendment() throws Exception {
    List<Map<String, Object>> rows = rows("bill_recorded_votes", "BILLSTATUS-109hres151.xml");
    assertEquals(2, rows.size());
    for (Map<String, Object> vote : rows) {
      assertEquals("HAMDT", vote.get("amendment_type"));
      assertEquals(43, vote.get("amendment_number"));
      assertEquals(69, vote.get("roll_number"));
      assertEquals("House", vote.get("vote_chamber"));
      assertEquals("2005-03-15", vote.get("action_date"));
    }
  }

  @Test void billLevelRecordedVotesFromLegacyFilesHaveNoAction() throws Exception {
    // Excerpt of the bill-level recordedVotes block in BILLSTATUS-115hr3354.xml.
    BillRecord rec = parseXml("<billStatus><bill><number>3354</number><type>HR</type>"
        + "<congress>115</congress><recordedVotes>"
        + "<recordedVote><rollNumber>528</rollNumber>"
        + "<url>http://clerk.house.gov/evs/2017/roll528.xml</url>"
        + "<fullActionName>Passage of a Measure</fullActionName><chamber>House</chamber>"
        + "<congress>115</congress><date>2017-09-14T16:00:23Z</date>"
        + "<sessionNumber>1</sessionNumber></recordedVote>"
        + "<recordedVote><rollNumber>527</rollNumber>"
        + "<url>http://clerk.house.gov/evs/2017/roll527.xml</url>"
        + "<fullActionName>Motion to Commit/Recommit With Instructions Results</fullActionName>"
        + "<chamber>House</chamber><congress>115</congress><date>2017-09-14T15:53:37Z</date>"
        + "<sessionNumber>1</sessionNumber></recordedVote></recordedVotes></bill></billStatus>");
    List<Map<String, Object>> rows = CongressBillStatusProvider.rows("bill_recorded_votes", rec);
    assertEquals(2, rows.size());
    assertEquals(528, rows.get(0).get("roll_number"));
    assertEquals("Passage of a Measure", rows.get(0).get("full_action_name"));
    assertNull(rows.get(0).get("action_date"));
    assertNull(rows.get(0).get("action_text"));
    assertNull(rows.get(0).get("amendment_type"));
  }

  @Test void amendmentCosponsorsAndOnBehalfSponsors() throws Exception {
    Map<String, Object> cosponsor = only(rows("bill_amendment_cosponsors",
        "BILLSTATUS-119s3052.xml"));
    assertEquals("SAMDT", cosponsor.get("amendment_type"));
    assertEquals(3989, cosponsor.get("amendment_number"));
    assertEquals("K000377", cosponsor.get("bioguide_id"));
    assertEquals("2025-12-15", cosponsor.get("sponsorship_date"));
    assertEquals(Boolean.TRUE, cosponsor.get("is_original_cosponsor"));

    Map<String, Object> amendment = null;
    for (Map<String, Object> r : rows("bill_amendments", "BILLSTATUS-119sres178.xml")) {
      if (Integer.valueOf(2227).equals(r.get("amendment_number"))) {
        amendment = r;
      }
    }
    assertTrue(amendment != null, "SAMDT 2227 not found");
    assertEquals("T000250", amendment.get("on_behalf_of_bioguide_id"));
    assertEquals("Submitted on behalf of", amendment.get("on_behalf_of_type"));
    assertEquals("C001056", amendment.get("sponsor_bioguide_id"));
  }

  @Test void amendmentActionsAreRowsWithoutASequence() throws Exception {
    List<Map<String, Object>> rows = rows("bill_amendment_actions", "BILLSTATUS-109hres151.xml");
    assertTrue(rows.size() >= 2);
    assertEquals("HAMDT", rows.get(0).get("amendment_type"));
    assertFalse(rows.get(0).containsKey("action_seq"));
    assertTrue(rows.stream().anyMatch(
        r -> "Roll call votes on amendments in House".equals(r.get("text"))));
  }

  @Test void actionCommitteesUseTheSameSequenceAsBillActions() throws Exception {
    Map<String, Object> row = only(rows("bill_action_committees", "BILLSTATUS-119s5496.xml"));
    // Two actions listed newest-first; the referral is the newer, so seq 2.
    assertEquals(2, row.get("action_seq"));
    assertEquals("ssfi00", row.get("committee_system_code"));
    assertEquals("Finance Committee", row.get("committee_name"));
    Map<String, Object> action = rows("bill_actions", "BILLSTATUS-119s5496.xml").get(0);
    assertEquals(2, action.get("action_seq"));
    assertTrue(((String) action.get("text")).startsWith("Read twice and referred"));
  }

  @Test void calendarNumberIsOnTheActionAndReportsAreListed() throws Exception {
    List<Map<String, Object>> actions = rows("bill_actions", "BILLSTATUS-108hres829.xml");
    assertEquals(4, actions.size());
    // Listed newest-first: index 1 of 4 is seq 3.
    assertEquals("H00243", actions.get(1).get("calendar_number"));
    assertEquals(3, actions.get(1).get("action_seq"));
    assertNull(actions.get(0).get("calendar_number"));
    Map<String, Object> report = only(rows("bill_committee_reports", "BILLSTATUS-108hres829.xml"));
    assertEquals("H. Rept. 108-753", report.get("citation"));
  }

  @Test void costEstimatesAreOneRowEach() throws Exception {
    List<Map<String, Object>> rows = rows("bill_cbo_cost_estimates", "BILLSTATUS-111hr3111.xml");
    assertEquals(2, rows.size());
    assertEquals("2010-04-22T04:00:00Z", rows.get(0).get("pub_date"));
    assertTrue(((String) rows.get(0).get("title")).startsWith("H.R. 3111, Faster FOIA Act"));
    assertEquals("http://www.cbo.gov/publication/21424", rows.get(0).get("url"));
    assertTrue(((String) rows.get(0).get("description")).contains("Cost estimate"));
  }

  @Test void notesHaveOneRowPerLinkAndOneRowWhenThereIsNone() throws Exception {
    Map<String, Object> linked = only(rows("bill_notes", "BILLSTATUS-112s2608.xml"));
    assertTrue(((String) linked.get("note_text")).startsWith("For further action, see H.R.366"));
    assertEquals("H.R.366", linked.get("link_name"));
    assertEquals("https://www.congress.gov/bill/112th-congress/house-bill/366",
        linked.get("link_url"));
    Map<String, Object> unlinked = only(rows("bill_notes", "BILLSTATUS-119s3690.xml"));
    assertTrue(((String) unlinked.get("note_text")).startsWith("The text of"));
    assertNull(unlinked.get("link_name"));
    assertNull(unlinked.get("link_url"));
  }

  @Test void noteTextInCdataIsRead() throws Exception {
    // Verbatim excerpt of a note in BILLSTATUS-118hr5009.xml, the one file that spells it
    // notes/item/cdata/text.
    BillRecord rec = parseXml("<billStatus><bill><number>5009</number><type>HR</type>"
        + "<congress>118</congress><notes><item><cdata><text>The Joint Explanatory Statement and "
        + "text to accompany Public Law 118-159 are found within &lt;a href=\"https://www."
        + "congress.gov/committee-print/119th-congress/house-committee-print/58246?outputFormat="
        + "pdf\"&gt; Armed Services Committee Print No. 2&lt;/a&gt;</text></cdata></item></notes>"
        + "</bill></billStatus>");
    Map<String, Object> note = only(CongressBillStatusProvider.rows("bill_notes", rec));
    assertTrue(((String) note.get("note_text"))
        .startsWith("The Joint Explanatory Statement and text to accompany Public Law 118-159"));
  }

  @Test void summaryTextIsReadFromTextOrCdataText() throws Exception {
    Map<String, Object> viaCdata = only(rows("bill_summaries", "BILLSTATUS-118s358.xml"));
    assertEquals("00", viaCdata.get("version_code"));
    assertEquals("Introduced in Senate", viaCdata.get("action_description"));
    assertEquals("2023-02-09", viaCdata.get("action_date"));
    assertTrue(((String) viaCdata.get("summary_text")).startsWith("Provides for the relief of"));

    Map<String, Object> viaText = only(rows("bill_summaries", "BILLSTATUS-115s1826.xml"));
    assertEquals("Provides for the relief of Adrian Emin.", viaText.get("summary_text"));
  }

  @Test void legacyBillSummariesAreSummariesWithANameAndLastUpdate() throws Exception {
    List<Map<String, Object>> rows = rows("bill_summaries", "BILLSTATUS-113hr4200.xml");
    assertEquals(2, rows.size());
    assertEquals("00", rows.get(0).get("version_code"));
    assertEquals("Introduced in House", rows.get(0).get("summary_name"));
    assertEquals("2014-09-08T21:32:48Z", rows.get(0).get("last_summary_update_date"));
    assertEquals("81", rows.get(1).get("version_code"));
    assertTrue(((String) rows.get(0).get("summary_text")).contains("SBIC Advisers Relief"));
  }

  @Test void billLevelFieldsAddedLater() throws Exception {
    Map<String, Object> onBehalf = only(rows("bills", "BILLSTATUS-119s2844.xml"));
    assertEquals("L000577", onBehalf.get("sponsor_bioguide_id"));
    assertEquals("M001198", onBehalf.get("on_behalf_of_bioguide_id"));
    assertEquals("Introduced on behalf of", onBehalf.get("on_behalf_of_type"));

    Map<String, Object> cas = only(rows("bills", "BILLSTATUS-119hr1926.xml"));
    assertTrue(((String) cas.get("constitutional_authority_statement"))
        .startsWith("<pre>[Congressional Record Volume 171"));

    assertEquals(Boolean.TRUE, only(rows("bills", "BILLSTATUS-110s2091.xml"))
        .get("sponsor_is_by_request"));
    assertEquals(Boolean.FALSE, only(rows("bills", "BILLSTATUS-119hr100.xml"))
        .get("sponsor_is_by_request"));
  }

  @Test void relatedBillWithSeveralRelationshipsHasOneRowEach() throws Exception {
    List<Map<String, Object>> rows = rows("bill_related_bills", "BILLSTATUS-108s2455.xml");
    assertEquals(2, rows.size());
    assertEquals("hr", rows.get(0).get("related_bill_type"));
    assertEquals(4391, rows.get(0).get("related_bill_number"));
    assertEquals("Identical bill", rows.get(0).get("relationship_type"));
    assertEquals("Companion bill", rows.get(1).get("relationship_type"));
  }
}
