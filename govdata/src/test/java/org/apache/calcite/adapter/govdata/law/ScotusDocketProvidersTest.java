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

import org.apache.calcite.adapter.govdata.law.ScotusDocketPage.Docket;
import org.apache.calcite.adapter.govdata.law.ScotusDocketPage.Entry;
import org.apache.calcite.adapter.govdata.law.ScotusDocketPage.Filing;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ScotusDocketFetcher} and the row mapping of the two docket providers.
 */
@Tag("unit")
class ScotusDocketProvidersTest {

  private static ScotusSlipListing.Entry listed(String docket) {
    return new ScotusSlipListing.Entry("1", "2025-01-01", docket, "A v. B", null, "PC", null, null,
        "/opinions/24pdf/x.pdf", null);
  }

  @Test void regularAndApplicationDocketsAreFetchable() {
    assertTrue(ScotusDocketFetcher.fetchable("23-583"));
    assertTrue(ScotusDocketFetcher.fetchable("24A910"));
    assertTrue(ScotusDocketFetcher.fetchable("22-1"));
  }

  @Test void anOriginalJurisdictionDocketIsNotFetchable() {
    assertFalse(ScotusDocketFetcher.fetchable("141, Orig."));
    assertFalse(ScotusDocketFetcher.fetchable("141-Orig"));
    assertFalse(ScotusDocketFetcher.fetchable(""));
  }

  @Test void pageUrlIsLowerCased() {
    assertEquals("https://www.supremecourt.gov/search.aspx?filename=/docket/docketfiles/html/"
        + "public/24a910.html", ScotusDocketFetcher.pageUrl("24A910"));
    assertEquals("https://www.supremecourt.gov/search.aspx?filename=/docket/docketfiles/html/"
        + "public/23-583.html", ScotusDocketFetcher.pageUrl("23-583"));
  }

  @Test void docketNumbersAreDistinctInListingOrderAndSkipOriginals() {
    List<String> numbers = ScotusDocketFetcher.docketNumbers(Arrays.asList(
        listed("24-809"), listed("141, Orig."), listed("24A884"), listed("24-809"),
        listed("23-1122")));
    assertEquals(Arrays.asList("24-809", "24A884", "23-1122"), numbers);
  }

  private static Docket docket(String... entryTexts) {
    List<Entry> entries = new ArrayList<Entry>();
    int seq = 0;
    for (String text : entryTexts) {
      seq++;
      List<Filing> filings = text.startsWith("Petition for")
          ? Arrays.asList(new Filing("Petition", "https://x/p.pdf"),
              new Filing("Appendix", "https://x/a.pdf"))
          : Collections.<Filing>emptyList();
      entries.add(new Entry(seq, "2024-0" + seq + "-01", text, filings));
    }
    return new Docket("23-583", "A v. B", "2023-11-30", null, "Eleventh Circuit", "22-12429",
        "2023-07-28", null, null, null, "https://x/qp.pdf", entries);
  }

  @Test void docketRowCarriesTheHeaderAndCountsEntriesAndDocuments() {
    Map<String, Object> row = ScotusDocketsProvider.row(
        docket("Petition for a writ of certiorari filed.", "Petition GRANTED.", "Argued.",
            "Judgment Issued."));
    assertEquals("23-583", row.get("docket_number"));
    assertEquals("2023-11-30", row.get("docketed"));
    assertEquals("Eleventh Circuit", row.get("lower_court"));
    assertEquals("22-12429", row.get("lower_court_case_numbers"));
    assertEquals(Integer.valueOf(4), row.get("entry_count"));
    assertEquals(Integer.valueOf(2), row.get("document_count"));
    assertEquals("2024-02-01", row.get("granted_date"));
    assertEquals("2024-03-01", row.get("argued_date"));
    assertEquals("2024-04-01", row.get("judgment_issued_date"));
  }

  @Test void absentHeaderFieldsAreNotInTheRow() {
    Map<String, Object> row = ScotusDocketsProvider.row(docket("Petition for a writ."));
    assertFalse(row.containsKey("linked_with"));
    assertFalse(row.containsKey("rehearing_denied"));
    assertFalse(row.containsKey("granted_date"));
    assertFalse(row.containsKey("argued_date"));
  }

  @Test void oneEntryRowPerEntryInOrderWithItsDocket() {
    List<Map<String, Object>> rows = ScotusDocketEntriesProvider.rows(
        docket("Petition for a writ of certiorari filed.", "Petition GRANTED."));
    assertEquals(2, rows.size());
    assertEquals("23-583", rows.get(0).get("docket_number"));
    assertEquals(Integer.valueOf(1), rows.get(0).get("sequence"));
    assertEquals(Integer.valueOf(2), rows.get(1).get("sequence"));
    assertEquals("2024-01-01", rows.get(0).get("entry_date"));
    assertEquals("Petition GRANTED.", rows.get(1).get("entry_text"));
  }

  @Test void documentLabelsAndUrlsAreParallelListsAndAbsentWhenThereAreNone() {
    List<Map<String, Object>> rows = ScotusDocketEntriesProvider.rows(
        docket("Petition for a writ of certiorari filed.", "Petition GRANTED."));
    assertEquals(Arrays.asList("Petition", "Appendix"), rows.get(0).get("document_labels"));
    assertEquals(Arrays.asList("https://x/p.pdf", "https://x/a.pdf"),
        rows.get(0).get("document_urls"));
    assertFalse(rows.get(1).containsKey("document_labels"));
    assertFalse(rows.get(1).containsKey("document_urls"));
  }
}
