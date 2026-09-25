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

import org.apache.calcite.adapter.govdata.law.ScotusSlipListing.Entry;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ScotusSlipListing}; the rows are copied from the live listings.
 */
@Tag("unit")
class ScotusSlipListingTest {

  private static final String HEADER_ROW =
      "<tr><td>R-</td><td>Date</td><td>Docket</td><td>Name</td><td>J.</td><td>Citation</td></tr>";

  private static final String CELL = "<td style=\"text-align: center;\">";

  private static String row(String release, String date, String docket, String href,
      String title, String name, String author, String citation) {
    return "<tr>" + CELL + release + "</td>" + CELL + date + "</td>"
        + "<td style=\"text-align: center; white-space: nowrap;\">" + docket + "</td>"
        + "<td><a href='" + href + "' target='_blank' title=\"" + title + "\">" + name + "</a></td>"
        + CELL + author + "</td>" + CELL + citation + "</td></tr>";
  }

  private static String page(String... rows) {
    StringBuilder sb = new StringBuilder("<html><body><table>");
    sb.append("<tr><td><input name=\"q\"/></td></tr>").append(HEADER_ROW);
    for (String r : rows) {
      sb.append(r);
    }
    return sb.append("</table></body></html>").toString();
  }

  private static final String PER_OPINION = row("67", "6/30/25", "24-809",
      "/opinions/24pdf/606us2r67_8nka.pdf",
      "The Fourth Circuit’s determination is reversed, and the case is remanded.",
      "Goldey v. Fields", "PC", "606 U.S. 942");

  private static final String INTO_VOLUME = row("76", "6/28/18", "17-1364",
      "/opinions/preliminaryprint/585US2PP_final.pdf#page=474",
      " The District Court’s order is affirmed in part.", "North Carolina v. Covington",
      "PC", "585 U.S. 969");

  @Test void perOpinionPdfRow() {
    List<Entry> entries = ScotusSlipListing.parse(page(PER_OPINION));
    assertEquals(1, entries.size());
    Entry e = entries.get(0);
    assertEquals("67", e.release);
    assertEquals("2025-06-30", e.decisionDate);
    assertEquals("24-809", e.docket);
    assertEquals("Goldey v. Fields", e.caseName);
    assertEquals("PC", e.authorInitials);
    assertEquals("606 U.S. 942", e.usCitation());
    assertEquals(Integer.valueOf(606), e.volume);
    assertEquals(Integer.valueOf(942), e.firstPage);
    assertEquals("/opinions/24pdf/606us2r67_8nka.pdf", e.pdfPath());
    assertNull(e.pdfPage);
  }

  @Test void holdingSummaryIsTheLinkTitleTrimmed() {
    Entry e = ScotusSlipListing.parse(page(INTO_VOLUME)).get(0);
    assertEquals("The District Court’s order is affirmed in part.", e.holdingSummary);
  }

  @Test void rowLinkedIntoAVolumePdfCarriesItsPage() {
    Entry e = ScotusSlipListing.parse(page(INTO_VOLUME)).get(0);
    assertEquals(Integer.valueOf(474), e.pdfPage);
    assertEquals("/opinions/preliminaryprint/585US2PP_final.pdf", e.pdfPath());
    assertEquals("2018-06-28", e.decisionDate);
  }

  @Test void entriesKeepThePagesOrderAndHeaderAndSearchRowsAreSkipped() {
    List<Entry> entries = ScotusSlipListing.parse(page(PER_OPINION, INTO_VOLUME));
    assertEquals(2, entries.size());
    assertEquals("67", entries.get(0).release);
    assertEquals("76", entries.get(1).release);
  }

  @Test void applicationAndOriginalDocketsAreKept() {
    List<Entry> entries = ScotusSlipListing.parse(page(
        row("66", "6/27/25", "24A884", "/opinions/24pdf/606us2r66_j426.pdf", "t", "Trump v. CASA",
            "B", "606 U.S. 831"),
        row("3", "1/2/19", "137, Orig.", "/opinions/18pdf/586us1r03_ab12.pdf", "t",
            "Texas v. New Mexico", "PC", "586 U.S. 1")));
    assertEquals("24A884", entries.get(0).docket);
    assertEquals("137, Orig.", entries.get(1).docket);
  }

  @Test void citationSpacingVariantsAreNormalised() {
    List<Entry> entries = ScotusSlipListing.parse(page(
        row("1", "1/2/19", "18-1", "/opinions/18pdf/586us1r01_a.pdf", "t", "A v. B", "PC",
            "586 U.S.1"),
        row("2", "1/3/19", "18-2", "/opinions/18pdf/586us1r02_b.pdf", "t", "C v. D", "PC",
            "586 U. S. 20")));
    assertEquals("586 U.S. 1", entries.get(0).usCitation());
    assertEquals("586 U.S. 20", entries.get(1).usCitation());
  }

  @Test void aMissingPeriodAfterUSIsAccepted() {
    Entry e = ScotusSlipListing.parse(page(row("35", "5/21/26", "24-872",
        "/opinions/25pdf/608us2r35_a.pdf", "t", "Hamm v. Smith", "PC", "608 U.S 278"))).get(0);
    assertEquals("608 U.S. 278", e.usCitation());
  }

  @Test void aVolumeAndPrintPartCitationHasAVolumeAndNoPage() {
    Entry e = ScotusSlipListing.parse(page(row("72", "9/25/26", "26A308",
        "/opinions/25pdf/609us2r72_a.pdf", "t", "DHS v. League of Women Voters", "PC",
        "609/2"))).get(0);
    assertEquals(Integer.valueOf(609), e.volume);
    assertNull(e.firstPage);
    assertNull(e.usCitation());
  }

  @Test void aBlankCitationIsNullNotInvented() {
    Entry e = ScotusSlipListing.parse(page(row("1", "1/2/19", "18-1",
        "/opinions/18pdf/586us1r01_a.pdf", "t", "A v. B", "PC", ""))).get(0);
    assertNull(e.volume);
    assertNull(e.usCitation());
  }

  @Test void aDecreeIsLabelledNotNumbered() {
    Entry e = ScotusSlipListing.parse(page(row("D1", "5/26/26", "141, Orig.",
        "/opinions/25pdf/608us1r36d_21o3.pdf", "t", "Texas v. New Mexico", "D",
        "608 U.S. 346"))).get(0);
    assertEquals("D1", e.release);
    assertEquals("141, Orig.", e.docket);
  }

  @Test void aMissingSummaryIsNull() {
    Entry e = ScotusSlipListing.parse(page(row("1", "1/2/19", "18-1",
        "/opinions/18pdf/586us1r01_a.pdf", "", "A v. B", "PC", "586 U.S. 1"))).get(0);
    assertNull(e.holdingSummary);
  }

  @Test void aMissingReleaseNumberIsAnError() {
    assertThrows(IllegalArgumentException.class, () -> ScotusSlipListing.parse(page(
        row("", "1/2/19", "18-1", "/opinions/18pdf/a.pdf", "t", "A v. B", "PC", "586 U.S. 1"))));
  }

  @Test void aRowWithTheWrongCellCountIsAnErrorNotSkipped() {
    String bad = "<tr><td>1</td><td>1/2/19</td><td><a href='/opinions/18pdf/x.pdf'>A v. B</a></td>"
        + "</tr>";
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> ScotusSlipListing.parse(page(bad)));
    assertTrue(e.getMessage().contains("cells"), e.getMessage());
  }

  @Test void aMalformedDateOrCitationIsAnError() {
    assertThrows(IllegalArgumentException.class, () -> ScotusSlipListing.parse(page(
        row("1", "January 2", "18-1", "/opinions/18pdf/a.pdf", "t", "A v. B", "PC", "586 U.S. 1"))));
    assertThrows(IllegalArgumentException.class, () -> ScotusSlipListing.parse(page(
        row("1", "1/2/19", "18-1", "/opinions/18pdf/a.pdf", "t", "A v. B", "PC", "not a cite"))));
  }
}
