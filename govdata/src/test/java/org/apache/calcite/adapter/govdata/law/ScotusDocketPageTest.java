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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ScotusDocketPage}; the markup mirrors the docket page for No. 23-583.
 */
@Tag("unit")
class ScotusDocketPageTest {

  private static final String BASE =
      "https://www.supremecourt.gov/search.aspx?filename=/docket/docketfiles/html/public/23-583.html";

  private static String info(String... rows) {
    StringBuilder sb = new StringBuilder("<table id=\"docketinfo\"><tr><form id='temp'><tbody>"
        + "<tr><td class=\"InfoTitle\" colspan=\"2\"><span class=\"DocketInfoTitle\">No. 23-583 "
        + "<br /></span><span class=\"DocketInfoTitle\"><span><span> </span></span></span></td></tr>"
        + "<tr><td colspan=\"2\">&nbsp;</td></tr>");
    for (String r : rows) {
      sb.append(r);
    }
    return sb.append("<tr><td colspan=\"2\"><span><a href=\"../qp/23-00583qp.pdf\">Questions "
        + "Presented</a> </span></td></tr></tbody></table>").toString();
  }

  private static String row(String label, String value) {
    return "<tr><td width=\"200\"><span>" + label + "</span></td><td><span>" + value
        + "</span></td></tr>";
  }

  private static final String TITLE = "<tr><td width=\"200\"><span>Title:</span></td><td> "
      + "<span class=\"title\">Amina Bouarfa, Petitioner<br /> v. <br /> Alejandro Mayorkas, "
      + "Secretary of Homeland Security, et al. </span> </td></tr>";

  private static String entry(String date, String text, String links) {
    return "<div class=\"card\"><table class=\"ProceedingItem\"><tr><td class=\"ProceedingDate\">"
        + date + "</td><td>" + text + " <br /><span class=\"documentlinks\"> " + links
        + " </span></td></tr></table></div>";
  }

  private static String page(String infoTable, String... entries) {
    StringBuilder sb = new StringBuilder("<html><body>").append(infoTable)
        .append("<div id=\"proceedings\"><div class=\"coloredheader\">Proceedings and Orders</div>");
    for (String e : entries) {
      sb.append(e);
    }
    return sb.append("</div></body></html>").toString();
  }

  private static final String FULL_HEADER = info(TITLE, row("Docketed:", "November 30, 2023"),
      row("Linked with:", "23A348"),
      row("Lower Ct:", "United States Court of Appeals for the Eleventh Circuit"),
      row("&nbsp;&nbsp;&nbsp;Case Numbers:", "(22-12429)"),
      row("&nbsp;&nbsp;&nbsp;Decision Date:", "July 28, 2023 "));

  @Test void headerFields() {
    Docket d = ScotusDocketPage.parse(page(FULL_HEADER), BASE);
    assertEquals("23-583", d.number);
    assertEquals("Amina Bouarfa, Petitioner v. Alejandro Mayorkas, Secretary of Homeland "
        + "Security, et al.", d.title);
    assertEquals("2023-11-30", d.docketed);
    assertEquals("23A348", d.linkedWith);
    assertEquals("United States Court of Appeals for the Eleventh Circuit", d.lowerCourt);
    assertEquals("22-12429", d.lowerCourtCaseNumbers);
    assertEquals("2023-07-28", d.lowerCourtDecisionDate);
    assertNull(d.rehearingDenied);
  }

  @Test void questionsPresentedLinkIsMadeAbsolute() {
    Docket d = ScotusDocketPage.parse(page(FULL_HEADER), BASE);
    assertEquals("https://www.supremecourt.gov/qp/23-00583qp.pdf", d.questionsPresentedUrl);
  }

  @Test void optionalRowsThatAreAbsentAreNull() {
    Docket d = ScotusDocketPage.parse(page(info(TITLE, row("Docketed:", "July 3, 2024"))), BASE);
    assertNull(d.linkedWith);
    assertNull(d.lowerCourt);
    assertNull(d.lowerCourtCaseNumbers);
    assertNull(d.lowerCourtDecisionDate);
    assertEquals("2024-07-03", d.docketed);
  }

  @Test void rehearingDenied() {
    Docket d = ScotusDocketPage.parse(page(info(TITLE, row("Docketed:", "November 30, 2023"),
        row("Rehearing Denied:", "March 3, 2025"))), BASE);
    assertEquals("2025-03-03", d.rehearingDenied);
  }

  @Test void aDecisionDateWithARuleAnnotationKeepsTheDateAndTheNote() {
    Docket d = ScotusDocketPage.parse(page(info(TITLE, row("Docketed:", "January 2, 2017"),
        row("Decision Date:", "January 17, 2017 Rule 12.4"))), BASE);
    assertEquals("2017-01-17", d.lowerCourtDecisionDate);
    assertEquals("Rule 12.4", d.lowerCourtDecisionNote);
    assertNull(ScotusDocketPage.parse(page(FULL_HEADER), BASE).lowerCourtDecisionNote);
  }

  @Test void discretionaryCourtDecisionDate() {
    Docket d = ScotusDocketPage.parse(page(info(TITLE, row("Docketed:", "January 2, 2025"),
        row("Discretionary Court Decision Date:", "April 1, 2025"),
        row("Decision Date:", "March 3, 2025"))), BASE);
    assertEquals("2025-04-01", d.discretionaryCourtDecisionDate);
    assertEquals("2025-03-03", d.lowerCourtDecisionDate);
    assertNull(ScotusDocketPage.parse(page(FULL_HEADER), BASE).discretionaryCourtDecisionDate);
  }

  @Test void entriesKeepOrderDatesAndText() {
    Docket d = ScotusDocketPage.parse(page(FULL_HEADER,
        entry("Oct 13 2023", "Application (23A348) to extend the time.", ""),
        entry("Apr 29 2024", "Petition GRANTED.", ""),
        entry("Oct 15 2024", "Argued. For petitioner: Samir Deger-Sen, New York, N. Y.", "")),
        BASE);
    assertEquals(3, d.entries.size());
    Entry first = d.entries.get(0);
    assertEquals(1, first.sequence);
    assertEquals("2023-10-13", first.date);
    assertEquals("Application (23A348) to extend the time.", first.text);
    assertEquals(3, d.entries.get(2).sequence);
  }

  @Test void documentLinksAreCollectedAndKeptOutOfTheText() {
    String links = "<a href= https://www.supremecourt.gov/DocketPDF/23/23-583/291930/"
        + "20240501_Petition%20and%20%20Appendix.pdf class=\"documentanchor\" "
        + "target=\"_blank\">Petition</a>";
    Docket d = ScotusDocketPage.parse(page(FULL_HEADER,
        entry("Nov 27 2023", "Petition for a writ of certiorari filed. (Response due January 2, "
            + "2024)", links)), BASE);
    Entry e = d.entries.get(0);
    assertEquals("Petition for a writ of certiorari filed. (Response due January 2, 2024)",
        e.text);
    assertEquals(1, e.documents.size());
    assertEquals("Petition", e.documents.get(0).label);
    assertTrue(e.documents.get(0).url.endsWith("Petition%20and%20%20Appendix.pdf"),
        e.documents.get(0).url);
  }

  @Test void keyDatesAreReadFromTheEntries() {
    Docket d = ScotusDocketPage.parse(page(FULL_HEADER,
        entry("Nov 27 2023", "Petition for a writ of certiorari filed.", ""),
        entry("Apr 29 2024", "Petition GRANTED.", ""),
        entry("Oct 15 2024", "Argued. For petitioner: Samir Deger-Sen.", ""),
        entry("Jan 13 2025", "Judgment Issued.", "")), BASE);
    assertEquals("2024-04-29", d.grantedDate());
    assertEquals("2024-10-15", d.arguedDate());
    assertEquals("2025-01-13", d.judgmentIssuedDate());
  }

  @Test void everyWordingOfTheGrantIsRecognisedButAMotionGrantIsNot() {
    String[][] cases = {
        {"Petition GRANTED.", "2024-05-01"},
        {"Petition GRANTED limited to the following question: Whether a rule applies.", "2024-05-01"},
        {"Petition for a writ of certiorari before judgment GRANTED. The motion to expedite", "2024-05-01"},
        {"Petition for a writ of certiorari GRANTED.", "2024-05-01"},
        {"Motion to proceed in forma pauperis and petition for a writ of certiorari GRANTED.",
            "2024-05-01"},
        {"Motion for divided argument and for enlargement of time for oral argument GRANTED.", null},
        {"Motion to dispense with printing the joint appendix filed by the Solicitor General "
            + "GRANTED.", null},
        {"Motion of petitioners to expedite consideration of the petition for a writ of "
            + "certiorari before judgment DENIED.", null},
        {"Reply in support of motion to dismiss the writ as improvidently granted filed.", null},
    };
    for (String[] c : cases) {
      Docket d = ScotusDocketPage.parse(page(FULL_HEADER, entry("May 01 2024", c[0], "")), BASE);
      assertEquals(c[1], d.grantedDate(), c[0]);
    }
  }

  @Test void olderDocketsPrintJudgmentIssuedInCapitals() {
    Docket d = ScotusDocketPage.parse(page(FULL_HEADER,
        entry("Aug 03 2020", "MANDATE ISSUED.", ""),
        entry("Aug 03 2020", "JUDGMENT ISSUED.", "")), BASE);
    assertEquals("2020-08-03", d.judgmentIssuedDate());
  }

  @Test void aMisspeltJudgmentEntryIsNotGuessed() {
    Docket d = ScotusDocketPage.parse(page(FULL_HEADER,
        entry("Jul 30 2018", "JUDGMENT SSUED.", "")), BASE);
    assertNull(d.judgmentIssuedDate());
  }

  @Test void keyDatesAreNullWhenThereIsNoSuchEntry() {
    Docket d = ScotusDocketPage.parse(page(FULL_HEADER,
        entry("Nov 27 2023", "Petition for a writ of certiorari filed.", "")), BASE);
    assertNull(d.grantedDate());
    assertNull(d.arguedDate());
    assertNull(d.judgmentIssuedDate());
  }

  @Test void theGenericSearchPageIsNotADocket() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> ScotusDocketPage.parse("<html><body><div id=\"pagemaindiv\">Search - Supreme Court "
            + "of the United States</div></body></html>", BASE));
    assertTrue(e.getMessage().contains("Not a docket page"), e.getMessage());
  }

  @Test void anUnrecognisedHeaderRowIsAnErrorNotIgnored() {
    assertThrows(IllegalArgumentException.class, () -> ScotusDocketPage.parse(
        page(info(TITLE, row("Docketed:", "November 30, 2023"), row("Mystery:", "x"))), BASE));
  }

  @Test void aMalformedDateIsAnError() {
    assertThrows(IllegalArgumentException.class, () -> ScotusDocketPage.parse(
        page(info(TITLE, row("Docketed:", "sometime in 2023"))), BASE));
    assertThrows(IllegalArgumentException.class, () -> ScotusDocketPage.parse(
        page(FULL_HEADER, entry("13/45/2023", "x", "")), BASE));
  }
}
