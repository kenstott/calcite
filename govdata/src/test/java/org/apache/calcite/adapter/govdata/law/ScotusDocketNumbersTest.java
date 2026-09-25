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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ScotusDocketNumbers}; the header text is copied from real opinions.
 */
@Tag("unit")
class ScotusDocketNumbersTest {

  private static List<String> parse(String... pages) {
    return ScotusDocketNumbers.parse(Arrays.asList(pages));
  }

  @Test void singleDocket() {
    assertEquals(Collections.singletonList("23-583"),
        parse("cover", "Syllabus BOUARFA v. MAYORKAS certiorari to the united states court of "
            + "appeals for the eleventh circuit No. 23–583. Argued October 15, "
            + "2024—Decided December 10, 2024 Amina Bouarfa began"));
  }

  @Test void perCuriamWithNoArgument() {
    assertEquals(Collections.singletonList("24-809"),
        parse("Per Curiam GOLDEY v. FIELDS on petition for writ of certiorari to the united "
            + "states court of appeals for the fourth circuit No. 24–809. Decided June 30, "
            + "2025 Prison officials"));
  }

  @Test void application() {
    assertEquals(Collections.singletonList("24A910"),
        parse("cover", "on application to vacate the order No. 24A910. Decided April 4, 2025 "
            + "The District Court"));
  }

  @Test void applicationLinkedToAPetitionReturnsBothNumbers() {
    assertEquals(Arrays.asList("13A1003", "13-854"),
        parse("cover", "on application for stay No. 13A1003 (13–854). Decided March 3, 2014 "
            + "text"));
  }

  @Test void originalJurisdiction() {
    assertEquals(Collections.singletonList("137-Orig"),
        parse("cover", "STATE v. STATE No. 137, Orig. Decided June 1, 2015 The Special Master"));
  }

  @Test void consolidatedCaseTakesTheFootnoteDockets() {
    assertEquals(Arrays.asList("24-656", "24-657"),
        parse("cover", "TIKTOK INC. v. GARLAND certiorari to the united states court of "
            + "appeals for the district of columbia circuit No. 24–656. Argued January 10, "
            + "2025—Decided January 17, 2025* *Together with No. 24–657, Firebaugh "
            + "et al. v. Garland, Attorney Gen- eral, also on certiorari to the same court. "
            + "Page Proof Pending Publication Cite as: 604 U. S. 56 (2025) 57 Syllabus"));
  }

  @Test void companionWithoutANumberContributesNone() {
    assertEquals(Collections.singletonList("23-713"),
        parse("cover", "BUFKIN v. COLLINS No. 23–713. Argued October 16, 2024—Decided "
            + "March 5, 2025* *Together with Thornton v. Collins, Secretary of Veterans Affairs "
            + "(see this Court's Rule 12.4), also on certiorari to the same court. Page Proof"));
  }

  @Test void severalCompanionsInOneFootnote() {
    assertEquals(Arrays.asList("15-1", "15-2", "15-3"),
        parse("cover", "A v. B No. 15–1. Argued October 1, 2015—Decided May 2, 2016* "
            + "*Together with No. 15–2, C v. D, and No. 15–3, E v. F, also on "
            + "certiorari to the same court. Syllabus"));
  }

  @Test void aStarThatIsNotAConsolidationFootnoteAddsNothing() {
    assertEquals(Collections.singletonList("23-1275"),
        parse("cover", "No. 23–1275. Argued April 2, 2025—Decided June 26, 2025* "
            + "*Justice Gorsuch took no part in the decision. Congress created Medicaid"));
  }

  @Test void anOpinionAsFirstReleasedHasNoDatesAfterTheNumber() {
    assertEquals(Collections.singletonList("26A308"),
        parse("_________________ 1 Cite as: 609 U. S. ____ (2026) Per Curiam SUPREME COURT OF "
            + "THE UNITED STATES No. 26A308 DEPARTMENT OF HOMELAND SECURITY, ET AL. v. LEAGUE OF "
            + "WOMEN VOTERS, ET AL. ON APPLICATION FOR STAY [September 25, 2026] PER CURIAM."));
  }

  @Test void aReleasedOpinionOfAConsolidatedCaseListsEveryNumber() {
    assertEquals(Arrays.asList("24-656", "24-657"),
        parse("SUPREME COURT OF THE UNITED STATES Nos. 24\u2013656 and 24\u2013657 A v. B ON "
            + "WRITS OF CERTIORARI [January 17, 2025]"));
  }

  @Test void aFinalDecreeInAnOriginalCase() {
    assertEquals(Collections.singletonList("141-Orig"),
        parse("346 OCTOBER TERM, 2025 Decree TEXAS v. NEW MEXICO, et al. on receipt of the fourth "
            + "interim report of the special master No. 141, Orig. Final Decree Entered May 26, "
            + "2026 Opinions reported: 583 U. S. 407"));
  }

  @Test void noHeaderMeansNoNumbersNotAGuess() {
    assertTrue(parse("cover", "Errata: p. 12, line 3: \"No. 5\" is changed to \"No. 6\"").isEmpty());
  }
}
