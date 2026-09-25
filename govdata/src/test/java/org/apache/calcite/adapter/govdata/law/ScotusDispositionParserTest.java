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

import org.apache.calcite.adapter.govdata.law.ScotusDispositionParser.Kind;
import org.apache.calcite.adapter.govdata.law.ScotusDispositionParser.Result;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link ScotusDispositionParser}. The page texts are excerpts of real Supreme
 * Court opinion PDFs as extracted by PDFBox, including the dropped "i" of the "fi" ligature
 * ("affrmed").
 */
@Tag("unit")
class ScotusDispositionParserTest {

  private static Result parse(String... pages) {
    return ScotusDispositionParser.parse(Arrays.asList(pages));
  }

  @Test void syllabusLineAfterReporterCitation() {
    Result r = parse("cover page",
        "Syllabus BOUARFA v. MAYORKAS certiorari to the united states court of appeals "
            + "for the eleventh circuit No. 23–583. Argued October 15, 2024—Decided "
            + "December 10, 2024 The Eleventh Circuit affrmed. Held: Revocation is not "
            + "reviewable. Pp. 7–14.\n75 F. 4th 1157, affrmed.\nJackson, J., delivered the "
            + "opinion for a unanimous Court.");
    assertEquals(Kind.SYLLABUS_LINE, r.kind);
    assertEquals("Affirmed (includes modified)", r.disposition);
  }

  @Test void narrativeSentenceBeforeTheDispositionLineIsNotTheDisposition() {
    Result r = parse("cover",
        "The Tax Court vacated the dismissal, holding that the levy was moot. The Third "
            + "Circuit reversed. Held: The Tax Court lacks jurisdiction. Pp. 4–10.\n"
            + "97 F. 4th 81, reversed and remanded.\nGorsuch, J., delivered the opinion of "
            + "the Court.");
    assertEquals("Reversed and remanded", r.disposition);
  }

  @Test void certiorariGrantedPrefixIsNotPartOfTheDisposition() {
    Result r = parse("cover",
        "Per Curiam GOLDEY v. FIELDS No. 24–809. Decided June 30, 2025 Certiorari "
            + "granted; 109 F. 4th 264, reversed and remanded.\nPER CURIAM. The petition is "
            + "granted.");
    assertEquals("Reversed and remanded", r.disposition);
  }

  @Test void dispositionLineWrappedAcrossTwoLines() {
    Result r = parse("cover",
        "Held: The order is unlawful. Pp. 3–9.\nCertiorari and temporary injunctive relief "
            + "granted; judgment vacated\nand remanded.\nSotomayor, J., delivered the opinion.");
    assertEquals("Vacated and remanded", r.disposition);
  }

  @Test void wordBrokenByAHyphenAtALineEnd() {
    Result r = parse("cover",
        "Pp. 5–8. 88 F. 4th 1010, vacated and re-\nmanded.\nKagan, J., delivered the opinion.");
    assertEquals("Vacated and remanded", r.disposition);
  }

  @Test void consolidatedCaseReturnsTheFirstClause() {
    Result r = parse(
        "Pp. 477–479. No. 14–1468, 2015 ND 6, 858 N. W. 2d 302, reversed and remanded; "
            + "No. 14–1470, 859 N. W. 2d 762, affrmed; No. 14–1507, 2015 MN 1, vacated "
            + "and remanded.\nAlito, J., delivered the opinion of the Court.");
    assertEquals(Kind.SYLLABUS_LINE, r.kind);
    assertEquals("Reversed and remanded", r.disposition);
  }

  @Test void affirmedInPartVacatedInPartAndRemandedUsesTheMixedVocabulary() {
    Result r = parse("cover",
        "Pp. 413–414. 554 F. 3d 529, affrmed in part, vacated in part, and remanded.\n"
            + "Ginsburg, J., delivered the opinion of the Court.");
    assertEquals(Kind.SYLLABUS_LINE, r.kind);
    assertEquals("Affirmed and reversed (or vacated) in part and remanded", r.disposition);
  }

  @Test void reversedInPartAffirmedInPartIsTheSameMixedOutcome() {
    Result r = parse("cover",
        "Pp. 309–313. 546 F. 3d 1169, reversed in part, affrmed in part, and remanded.\n"
            + "Thomas, J., delivered the opinion of the Court.");
    assertEquals("Affirmed and reversed (or vacated) in part and remanded", r.disposition);
  }

  @Test void mixedOutcomeWithoutARemand() {
    Result r = parse("cover",
        "Pp. 20–25. Affrmed in part and reversed in part.\n"
            + "Kagan, J., delivered the opinion of the Court.");
    assertEquals("Affirmed and reversed (or vacated) in part", r.disposition);
  }

  @Test void affirmedAndRemandedIsFoldedIntoAffirmedAsModsDoes() {
    Result r = parse("cover",
        "Pp. 386. 830 F. 3d 690, affrmed and remanded.\nSotomayor, J., delivered the opinion "
            + "for a unanimous Court.");
    assertEquals("Affirmed (includes modified)", r.disposition);
  }

  @Test void equallyDividedCourt() {
    Result r = parse(
        "No. 13–1496. Argued December 7, 2015—Decided June 23, 2016 746 F. 3d 167, "
            + "affrmed by an equally divided Court.\nPer Curiam. The judgment is affrmed by an "
            + "equally divided Court.");
    assertEquals("Affirmed (includes modified)", r.disposition);
    assertEquals(ScotusDispositionParser.EQUALLY_DIVIDED_VOTE, r.decisionType);
  }

  @Test void dismissedAsImprovidentlyGranted() {
    Result r = parse("cover",
        "Certiorari dismissed. Reported below: 87 F. 4th 934. Per Curiam. The writ of "
            + "certiorari is dismissed as improvidently granted. It is so ordered.");
    assertEquals(Kind.DISMISSED_IMPROVIDENTLY, r.kind);
    assertEquals("Dismissed as improvidently granted", r.disposition);
  }

  @Test void applicationGranted() {
    Result r = parse("cover",
        "No. 24A931. Decided April 7, 2025 The application to vacate the orders presented "
            + "to The Chief Justice and by him referred to the Court is granted. The March "
            + "minute orders are vacated. It is so ordered.");
    assertEquals(Kind.APPLICATION, r.kind);
    assertEquals("Application granted", r.disposition);
  }

  @Test void applicationDeniedUsesTheLastRulingBeforeTheOrder() {
    Result r = parse("cover",
        "No. 24A123. Decided May 1, 2025 The stay is granted as to the first order. The "
            + "application is denied. It is so ordered. Justice Alito, dissenting. The "
            + "application should be granted.");
    assertEquals("Application denied", r.disposition);
  }

  @Test void applicationThatAlsoGrantedCertiorariTakesTheSyllabusLine() {
    Result r = parse("cover",
        "No. 24A1007. Decided May 16, 2025 Pp. 4–9.\nCertiorari and temporary injunctive "
            + "relief granted; judgment vacated and remanded.\nPer Curiam. The application is "
            + "granted. It is so ordered.");
    assertEquals(Kind.SYLLABUS_LINE, r.kind);
    assertEquals("Vacated and remanded", r.disposition);
  }

  @Test void noDispositionIsNullNeverADefault() {
    Result r = parse("cover",
        "No. 22O141, Original. Argued February 2025—Decided 2025 The Special Master's "
            + "report is received. Barrett, J., delivered the opinion of the Court.");
    assertEquals(Kind.NONE, r.kind);
    assertNull(r.disposition);
  }

  @Test void onlyTheFirstTwelvePagesAreRead() {
    List<String> pages = new ArrayList<String>(Collections.nCopies(14, "filler text"));
    pages.set(13, "Pp. 1–2. 1 F. 4th 1, reversed.\nRoberts, C. J., delivered the opinion.");
    assertEquals(Kind.NONE, ScotusDispositionParser.parse(pages).kind);
    pages.set(5, "Pp. 1–2. 1 F. 4th 1, reversed.\nRoberts, C. J., delivered the opinion.");
    assertEquals("Reversed", ScotusDispositionParser.parse(pages).disposition);
  }
}
