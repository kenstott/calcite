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

import org.apache.calcite.adapter.govdata.law.ScotusCasePages.CasePages;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ScotusCasePages}; page texts mirror the headers of real volume PDFs.
 */
@Tag("unit")
class ScotusCasePagesTest {

  private static final String START = "OCTOBER TERM, 2018 9 Syllabus WEYERHAEUSER CO. v. UNITED "
      + "STATES FISH AND WILDLIFE SERVICE certiorari to the united states court";
  private static final String BODY_EVEN = "10 WEYERHAEUSER CO. v. UNITED STATES FISH AND "
      + "WILDLIFE SERV . Syllabus Held: 1. An area is eligible";
  private static final String BODY_ODD = "Cite as: 586 U. S. 9 (2018) 11 Syllabus ately consider "
      + "all the relevant statutory factors";
  private static final String ORDERS = "486 OCTOBER TERM, 2018 ORDERS 586 U. S. Oct 1, 2018";

  private static List<CasePages> run(Set<Integer> starts, String... pages) throws IOException {
    final List<CasePages> out = new ArrayList<CasePages>();
    final Set<Integer> s = starts;
    ScotusCasePages.Slicer slicer = ScotusCasePages.slicer(
        (pdfPage, head) -> s.contains(pdfPage), out::add);
    for (int i = 0; i < pages.length; i++) {
      slicer.page(i + 1, pages[i]);
    }
    slicer.finish();
    return out;
  }

  private static Set<Integer> set(Integer... v) {
    return new HashSet<Integer>(Arrays.asList(v));
  }

  @Test void aCaseRunsUntilTheNextTermHeader() throws IOException {
    List<CasePages> cases = run(set(2, 6),
        "front matter", START, BODY_EVEN, BODY_ODD, "a continuation page", START, BODY_EVEN);
    assertEquals(2, cases.size());
    assertEquals(2, cases.get(0).startPage);
    assertEquals(4, cases.get(0).pages.size());
    assertEquals(6, cases.get(1).startPage);
    assertEquals(2, cases.get(1).pages.size());
  }

  @Test void thePageBeforeTheNextHeaderBelongsToTheEarlierCase() throws IOException {
    List<CasePages> cases = run(set(1, 4), START, BODY_EVEN, BODY_ODD, START, BODY_EVEN);
    assertEquals(3, cases.get(0).pages.size());
    assertEquals(BODY_ODD, cases.get(0).pages.get(2));
    assertEquals(START, cases.get(1).pages.get(0));
  }

  @Test void ordersEndTheLastCaseAndAreNotPartOfIt() throws IOException {
    List<CasePages> cases = run(set(1), START, BODY_EVEN, BODY_ODD, ORDERS, ORDERS);
    assertEquals(1, cases.size());
    assertEquals(3, cases.get(0).pages.size());
  }

  @Test void anUnlistedCaseEndsTheListedOneBeforeItAndIsDropped() throws IOException {
    // Page 4 opens a case the caller did not ask for; it must end case 1 and not be emitted.
    List<CasePages> cases = run(set(1, 7),
        START, BODY_EVEN, BODY_ODD, START, BODY_EVEN, BODY_ODD, START, BODY_EVEN);
    assertEquals(2, cases.size());
    assertEquals(3, cases.get(0).pages.size());
    assertEquals(1, cases.get(0).startPage);
    assertEquals(7, cases.get(1).startPage);
  }

  @Test void theFinalCaseRunsToTheEndOfADocumentWithNoOrders() throws IOException {
    List<CasePages> cases = run(set(1), START, BODY_EVEN, BODY_ODD);
    assertEquals(3, cases.get(0).pages.size());
  }

  @Test void noMatchingStartEmitsNothing() throws IOException {
    assertTrue(run(set(99), START, BODY_EVEN).isEmpty());
  }

  @Test void aCaseIsEmittedBeforeTheNextPageIsRead() throws IOException {
    final List<Integer> emittedAtPage = new ArrayList<Integer>();
    final int[] pageNow = new int[1];
    ScotusCasePages.Slicer slicer = ScotusCasePages.slicer(
        (p, head) -> p == 1 || p == 4,
        c -> emittedAtPage.add(pageNow[0]));
    String[] pages = {START, BODY_EVEN, BODY_ODD, START, BODY_EVEN};
    for (int i = 0; i < pages.length; i++) {
      pageNow[0] = i + 1;
      slicer.page(i + 1, pages[i]);
    }
    assertEquals(Arrays.asList(4), emittedAtPage, "case 1 must be released when case 2 starts");
    slicer.finish();
    assertEquals(Arrays.asList(4, 5), emittedAtPage);
  }

  // ---- printed-page start rule for bound volumes; the heads are copied from 586BV.pdf ----

  private static final String FIRST_CASE = "CASES ADJUDGED IN THE SUPREME COURT OF THE UNITED "
      + "STATES AT OCTOBER TERM, 2018 MOUNT LEMMON FIRE DISTRICT v. GUIDO et al";
  private static final String ODD_START = "OCTOBER TERM, 2018 485 Syllabus FRANK et al. v. GAOS, "
      + "individually and on behalf of all ot";
  private static final String EVEN_START = "486 OCTOBER TERM, 2018 Syllabus SOME v. CASE certiorari "
      + "to the united states court";
  private static final String PER_CURIAM = "OCTOBER TERM, 2018 12 Per Curiam FRANK v. GAOS Per "
      + "Curiam. Three named plaintiffs";
  private static final String BV_ORDERS = "486 OCTOBER TERM, 2018 ORDERS 586 U. S. Oct 1, 2018";
  private static final String BV_CONTINUATION = "Cite as: 586 U. S. 485 (2019) 487 Counsel son, "
      + "Brian D. Netter";

  @Test void printedPageRuleFindsTheCaseOnItsCitedPage() {
    ScotusCasePages.PrintedPageRule rule = ScotusCasePages.printedPages(set(485, 486, 12, 1));
    assertTrue(rule.startsCase(684, ODD_START));
    assertTrue(rule.startsCase(700, EVEN_START));
    assertTrue(rule.startsCase(30, PER_CURIAM));
    assertEquals(Integer.valueOf(485), rule.citedPageAt(684));
    assertEquals(Integer.valueOf(486), rule.citedPageAt(700));
  }

  @Test void theVolumesFirstCaseIsFoundByItsBanner() {
    ScotusCasePages.PrintedPageRule rule = ScotusCasePages.printedPages(set(1));
    assertTrue(rule.startsCase(200, FIRST_CASE));
    assertEquals(Integer.valueOf(1), rule.citedPageAt(200));
  }

  @Test void aPageWeDidNotAskForIsNotAStart() {
    ScotusCasePages.PrintedPageRule rule = ScotusCasePages.printedPages(set(999));
    assertTrue(!rule.startsCase(684, ODD_START));
    assertTrue(!rule.startsCase(200, FIRST_CASE));
  }

  @Test void ordersAndContinuationPagesAreNeverStarts() {
    ScotusCasePages.PrintedPageRule rule = ScotusCasePages.printedPages(set(486, 485, 487));
    assertTrue(!rule.startsCase(1000, BV_ORDERS));
    assertTrue(!rule.startsCase(686, BV_CONTINUATION));
  }
}
