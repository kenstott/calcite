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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cuts the cases out of a volume PDF (a preliminary print or a bound volume of the United
 * States Reports), where many opinions share one file.
 *
 * <p>A case begins at a page the caller identifies and runs until the next page that carries an
 * {@code OCTOBER TERM, YYYY} header. That header is on the first page of every case and of every
 * orders section, and on no continuation page (those carry the case name or {@code Cite as},
 * with the page number), so the first such header after a case's start ends it, whether it opens
 * the next opinion, an unlisted case, or the orders that follow the last opinion. Checked against
 * the listings on supremecourt.gov, the first header after each listed case is the next listed
 * case's start page, and the last case of a volume ends at its orders.
 *
 * <p>Pages before the first case, and pages between a case's end and the next start, belong to
 * no case and are dropped. Cases are emitted one at a time as the pages stream past, so only the
 * current case's pages are held.
 */
final class ScotusCasePages {

  /** How much of a page's opening text is searched for the header. */
  private static final int HEAD_CHARS = 100;

  private static final Pattern TERM_HEADER = Pattern.compile("OCTOBER TERM, \\d{4}");

  private static final Pattern WS = Pattern.compile("[\\s\\u00a0\\u2007\\u2009\\u202f]+");

  /** The pages of one case, first page first. */
  static final class CasePages {
    final int startPage;
    final List<String> pages;

    CasePages(int startPage) {
      this.startPage = startPage;
      this.pages = new ArrayList<String>();
    }
  }

  /** Decides whether a page opens a case. */
  interface StartRule {
    /**
     * @param pdfPage 1-based page number within the PDF
     * @param head the first characters of the page's text, whitespace collapsed
     */
    boolean startsCase(int pdfPage, String head);
  }

  /** Receives each case as soon as it is complete. */
  interface CaseSink {
    void accept(CasePages casePages) throws IOException;
  }

  private static final String VOLUME_BANNER = "CASES ADJUDGED IN THE SUPREME COURT";

  /** A case-opening page's header: the printed page and a heading that only opinions carry. */
  private static final Pattern OPENING_HEADING =
      Pattern.compile("Syllabus|Per Curiam|Opinion of|Opinion of the Court");

  /** {@code 486 OCTOBER TERM, 2018 Syllabus ...}: the printed page precedes the term header. */
  private static final Pattern PAGE_BEFORE_TERM =
      Pattern.compile("(?:^|\\s)(\\d+)\\s+OCTOBER TERM, \\d{4}");

  /** {@code OCTOBER TERM, 2018 485 Syllabus ...}: the printed page follows the term header. */
  private static final Pattern PAGE_AFTER_TERM =
      Pattern.compile("OCTOBER TERM, \\d{4}\\s+(\\d+)\\s");

  private ScotusCasePages() {
  }

  /**
   * Start rule for a bound volume, where the caller knows the printed United States Reports page
   * each wanted case begins on rather than its PDF page. A case begins on the page whose header
   * shows that printed page beside the term header and an opinion heading (Syllabus, Per Curiam);
   * the orders sections carry the term header too but no opinion heading. The volume's first case
   * has no such header and opens with the {@code CASES ADJUDGED IN THE SUPREME COURT} banner.
   */
  static PrintedPageRule printedPages(Set<Integer> citedPages) {
    return new PrintedPageRule(citedPages);
  }

  /** {@link StartRule} that also remembers which printed page each PDF page matched. */
  static final class PrintedPageRule implements StartRule {
    private final Set<Integer> citedPages;
    private final Map<Integer, Integer> citedByPdfPage = new HashMap<Integer, Integer>();

    private PrintedPageRule(Set<Integer> citedPages) {
      this.citedPages = citedPages;
    }

    @Override public boolean startsCase(int pdfPage, String head) {
      Integer cited = null;
      if (head.startsWith(VOLUME_BANNER)) {
        cited = 1;
      } else if (OPENING_HEADING.matcher(head).find()) {
        Matcher before = PAGE_BEFORE_TERM.matcher(head);
        Matcher after = PAGE_AFTER_TERM.matcher(head);
        if (before.find()) {
          cited = Integer.valueOf(before.group(1));
        } else if (after.find()) {
          cited = Integer.valueOf(after.group(1));
        }
      }
      if (cited != null && citedPages.contains(cited)) {
        citedByPdfPage.put(pdfPage, cited);
        return true;
      }
      return false;
    }

    /** The printed page the case starting at this PDF page was matched to, or null. */
    Integer citedPageAt(int pdfPage) {
      return citedByPdfPage.get(pdfPage);
    }
  }

  /**
   * A page sink for {@link PdfPageTexts#forEachPage} that emits cases to {@code cases}.
   * Call {@link Slicer#finish()} after the last page to emit the final case.
   */
  static Slicer slicer(StartRule rule, CaseSink cases) {
    return new Slicer(rule, cases);
  }

  /** Stateful page consumer; not thread-safe. */
  static final class Slicer implements PdfPageTexts.PageSink {
    private final StartRule rule;
    private final CaseSink cases;
    private CasePages current;

    private Slicer(StartRule rule, CaseSink cases) {
      this.rule = rule;
      this.cases = cases;
    }

    @Override public void page(int pageNumber, String text) throws IOException {
      String collapsed = WS.matcher(text).replaceAll(" ").trim();
      String head = collapsed.length() > HEAD_CHARS ? collapsed.substring(0, HEAD_CHARS)
          : collapsed;
      if (rule.startsCase(pageNumber, head)) {
        emit();
        current = new CasePages(pageNumber);
        current.pages.add(text);
      } else if (current != null) {
        if (TERM_HEADER.matcher(head).find()) {
          emit();
        } else {
          current.pages.add(text);
        }
      }
    }

    /** Emits the case in progress, if any; call once after the last page. */
    void finish() throws IOException {
      emit();
    }

    private void emit() throws IOException {
      if (current != null) {
        CasePages done = current;
        current = null;
        cases.accept(done);
      }
    }
  }
}
