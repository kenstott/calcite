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

import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.File;
import java.io.IOException;

/**
 * Streams the text of a PDF one page at a time.
 *
 * <p>The document is parsed with PDFBox's temp-file-backed buffers rather than the heap, and the
 * text of each page is handed to the caller as it is extracted and then dropped, so the cost is
 * one page of text however long the opinion is. A caller that builds a row from the whole text
 * holds that row, and nothing else.
 */
final class PdfPageTexts {

  /** Receives the extracted text of each page, in page order. */
  interface PageSink {
    /** @param pageNumber 1-based page number */
    void page(int pageNumber, String text) throws IOException;
  }

  /**
   * Word-gap tolerances for a page whose glyphs are set with wide letter-spacing. GovInfo's
   * volume 567 is typeset that way, and PDFBox's defaults read every gap as a space ("N o. 1 1
   * – 2 0 4"); these values close the gaps. They are not used elsewhere, because on a normally
   * spaced page they start to merge words.
   */
  private static final float LOOSE_SPACING_TOLERANCE = 2.5f;
  private static final float LOOSE_AVERAGE_CHAR_TOLERANCE = 2.0f;

  /** A page with at least this many tokens is judged on the share that are one character. */
  private static final int MIN_TOKENS_TO_JUDGE = 20;

  /** Share of one-character tokens above which a page is letter-spaced; prose is under 10%. */
  private static final double LETTER_SPACED_SHARE = 0.4;

  private PdfPageTexts() {
  }

  /**
   * Extracts every page of the PDF, calling the sink once per page. A page whose text comes out
   * letter-spaced is extracted again with looser word-gap tolerances.
   *
   * @return the number of pages in the document
   */
  static int forEachPage(File pdf, PageSink sink) throws IOException {
    try (PDDocument document = PDDocument.load(pdf, MemoryUsageSetting.setupTempFileOnly())) {
      PDFTextStripper stripper = new PDFTextStripper();
      PDFTextStripper loose = new PDFTextStripper();
      loose.setSpacingTolerance(LOOSE_SPACING_TOLERANCE);
      loose.setAverageCharTolerance(LOOSE_AVERAGE_CHAR_TOLERANCE);
      int pages = document.getNumberOfPages();
      for (int page = 1; page <= pages; page++) {
        stripper.setStartPage(page);
        stripper.setEndPage(page);
        String text = stripper.getText(document);
        if (isLetterSpaced(text)) {
          loose.setStartPage(page);
          loose.setEndPage(page);
          text = loose.getText(document);
        }
        sink.page(page, text);
      }
      return pages;
    }
  }

  /** True when most whitespace-separated tokens are single characters ("N o. 1 1 – 2 0 4"). */
  static boolean isLetterSpaced(String text) {
    int tokens = 0;
    int single = 0;
    for (String token : text.trim().split("\\s+")) {
      if (token.isEmpty()) {
        continue;
      }
      tokens++;
      if (token.length() == 1) {
        single++;
      }
    }
    return tokens >= MIN_TOKENS_TO_JUDGE && single > LETTER_SPACED_SHARE * tokens;
  }
}
