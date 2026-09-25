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

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link PdfPageTexts}; the PDF is built in the test, so no binary fixture is
 * checked in.
 */
@Tag("unit")
class PdfPageTextsTest {

  private static File pdfWith(File dir, String... pageTexts) throws IOException {
    File file = new File(dir, "opinion.pdf");
    try (PDDocument doc = new PDDocument()) {
      for (String text : pageTexts) {
        PDPage page = new PDPage();
        doc.addPage(page);
        try (PDPageContentStream out = new PDPageContentStream(doc, page)) {
          out.beginText();
          out.setFont(PDType1Font.HELVETICA, 12);
          out.newLineAtOffset(72, 700);
          out.showText(text);
          out.endText();
        }
      }
      doc.save(file);
    }
    return file;
  }

  @Test void deliversEachPageInOrderWithItsNumber(@TempDir File dir) throws IOException {
    File pdf = pdfWith(dir, "first page text", "second page text", "third page text");
    final List<String> texts = new ArrayList<String>();
    final List<Integer> numbers = new ArrayList<Integer>();
    int pages = PdfPageTexts.forEachPage(pdf, new PdfPageTexts.PageSink() {
      @Override public void page(int pageNumber, String text) {
        numbers.add(pageNumber);
        texts.add(text.trim());
      }
    });
    assertEquals(3, pages);
    assertEquals(java.util.Arrays.asList(1, 2, 3), numbers);
    assertEquals("first page text", texts.get(0));
    assertEquals("third page text", texts.get(2));
  }

  @Test void letterSpacedTextIsRecognised() {
    assertTrue(PdfPageTexts.isLetterSpaced("N o. 1 1 – 2 0 4. A r g u e d A p ril 1 6, 2 0 1 2 "
        + "— D e ci d e d J u n e 1 8, 2 0 1 2 T h e F ai r L a b o r St a n d a r d s"));
  }

  @Test void ordinaryProseIsNotLetterSpaced() {
    assertFalse(PdfPageTexts.isLetterSpaced("The Fair Labor Standards Act of 1938 requires "
        + "employers to pay employees overtime wages, see 29 U. S. C. § 207(a), but this "
        + "requirement does not apply with respect to workers employed in a bona fide "
        + "outside salesman capacity."));
  }

  @Test void aShortPageIsNotJudged() {
    assertFalse(PdfPageTexts.isLetterSpaced("1 2 3 4 5"));
  }

  @Test void aPageIsNotCarriedIntoTheNext(@TempDir File dir) throws IOException {
    File pdf = pdfWith(dir, "alpha", "beta");
    final List<String> texts = new ArrayList<String>();
    PdfPageTexts.forEachPage(pdf, new PdfPageTexts.PageSink() {
      @Override public void page(int pageNumber, String text) {
        texts.add(text);
      }
    });
    assertTrue(texts.get(1).contains("beta"));
    assertTrue(!texts.get(1).contains("alpha"));
  }
}
