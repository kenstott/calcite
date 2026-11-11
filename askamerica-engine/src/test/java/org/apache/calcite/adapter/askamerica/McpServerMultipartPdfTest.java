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
package org.apache.calcite.adapter.askamerica;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * crashstats.nhtsa.dot.gov returns its publications as a PDF wrapped in one
 * multipart/form-data part. web_fetch must read the PDF inside, not treat the body as text.
 */
@Tag("unit")
class McpServerMultipartPdfTest {

  private static byte[] pdfSaying(String text) throws Exception {
    try (PDDocument doc = new PDDocument()) {
      PDPage page = new PDPage();
      doc.addPage(page);
      try (PDPageContentStream cs = new PDPageContentStream(doc, page)) {
        cs.beginText();
        cs.setFont(PDType1Font.HELVETICA, 12);
        cs.newLineAtOffset(72, 700);
        cs.showText(text);
        cs.endText();
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      doc.save(out);
      return out.toByteArray();
    }
  }

  /** The framing crashstats sends, byte for byte in shape. */
  private static byte[] wrap(byte[] pdf) throws Exception {
    String boundary = "------WebKitFormBoundarynMLJ4NpJZbZz68Eq";
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write((boundary + "\r\n"
        + "Content-Disposition: form-data; name=\"813833.pdf\"; filename=\"813833.pdf\"\r\n"
        + "Content-Type: application/pdf\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
    out.write(pdf);
    out.write(("\r\n" + boundary + "--\r\n").getBytes(StandardCharsets.US_ASCII));
    return out.toByteArray();
  }

  @Test void aMultipartWrappedPdfIsReadAsThePdfInside() throws Exception {
    byte[] pdf = pdfSaying("first half 2025 fatalities 17,175");
    byte[] unwrapped = McpServer.unwrapMultipartPdf(wrap(pdf));

    assertEquals("pdf", McpServer.sniffContentKind(unwrapped, "application/pdf"));
    List<String> pages = McpServer.extractPages("pdf", "https://crashstats.example/813833",
        unwrapped);
    assertTrue(pages.get(0).contains("17,175"), pages.get(0));
  }

  @Test void anUnwrappedPdfIsReturnedUnchanged() throws Exception {
    byte[] pdf = pdfSaying("plain");
    assertSame(pdf, McpServer.unwrapMultipartPdf(pdf));
  }

  @Test void aMultipartBodyWithNoPdfIsReturnedUnchanged() {
    byte[] body = ("--boundary\r\nContent-Type: text/plain\r\n\r\nhello\r\n--boundary--\r\n")
        .getBytes(StandardCharsets.US_ASCII);
    assertSame(body, McpServer.unwrapMultipartPdf(body));
  }

  @Test void theTrailingBoundaryIsCutAtTheLastEofMarker() throws Exception {
    byte[] pdf = pdfSaying("trailer");
    // The PDF through its final %%EOF marker, without the line break PDFBox writes after it.
    int end = pdf.length;
    while (pdf[end - 1] == '\n' || pdf[end - 1] == '\r') {
      end--;
    }
    assertArrayEquals(java.util.Arrays.copyOf(pdf, end),
        McpServer.unwrapMultipartPdf(wrap(pdf)));
  }
}
