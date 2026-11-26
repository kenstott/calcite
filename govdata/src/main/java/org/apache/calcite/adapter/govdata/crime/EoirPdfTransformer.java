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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Base for tables built from the Adjudication Statistics PDFs of the Executive Office for
 * Immigration Review (EOIR, the US immigration courts).
 */
public abstract class EoirPdfTransformer implements ResponseTransformer {

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int MAX_BYTES = 40 * 1024 * 1024;

  /** Builds the table's rows from the configured source response and URL. */
  protected abstract String rows(String response, String sourceUrl) throws IOException;

  @Override public String transform(String response, RequestContext context) {
    try {
      return rows(response, context.getUrl());
    } catch (IOException e) {
      throw new IllegalStateException("EOIR: failed to read statistics via " + context.getUrl(), e);
    }
  }

  /**
   * The text of each page, top-to-bottom. Rotated glyphs are dropped: EOIR stamps superseded
   * pages with a diagonal "This version was archived on ..." watermark whose characters would
   * otherwise be interleaved with the table's digits.
   */
  static List<String> pageTexts(byte[] pdf) throws IOException {
    List<String> pages = new ArrayList<String>();
    try (PDDocument document = PDDocument.load(new ByteArrayInputStream(pdf))) {
      PDFTextStripper stripper = new PDFTextStripper() {
        @Override protected void processTextPosition(TextPosition position) {
          if (Math.abs(position.getTextMatrix().getShearY()) <= 0.01f) {
            super.processTextPosition(position);
          }
        }
      };
      stripper.setSortByPosition(true);
      stripper.setLineSeparator("\n");
      for (int page = 1; page <= document.getNumberOfPages(); page++) {
        stripper.setStartPage(page);
        stripper.setEndPage(page);
        pages.add(stripper.getText(document));
      }
    }
    return pages;
  }

  static byte[] download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(180000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (InputStream in = conn.getInputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = in.read(buf)) > 0) {
        out.write(buf, 0, len);
        if (out.size() > MAX_BYTES) {
          throw new IOException("Response from " + url + " exceeds " + MAX_BYTES + " bytes");
        }
      }
    }
    return out.toByteArray();
  }
}
