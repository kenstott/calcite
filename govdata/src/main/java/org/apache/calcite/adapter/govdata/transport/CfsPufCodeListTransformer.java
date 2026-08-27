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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;

/**
 * Extracts a fixed code-list appendix table out of the 2017 Commodity Flow Survey (CFS) Public
 * Use File Data Users Guide PDF (Census {@code cfs_2017_puf_users_guide.pdf}).
 *
 * <p>The guide's Appendix A-3 (SCTG commodity codes) and Appendix A-4 (Mode of transportation
 * codes) are the CFS PUF's own official definitions for {@code cfs_shipments.sctg_code} and
 * {@code .mode_code} &mdash; there is no CSV/XLSX distribution of either list, only this PDF, so
 * the appendix text is parsed directly rather than left un-joinable in the corpus. Both appendix
 * tables extract as one code + description per line via PDFBox's default text stripper (verified
 * against the live PDF: no interleaved columns), bounded by an {@code Appendix A-N} start marker
 * and the next {@code Appendix}/{@code NOTE:} line as the end marker. This is 2017-vintage,
 * one-time reference data — Census has not revised this PUF or its appendices since publication.
 */
public abstract class CfsPufCodeListTransformer implements ResponseTransformer {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  protected abstract void parseRows(String appendixText, ArrayNode rows);

  protected abstract String appendixStartMarker();

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    try {
      byte[] pdfBytes = downloadBytes(url);
      String fullText;
      try (PDDocument document = PDDocument.load(new ByteArrayInputStream(pdfBytes))) {
        fullText = new PDFTextStripper().getText(document);
      }
      // PDFBox's stripper leaves a trailing space or two before every line break (verified
      // against the live PDF); strip it so line-anchored markers/regexes don't need to guess at it.
      fullText = fullText.replaceAll("[ \\t]+\n", "\n");
      String appendixText = extractAppendix(fullText, appendixStartMarker());
      ArrayNode rows = MAPPER.createArrayNode();
      parseRows(appendixText, rows);
      return MAPPER.writeValueAsString(rows);
    } catch (Exception e) {
      throw new RuntimeException("CFS PUF code list: failed to parse from " + url, e);
    }
  }

  /**
   * Isolates the text between {@code startMarker} and the next {@code Appendix }/{@code NOTE:}
   * line. {@code startMarker} must include the appendix's title line (e.g. {@code "Appendix
   * A-3\nSCTG"}), not just the bare "Appendix A-N" heading &mdash; that heading also appears
   * verbatim in an earlier footnote cross-reference ("(3) See Appendix A-3 for..."), which a
   * plain {@code indexOf} would match first.
   */
  private String extractAppendix(String fullText, String startMarker) {
    int start = fullText.indexOf(startMarker);
    if (start < 0) {
      throw new IllegalStateException("Marker not found in CFS PUF guide: " + startMarker);
    }
    String[] lines = fullText.substring(start).split("\n");
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < lines.length; i++) {
      String line = lines[i];
      if (line.startsWith("Appendix ") || line.trim().startsWith("NOTE:")) {
        break;
      }
      sb.append(line).append('\n');
    }
    return sb.toString();
  }

  private byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    InputStream is = conn.getInputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
    } finally {
      is.close();
    }
    return baos.toByteArray();
  }
}
