/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// storage-provider-guard:ignore-file — the 14 MB source PDF is a transient local temp artifact
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.BufferedReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Streams the Section 301 additional-duty list membership (Lists 1, 2, 3 and 4A)
 * out of the Chapter 99 PDF of the current HTS revision.
 *
 * <p>The USITC JSON export carries the Chapter 99 headings but not which
 * products each heading covers. That membership is published only in the
 * subchapter III U.S. notes (note 20(b), (d), (f), (s)(i)), as blocks of 8-digit
 * subheadings printed four to a line. This transformer downloads the Chapter 99
 * PDF, extracts its text, locates each list's anchor paragraph and collects the
 * code lines that follow it until the first non-code line.
 *
 * <p>The additional rate is read from the note text ("additional 25 percent ad
 * valorem rate of duty imposed by heading 9903.88.01"); a list whose anchor,
 * code block or rate cannot be found fails the run instead of yielding a partial
 * table. List 4B (heading 9903.88.16) is suspended and carries no list here.
 */
public class UsitcAdditionalDutyListsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UsitcAdditionalDutyListsTransformer.class);

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 900_000;
  private static final int MAX_RETRIES = 4;
  /** Lines between an anchor paragraph and its first code line (compiler's notes, page headers). */
  private static final int MAX_ANCHOR_GAP_LINES = 40;

  private static final Pattern CODE = Pattern.compile("\\d{4}\\.\\d{2}\\.\\d{2}");
  private static final Pattern CODE_LINE = Pattern.compile("(?:\\d{4}\\.\\d{2}\\.\\d{2})+");
  private static final Pattern PAGE_NO = Pattern.compile("99 - III - \\d+");
  private static final Pattern REVISION =
      Pattern.compile("Harmonized Tariff Schedule of the United States Revision (\\d+) \\((\\d{4})\\)");

  /** One Section 301 list: the heading, how its note paragraph starts, and its name. */
  private static final class ListSpec {
    final String heading;
    final String listName;
    final String noteRef;
    final String anchorPrefix;

    ListSpec(String heading, String listName, String noteRef, String anchorPrefix) {
      this.heading = heading;
      this.listName = listName;
      this.noteRef = noteRef;
      this.anchorPrefix = anchorPrefix;
    }
  }

  private static final List<ListSpec> LISTS = new ArrayList<ListSpec>();

  static {
    LISTS.add(new ListSpec("9903.88.01", "List 1", "U.S. note 20(b)",
        "(b) Heading 9903.88.01 applies to all products of China"));
    LISTS.add(new ListSpec("9903.88.02", "List 2", "U.S. note 20(d)",
        "(d) Heading 9903.88.02 applies to all products of China"));
    LISTS.add(new ListSpec("9903.88.03", "List 3", "U.S. note 20(f)",
        "(f) Heading 9903.88.03 applies to all products of China"));
    LISTS.add(new ListSpec("9903.88.15", "List 4A", "U.S. note 20(s)(i)",
        "(s) Heading 9903.88.15 applies to:"));
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("UsitcAdditionalDutyListsTransformer: no URL in context");
    }
    LOGGER.info("usitc_additional_duty_lists: downloading Chapter 99 from {}", url);
    Path pdf = Files.createTempFile("hts-chapter99-", ".pdf");
    String text;
    try {
      download(url, context.getHeaders(), pdf);
      try (PDDocument doc = PDDocument.load(pdf.toFile())) {
        text = new PDFTextStripper().getText(doc);
      }
    } finally {
      Files.deleteIfExists(pdf);
    }
    List<Map<String, Object>> rows = parse(text);
    LOGGER.info("usitc_additional_duty_lists: {} (hts8, list) rows", rows.size());
    return rows.iterator();
  }

  /** Parses the extracted Chapter 99 text. Package-visible for tests. */
  static List<Map<String, Object>> parse(String text) throws IOException {
    List<String> lines = new ArrayList<String>();
    BufferedReader br = new BufferedReader(new StringReader(text));
    for (String l = br.readLine(); l != null; l = br.readLine()) {
      lines.add(l.trim());
    }

    Matcher rev = REVISION.matcher(text);
    if (!rev.find()) {
      throw new IOException("usitc_additional_duty_lists: HTS revision label not found in PDF");
    }
    String revision = "Revision " + rev.group(1) + " (" + rev.group(2) + ")";
    int year = Integer.parseInt(rev.group(2));

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (ListSpec spec : LISTS) {
      Set<String> codes = collectCodes(lines, spec);
      double rate = rate(text, spec);
      for (String code : codes) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("hts8", code.replace(".", ""));
        row.put("program", "Section 301");
        row.put("list_name", spec.listName);
        row.put("chapter99_heading", spec.heading);
        row.put("additional_rate_pct", rate);
        row.put("note_ref", spec.noteRef);
        row.put("hts_revision", revision);
        row.put("year", year);
        rows.add(row);
      }
      LOGGER.info("usitc_additional_duty_lists: {} {} codes at {}%", spec.listName,
          codes.size(), rate);
    }
    return rows;
  }

  private static Set<String> collectCodes(List<String> lines, ListSpec spec) throws IOException {
    int anchor = -1;
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).startsWith(spec.anchorPrefix)) {
        anchor = i;
        break;
      }
    }
    if (anchor < 0) {
      throw new IOException("usitc_additional_duty_lists: anchor not found for " + spec.listName
          + " (" + spec.anchorPrefix + ")");
    }
    int i = anchor + 1;
    int limit = Math.min(lines.size(), anchor + MAX_ANCHOR_GAP_LINES);
    while (i < limit && !CODE_LINE.matcher(lines.get(i)).matches()) {
      i++;
    }
    if (i >= limit) {
      throw new IOException("usitc_additional_duty_lists: no code block after anchor for "
          + spec.listName);
    }
    Set<String> codes = new LinkedHashSet<String>();
    for (; i < lines.size(); i++) {
      String l = lines.get(i);
      if (CODE_LINE.matcher(l).matches()) {
        Matcher m = CODE.matcher(l);
        while (m.find()) {
          codes.add(m.group());
        }
      } else if (!isPageFurniture(l)) {
        break;
      }
    }
    if (codes.isEmpty()) {
      throw new IOException("usitc_additional_duty_lists: empty code block for " + spec.listName);
    }
    return codes;
  }

  private static boolean isPageFurniture(String l) {
    return l.isEmpty()
        || l.startsWith("Harmonized Tariff Schedule of the United States Revision")
        || l.startsWith("Annotated for Statistical Reporting Purposes")
        || l.equals("XXII")
        || l.startsWith("U.S. Notes (con.)")
        || PAGE_NO.matcher(l).matches();
  }

  private static double rate(String text, ListSpec spec) throws IOException {
    Pattern p = Pattern.compile("additional\\s+(\\d+(?:\\.\\d+)?)\\s+percent\\s+ad\\s+valorem\\s+rate"
        + "\\s+of\\s+duty\\s+imposed\\s+by\\s+heading\\s+" + Pattern.quote(spec.heading));
    Matcher m = p.matcher(text);
    if (!m.find()) {
      throw new IOException("usitc_additional_duty_lists: additional rate not found in notes for "
          + spec.heading);
    }
    return Double.parseDouble(m.group(1));
  }

  private static void download(String url, Map<String, String> headers, Path dest)
      throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      if (headers != null) {
        for (Map.Entry<String, String> h : headers.entrySet()) {
          conn.setRequestProperty(h.getKey(), h.getValue());
        }
      }
      try {
        NetworkOutageWait.connect(conn);
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK) {
          try (InputStream in = conn.getInputStream()) {
            Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
          }
          return;
        }
        if (status != 429 && status != 500 && status != 503) {
          throw new IOException("HTTP " + status + " from " + url);
        }
        LOGGER.warn("usitc_additional_duty_lists: HTTP {} (attempt {}/{})", status, attempt,
            MAX_RETRIES);
        last = new IOException("HTTP " + status + " from " + url);
      } catch (IOException e) {
        if (e.getMessage() != null && e.getMessage().startsWith("HTTP ")) {
          throw e;
        }
        last = e;
      } finally {
        conn.disconnect();
      }
      try {
        Thread.sleep(1000L * attempt);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted", ie);
      }
    }
    throw last != null ? last
        : new IOException("usitc_additional_duty_lists: retries exhausted for " + url);
  }
}
