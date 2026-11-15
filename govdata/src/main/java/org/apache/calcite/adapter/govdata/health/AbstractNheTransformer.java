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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.CompleteBody;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Shared discovery, download and CSV reading for the CMS National Health Expenditure
 * Accounts (NHEA) historical tables.
 *
 * <p>The framework fetches the CMS "NHE historical" landing page and hands the HTML to
 * {@link #transform}. The zip filename embeds the data's last year
 * ({@code ...-cy-1960-2024.zip}) and changes each release, so the link is discovered from
 * the page: the {@code /files/zip/} href containing {@link #zipKeyword()} with the highest
 * embedded year wins. The zip holds one CSV (plus an XLS copy) in Windows-1252 encoding.
 */
abstract class AbstractNheTransformer implements ResponseTransformer {

  static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Charset CP1252 = Charset.forName("windows-1252");
  private static final String CMS_HOST = "https://www.cms.gov";
  private static final Pattern HREF_PATTERN =
      Pattern.compile("href=\"([^\"]+\\.zip)\"", Pattern.CASE_INSENSITIVE);
  private static final Pattern YEAR_PATTERN = Pattern.compile("(19|20)\\d{2}");

  /** Substring identifying this table's zip among the page's links. */
  abstract String zipKeyword();

  /** Parses the rows of the zip's CSV (header row first) into the output array. */
  abstract ArrayNode parse(List<List<String>> rows);

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException("NHE: empty landing-page response from " + context.getUrl());
    }
    try {
      String zipUrl = findZipUrl(response);
      byte[] csv = extractCsv(downloadBytes(zipUrl));
      return parse(readRows(csv)).toString();
    } catch (IOException e) {
      throw new RuntimeException("NHE: failed to load " + zipKeyword() + " from " + context.getUrl(), e);
    }
  }

  private String findZipUrl(String html) {
    Matcher m = HREF_PATTERN.matcher(html);
    String best = null;
    int bestYear = -1;
    while (m.find()) {
      String href = m.group(1);
      if (!href.toLowerCase(Locale.ROOT).contains(zipKeyword())) {
        continue;
      }
      int year = maxYear(href);
      if (year > bestYear) {
        bestYear = year;
        best = href;
      }
    }
    if (best == null) {
      throw new IllegalStateException("NHE: no zip link containing '" + zipKeyword()
          + "' on the CMS landing page — CMS may have renamed the download");
    }
    return best.startsWith("http") ? best : CMS_HOST + best;
  }

  private static int maxYear(String s) {
    Matcher m = YEAR_PATTERN.matcher(s);
    int best = -1;
    while (m.find()) {
      best = Math.max(best, Integer.parseInt(m.group()));
    }
    return best;
  }

  private static byte[] extractCsv(byte[] zipBytes) throws IOException {
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          byte[] buf = new byte[65536];
          int len;
          while ((len = zis.read(buf)) > 0) {
            baos.write(buf, 0, len);
          }
          return baos.toByteArray();
        }
      }
    }
    throw new IllegalStateException("NHE: zip contains no CSV entry");
  }

  private static List<List<String>> readRows(byte[] csv) throws IOException {
    List<List<String>> rows = new ArrayList<>();
    try (BufferedReader reader =
             new BufferedReader(new InputStreamReader(new ByteArrayInputStream(csv), CP1252))) {
      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (!line.trim().isEmpty()) {
          rows.add(CsvRecordReader.splitFields(line, ','));
        }
      }
    }
    return rows;
  }

  private static byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(180000);
    conn.setRequestProperty("User-Agent", "Mozilla/5.0 GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = CompleteBody.of(conn);
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return baos.toByteArray();
    }
  }

  /** Number of leading spaces; the NHE CSVs encode hierarchy as indentation. */
  static int indent(String s) {
    int i = 0;
    while (i < s.length() && s.charAt(i) == ' ') {
      i++;
    }
    return i;
  }

  /**
   * Parses a numeric cell. Returns null for the source's own "no value" markers (blank or
   * a lone dash, which CMS uses for not-applicable); throws on anything else so a format
   * change is loud.
   */
  static Double number(String cell) {
    String t = cell == null ? "" : cell.trim().replace(",", "");
    if (t.isEmpty() || "-".equals(t)) {
      return null;
    }
    return Double.valueOf(t);
  }
}
