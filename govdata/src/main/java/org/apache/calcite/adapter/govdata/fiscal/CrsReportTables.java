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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads a Congressional Research Service report from EveryCRSReport (the public archive of CRS
 * reports, which are public-domain congressional work product) and exposes its HTML tables.
 *
 * <p>The report landing page {@code /reports/<ID>.html} lists every published version; the
 * newest version's HTML file name carries a date prefix ({@code YYYY-MM-DD_<ID>_<hash>.html}),
 * so the current version is the dated link that sorts last. A table is located by the exact
 * text of its header cells, not by position, so a CRS edit that reorders or adds tables is
 * either handled or fails loudly with the report version named.
 */
final class CrsReportTables {

  private static final String BASE = "https://www.everycrsreport.com";
  private static final String USER_AGENT = "govdata-etl/1.0 (+https://github.com/kenstott/calcite)";

  private final String reportId;
  private final String versionFile;
  private final Document doc;

  private CrsReportTables(String reportId, String versionFile, Document doc) {
    this.reportId = reportId;
    this.versionFile = versionFile;
    this.doc = doc;
  }

  static CrsReportTables load(String reportId) throws IOException {
    String index = new String(get(BASE + "/reports/" + reportId + ".html"), StandardCharsets.UTF_8);
    Pattern p = Pattern.compile("href=\"(/files/(\\d{4}-\\d{2}-\\d{2})_" + reportId
        + "_[0-9a-f]+\\.html)\"");
    Matcher m = p.matcher(index);
    String best = null;
    String bestDate = null;
    while (m.find()) {
      if (bestDate == null || m.group(2).compareTo(bestDate) > 0) {
        bestDate = m.group(2);
        best = m.group(1);
      }
    }
    if (best == null) {
      throw new IOException("CRS " + reportId + ": no dated HTML version link on " + BASE
          + "/reports/" + reportId + ".html");
    }
    Document d = Jsoup.parse(new String(get(BASE + best), StandardCharsets.UTF_8));
    return new CrsReportTables(reportId, best, d);
  }

  /**
   * Returns the body rows (header excluded), each as its cell texts, of the one table whose
   * first-row header cells start with {@code headerPrefixes} (compared after whitespace
   * normalisation). Throws if no table or more than one table matches.
   */
  List<List<String>> table(String... headerPrefixes) throws IOException {
    List<List<String>> found = null;
    for (Element t : doc.select("table")) {
      Elements trs = t.select("tr");
      if (trs.isEmpty()) {
        continue;
      }
      List<String> head = cells(trs.get(0));
      if (head.size() < headerPrefixes.length) {
        continue;
      }
      boolean ok = true;
      for (int i = 0; i < headerPrefixes.length; i++) {
        if (!norm(head.get(i)).startsWith(headerPrefixes[i])) {
          ok = false;
          break;
        }
      }
      if (!ok) {
        continue;
      }
      if (found != null) {
        throw new IOException("CRS " + reportId + " (" + versionFile + "): more than one table "
            + "has header starting " + headerPrefixes[0]);
      }
      found = new ArrayList<List<String>>();
      for (int r = 1; r < trs.size(); r++) {
        found.add(cells(trs.get(r)));
      }
    }
    if (found == null) {
      throw new IOException("CRS " + reportId + " (" + versionFile + "): no table with header "
          + "starting " + headerPrefixes[0]);
    }
    return found;
  }

  String version() {
    return versionFile;
  }

  private static List<String> cells(Element tr) {
    List<String> out = new ArrayList<String>();
    for (Element c : tr.select("th,td")) {
      out.add(norm(c.text()));
    }
    return out;
  }

  private static String norm(String s) {
    return s.replace(' ', ' ').replaceAll("\\s+", " ").trim();
  }

  /**
   * Removes the trailing footnote letter CRS superscripts onto a cell ("2b" becomes "2",
   * "(365)r" becomes "(365)", "P.L. 99-591f" becomes "P.L. 99-591"). A footnote letter only ever
   * follows a digit, a closing bracket or a closing parenthesis.
   */
  static String stripFootnote(String s) {
    return s.replaceAll("(?<=[\\d)\\]])\\s*[a-z]{1,2}$", "").trim();
  }

  /** Parses {@code MM-DD-YYYY} or {@code MM/DD/YYYY}. */
  static LocalDate parseDate(String s, String what) throws IOException {
    Matcher m = Pattern.compile("^(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})$").matcher(s);
    if (!m.matches()) {
      throw new IOException("CRS table: unparseable " + what + " date '" + s + "'");
    }
    return LocalDate.of(Integer.parseInt(m.group(3)), Integer.parseInt(m.group(1)),
        Integer.parseInt(m.group(2)));
  }

  private static byte[] get(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setInstanceFollowRedirects(true);
    NetworkOutageWait.connect(conn);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("HTTP " + code + " for " + url);
    }
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (InputStream in = conn.getInputStream()) {
      byte[] chunk = new byte[1 << 16];
      int n;
      while ((n = in.read(chunk)) != -1) {
        buf.write(chunk, 0, n);
      }
    } finally {
      conn.disconnect();
    }
    return buf.toByteArray();
  }
}
