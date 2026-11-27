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
package org.apache.calcite.adapter.govdata.environment;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds {@code npdes_cafo_status} rows from EPA's annual "NPDES CAFO Permitting Status Report"
 * national summaries.
 *
 * <p>The table's source URL is the EPA page that links one PDF per end-of-year report. This
 * transformer reads those links from the page, downloads each report and reads, per state, the
 * EPA region, the total number of CAFOs and the number with NPDES permits. Earlier reports carry
 * further free-text program-status columns, which are not read.
 *
 * <p>Each report prints national totals. The parsed rows of a report must add up to them exactly,
 * otherwise parsing throws, so a layout change or a skipped row fails the load.
 */
public class NpdesCafoStatusTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int MAX_BYTES = 10 * 1024 * 1024;

  private static final Pattern REPORT_LINK = Pattern.compile(
      "<a[^>]+href=\"([^\"]+\\.pdf)\"[^>]*>(?:(?!</a>).)*?Endyear\\s*(?:&nbsp;)?\\s*(\\d{4})",
      Pattern.DOTALL);
  private static final Pattern TOTALS = Pattern.compile(
      "(?m)^\\s*TOTALS\\s+(\\d[\\d,]*)\\s+(\\d[\\d,]*)");

  /** Report row label to USPS code; the tribal rows have none. */
  private static final Map<String, String> AREAS = new LinkedHashMap<String, String>();

  static {
    String[] p = {
        "Alabama", "AL", "Alaska", "AK", "Arizona", "AZ", "Arkansas", "AR", "California", "CA",
        "Colorado", "CO", "Connecticut", "CT", "Delaware", "DE", "District of Columbia", "DC",
        "Florida", "FL", "Georgia", "GA", "Hawaii", "HI", "Idaho", "ID", "Illinois", "IL",
        "Indiana", "IN", "Iowa", "IA", "Kansas", "KS", "Kentucky", "KY", "Louisiana", "LA",
        "Maine", "ME", "Maryland", "MD", "Massachusetts", "MA", "Michigan", "MI",
        "Minnesota", "MN", "Mississippi", "MS", "Missouri", "MO", "Montana", "MT",
        "Nebraska", "NE", "Nevada", "NV", "New Hampshire", "NH", "New Jersey", "NJ",
        "New Mexico", "NM", "New York", "NY", "North Carolina", "NC", "North Dakota", "ND",
        "Ohio", "OH", "Oklahoma", "OK", "Oregon", "OR", "Pennsylvania", "PA",
        "Puerto Rico", "PR", "Rhode Island", "RI", "South Carolina", "SC",
        "South Dakota", "SD", "Tennessee", "TN", "Texas", "TX", "Utah", "UT", "Vermont", "VT",
        "Virgin Islands", "VI", "Virginia", "VA", "Washington", "WA", "West Virginia", "WV",
        "Wisconsin", "WI", "Wyoming", "WY"};
    for (int i = 0; i < p.length; i += 2) {
      AREAS.put(p[i], p[i + 1]);
    }
  }

  /** A row label (state name or "Indian Country (Rn)"), region, total CAFOs, permitted CAFOs. */
  private static final Pattern ROW = Pattern.compile(
      "^\\s*([A-Z][A-Za-z ]+?(?: \\(R\\d+\\))?)\\s*\\*?\\s+(\\d{1,2})\\s+(\\d[\\d,]*)"
          + "\\s+(\\d[\\d,]*)(?=\\s|$)");

  @Override public String transform(String response, RequestContext context) {
    String pageUrl = context.getUrl();
    Map<Integer, String> reports = reportLinks(response, pageUrl);
    ArrayNode rows = MAPPER.createArrayNode();
    for (Map.Entry<Integer, String> report : reports.entrySet()) {
      String url = report.getValue();
      try (PDDocument doc = PDDocument.load(new ByteArrayInputStream(download(url)))) {
        PDFTextStripper stripper = new PDFTextStripper();
        stripper.setSortByPosition(true);
        rows.addAll(parseReport(report.getKey(), url, stripper.getText(doc)));
      } catch (IOException e) {
        throw new IllegalStateException("npdes_cafo_status: failed reading " + url, e);
      }
    }
    return rows.toString();
  }

  /** Report year to PDF URL, from the links on the EPA page. Package-visible for tests. */
  static Map<Integer, String> reportLinks(String html, String pageUrl) {
    Map<Integer, String> reports = new TreeMap<Integer, String>();
    Matcher m = REPORT_LINK.matcher(html);
    while (m.find()) {
      int year = Integer.parseInt(m.group(2));
      String url = URI.create(pageUrl).resolve(m.group(1).replace(" ", "%20")).toString();
      String previous = reports.put(year, url);
      if (previous != null && !previous.equals(url)) {
        throw new IllegalStateException("npdes_cafo_status: two reports linked for " + year
            + ": " + previous + " and " + url);
      }
    }
    if (reports.isEmpty()) {
      throw new IllegalStateException("npdes_cafo_status: no report links on " + pageUrl);
    }
    return reports;
  }

  /** Parses the text of one report and reconciles it with its totals. Package-visible for tests. */
  static ArrayNode parseReport(int year, String url, String text) {
    ArrayNode rows = MAPPER.createArrayNode();
    Map<String, Boolean> seen = new LinkedHashMap<String, Boolean>();
    long total = 0;
    long permitted = 0;
    for (String line : text.split("\r?\n")) {
      Matcher m = ROW.matcher(line);
      if (!m.find()) {
        continue;
      }
      String area = m.group(1);
      boolean tribal = area.startsWith("Indian Country (R");
      if (!tribal && !AREAS.containsKey(area)) {
        continue;
      }
      if (seen.put(area, Boolean.TRUE) != null) {
        throw new IllegalStateException("npdes_cafo_status: two rows for " + area + " in " + url);
      }
      long areaTotal = count(m.group(3));
      long areaPermitted = count(m.group(4));
      total += areaTotal;
      permitted += areaPermitted;
      ObjectNode row = rows.addObject();
      row.put("report_year", year);
      row.put("area_name", area);
      row.put("state_abbr", tribal ? null : AREAS.get(area));
      row.put("epa_region", Integer.parseInt(m.group(2)));
      row.put("total_cafos", areaTotal);
      row.put("permitted_cafos", areaPermitted);
      row.put("report_url", url);
      row.put("type", "npdes_cafo_status");
    }
    Matcher totals = TOTALS.matcher(text);
    if (!totals.find()) {
      throw new IllegalStateException("npdes_cafo_status: no TOTALS line in " + url);
    }
    if (total != count(totals.group(1)) || permitted != count(totals.group(2))) {
      throw new IllegalStateException("npdes_cafo_status: parsed rows sum to " + total + "/"
          + permitted + " but " + url + " prints totals " + totals.group(1) + "/"
          + totals.group(2));
    }
    return rows;
  }

  private static long count(String printed) {
    return Long.parseLong(printed.replace(",", ""));
  }

  private static byte[] download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (InputStream in = conn.getInputStream()) {
      byte[] buffer = new byte[65536];
      int length;
      while ((length = in.read(buffer)) > 0) {
        out.write(buffer, 0, length);
        if (out.size() > MAX_BYTES) {
          throw new IOException("Response from " + url + " exceeds " + MAX_BYTES + " bytes");
        }
      }
    }
    return out.toByteArray();
  }
}
