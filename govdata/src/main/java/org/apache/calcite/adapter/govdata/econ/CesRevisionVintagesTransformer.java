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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Transforms BLS's CES vintage-data product (cesvinall.zip) into long-format revision
 * history rows: one row per (series, release, reference period).
 *
 * <p>Source: {@code https://www.bls.gov/web/empsit/cesvinall.zip} — a ~3MB ZIP of ~226
 * CSVs named {@code tri_<industry_code>_SA.csv} / {@code tri_<industry_code>_NSA.csv}
 * (113 CES industry codes, seasonally-adjusted and not, one file each). Each CSV is a WIDE
 * matrix: one row per monthly Employment Situation release (identified by its own
 * {@code year,month} columns), one column per reference month back to January 1939
 * (1000+ date columns, header label {@code Mon_YY}). A release row's value for a given
 * reference-month column is that vintage's published estimate for that month; later
 * release rows carry the revised value for the same reference month, which is exactly the
 * revision history D-023/D-156 asked for (the annual benchmark revision is just the diff
 * between two vintages of the same reference month already inside this matrix — no second
 * QCEW source is needed).
 *
 * <p><b>Scope decision (deliberate partial build)</b>: this ZIP carries 113 industry codes
 * (226 files); this transformer keeps only 4 — total nonfarm and 3 major supersector
 * aggregates (see {@link #TARGET_INDUSTRY_CODES}) — not all 58+ industry-detail series.
 * Widening coverage later only requires adding codes to that set.
 *
 * <p><b>Reference-period window (deliberate row-count bound)</b>: BLS leaves not-yet-published
 * (future, relative to a given release) reference-month columns blank, but populated columns
 * run back to 1939 for these headline series — melting the full width of every release row
 * would repeat 80+ years of frozen (never-revised-again) values in every single vintage, for
 * no revision signal. Each release keeps only its most recent {@link #WINDOW_MONTHS} reference
 * months (the rightmost non-blank column, "anchor", and up to 72 columns before it) — enough
 * to see the preliminary estimate, the following 1-2 monthly revisions, and the annual
 * February benchmark revision (which restates the prior calendar year).
 *
 * <p>Series ID reconstruction: BLS CES series IDs are {@code CES}/{@code CEU} (seasonally
 * adjusted / not) + 2-digit supersector + 6-digit industry + 2-digit data-type code, e.g.
 * {@code CES0000000001} (total nonfarm, all employees, thousands). This ZIP's filename
 * industry_code is exactly that 6-digit supersector+industry field (confirmed against known
 * series: {@code 000000} to total nonfarm CES0000000001, {@code 050000} to total private
 * CES0500000001 — both already loaded by econ.employment_statistics); appending data-type
 * suffix {@code 0001} (all employees, thousands) reconstructs the full series ID.
 */
public class CesRevisionVintagesTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CesRevisionVintagesTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // download.bls.gov / www.bls.gov 403s a generic/blank User-Agent (Akamai) — same
  // self-identifying UA convention already used by this schema's other bulk BLS fetches.
  private static final String USER_AGENT = "calcite-govdata (+mailto:kennethstott@gmail.com)";

  // See class javadoc "Scope decision".
  private static final Set<String> TARGET_INDUSTRY_CODES = new HashSet<>(Arrays.asList(
      "000000", // Total nonfarm (CES/CEU0000000001)
      "050000", // Total private (CES/CEU0500000001)
      "060000", // Goods-producing (CES/CEU0600000001)
      "070000" // Service-providing (CES/CEU0700000001)
  ));

  private static final Pattern ENTRY_PATTERN =
      Pattern.compile(".*tri_(\\d{6})_(SA|NSA)\\.csv$", Pattern.CASE_INSENSITIVE);
  private static final Pattern REF_COLUMN_PATTERN = Pattern.compile("^([A-Za-z]{3})_(\\d{2})$");

  // See class javadoc "Reference-period window". 73 = current (anchor) month + 72 back.
  private static final int WINDOW_MONTHS = 73;

  private static final Map<String, Integer> MONTH_NUMBERS = buildMonthNumbers();

  private static Map<String, Integer> buildMonthNumbers() {
    Map<String, Integer> map = new HashMap<>();
    String[] names = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    for (int i = 0; i < names.length; i++) {
      map.put(names[i], i + 1);
    }
    return map;
  }

  @Override public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    byte[] zipBytes;
    try {
      zipBytes = downloadBytes(url);
    } catch (IOException e) {
      throw new RuntimeException("CES vintage archive: failed to download from " + url, e);
    }

    ArrayNode result = MAPPER.createArrayNode();
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        Matcher m = ENTRY_PATTERN.matcher(entry.getName());
        if (m.matches() && TARGET_INDUSTRY_CODES.contains(m.group(1))) {
          String industryCode = m.group(1);
          boolean seasonallyAdjusted = "SA".equalsIgnoreCase(m.group(2));
          byte[] csvBytes = readEntry(zis);
          parseCsv(csvBytes, industryCode, seasonallyAdjusted, result);
        }
        zis.closeEntry();
      }
    } catch (IOException e) {
      throw new RuntimeException("CES vintage archive present but unparseable from " + url, e);
    }

    LOGGER.info("CES revision vintages: parsed {} rows from {} target series (of 113 in archive)",
        result.size(), TARGET_INDUSTRY_CODES.size() * 2);
    return result.toString();
  }

  private byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", USER_AGENT);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream()) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return baos.toByteArray();
    }
  }

  private byte[] readEntry(ZipInputStream zis) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int len;
    while ((len = zis.read(buf)) > 0) {
      baos.write(buf, 0, len);
    }
    return baos.toByteArray();
  }

  private void parseCsv(byte[] csvBytes, String industryCode, boolean seasonallyAdjusted,
      ArrayNode result) throws IOException {
    String seriesId = toSeriesId(industryCode, seasonallyAdjusted);
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new ByteArrayInputStream(csvBytes), StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return;
      }
      // header[0]=year, header[1]=month (the release identifier); header[2..] are
      // reference-period columns labeled "Mon_YY" (Jan_39 .. present).
      String[] header = headerLine.split(",", -1);
      int[] refYear = new int[header.length];
      int[] refMonth = new int[header.length];
      for (int i = 2; i < header.length; i++) {
        Matcher hm = REF_COLUMN_PATTERN.matcher(header[i].trim());
        if (hm.matches()) {
          refMonth[i] = MONTH_NUMBERS.getOrDefault(hm.group(1), 0);
          int yy = Integer.parseInt(hm.group(2));
          // Two-digit year disambiguation: this file only ever spans 1939-present, so
          // yy >= 39 is unambiguously 19xx and yy < 39 is unambiguously 20xx.
          refYear[i] = yy >= 39 ? 1900 + yy : 2000 + yy;
        }
      }

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] fields = line.split(",", -1);
        if (fields.length < 3) {
          continue;
        }
        int releaseYear;
        int releaseMonth;
        try {
          releaseYear = Integer.parseInt(fields[0].trim());
          releaseMonth = Integer.parseInt(fields[1].trim());
        } catch (NumberFormatException e) {
          continue;
        }

        // Anchor = rightmost populated reference-period column. BLS leaves
        // not-yet-published (future, relative to this release) columns blank, so this
        // finds the release's own "current month" estimate without needing to match it
        // back to the release's own (year, month) label.
        int anchor = -1;
        for (int i = fields.length - 1; i >= 2; i--) {
          if (!fields[i].trim().isEmpty()) {
            anchor = i;
            break;
          }
        }
        if (anchor < 2) {
          continue;
        }

        int windowStart = Math.max(2, anchor - (WINDOW_MONTHS - 1));
        for (int i = anchor; i >= windowStart; i--) {
          if (refYear[i] == 0) {
            continue;
          }
          String raw = fields[i].trim();
          if (raw.isEmpty()) {
            continue;
          }
          double value;
          try {
            value = Double.parseDouble(raw);
          } catch (NumberFormatException e) {
            continue;
          }
          ObjectNode row = MAPPER.createObjectNode();
          row.put("series_id", seriesId);
          row.put("seasonally_adjusted", seasonallyAdjusted);
          row.put("industry_code", industryCode);
          row.put("release_year", releaseYear);
          row.put("release_month", releaseMonth);
          row.put("reference_year", refYear[i]);
          row.put("reference_month", refMonth[i]);
          row.put("value", value);
          result.add(row);
        }
      }
    }
  }

  private String toSeriesId(String industryCode, boolean seasonallyAdjusted) {
    String prefix = seasonallyAdjusted ? "CES" : "CEU";
    return prefix + industryCode + "0001";
  }
}
