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
// storage-provider-guard:ignore-file - the downloaded zip is a local temp artifact, deleted in the
// finally block, that never reaches storage.
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.SkippedBatchException;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * Turns one CMS Uniform Rate Review public use file (one plan year) into
 * {@code health.aca_rate_review_products} rows: one row per individual-market submission and
 * product, aggregated from the plan-level worksheet 2 ("WKSH2") of the file.
 *
 * <p>CMS names each year's zip differently (and PY2022/PY2023 nest the CSVs in a second zip), so
 * the zip for each plan year is listed in {@link #PUF_ZIPS}. A plan year with no entry has not
 * been registered yet and the batch is skipped; an HTTP 404 on a registered file also skips.
 * Columns are located by header name, and a missing required header fails the batch instead of
 * shifting to a guessed position.
 */
public class AcaRateReviewTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AcaRateReviewTransformer.class);

  private static final String BASE = "https://www.cms.gov/files/zip/";

  /** Plan year to CMS zip file name; the names are irregular, so they cannot be templated. */
  static final Map<Integer, String> PUF_ZIPS;

  static {
    Map<Integer, String> m = new TreeMap<Integer, String>();
    m.put(2022, "2022-urr-puf2.zip");
    m.put(2023, "2023-urr-puf.zip");
    m.put(2024, "py20204-puf-20231031.zip");
    m.put(2025, "py2025puf20241024.zip");
    m.put(2026, "py2026-puf-20260327.zip");
    m.put(2027, "urr-puf-2027.zip");
    PUF_ZIPS = java.util.Collections.unmodifiableMap(m);
  }

  private static final String COL_SUBMISSION = "SUB_TRK_NUM";
  private static final String COL_COMPANY = "COMPANY";
  private static final String COL_ISSUER_ID = "ISSUER_ID";
  private static final String COL_EFF_DT = "EFF_DT";
  private static final String COL_STATE = "STATE";
  private static final String COL_MARKET = "MARKET";
  private static final String COL_PROD_NAME = "PROD_NAME";
  private static final String COL_PROD_ID = "PROD_ID";
  private static final String COL_PLAN_ID = "PLAN_ID";
  private static final String COL_EXCHANGE = "EXCHANGE";
  private static final String COL_RT_PROD = "RT_CHG_CUM_PROD";
  private static final String COL_RT_SUB = "RT_CHG_CUM_SUB";
  private static final String COL_ENROLL = "CUR_ENR";
  private static final String COL_DETERMINATION = "DETERMINATION";

  private static final List<String> REQUIRED = Arrays.asList(COL_SUBMISSION, COL_COMPANY,
      COL_ISSUER_ID, COL_EFF_DT, COL_STATE, COL_MARKET, COL_PROD_NAME, COL_PROD_ID, COL_PLAN_ID,
      COL_EXCHANGE, COL_RT_PROD, COL_RT_SUB, COL_ENROLL, COL_DETERMINATION);

  private static final String INDIVIDUAL = "Individual";

  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(30))
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearText = context.getDimensionValues().get("effective_year");
    if (yearText == null) {
      throw new IllegalStateException(
          "ACA rate review: effective_year dimension is required to pick the plan year file");
    }
    int planYear = Integer.parseInt(yearText.trim());
    String zipName = PUF_ZIPS.get(planYear);
    if (zipName == null) {
      throw new SkippedBatchException("ACA rate review: no CMS file registered for plan year "
          + planYear + " (add it to AcaRateReviewTransformer.PUF_ZIPS once CMS posts it)");
    }
    String url = BASE + zipName;

    Path zip = org.apache.calcite.adapter.file.StagingDirectory
        .newTempFile("aca-urr-", ".zip").toPath();
    try {
      download(url, zip);
      Aggregator agg = new Aggregator(planYear);
      try (ZipFile zf = new ZipFile(zip.toFile())) {
        if (!readWorksheet2(zf, agg)) {
          throw new IOException("ACA rate review: no WKSH2 csv in " + url);
        }
      }
      List<Map<String, Object>> rows = agg.finish(url);
      LOGGER.debug("ACA rate review {}: {} product rows for plan year {}", url, rows.size(),
          planYear);
      return rows.iterator();
    } finally {
      Files.deleteIfExists(zip);
    }
  }

  /** Streams the WKSH2 csv, whether directly in the zip or inside one nested zip. */
  private static boolean readWorksheet2(ZipFile zf, Aggregator agg) throws IOException {
    for (Enumeration<? extends ZipEntry> e = zf.entries(); e.hasMoreElements();) {
      ZipEntry entry = e.nextElement();
      String name = entry.getName().toLowerCase(Locale.ROOT);
      if (isWorksheet2(name)) {
        try (InputStream in = zf.getInputStream(entry)) {
          parseCsv(in, agg);
        }
        return true;
      }
    }
    for (Enumeration<? extends ZipEntry> e = zf.entries(); e.hasMoreElements();) {
      ZipEntry entry = e.nextElement();
      if (!entry.getName().toLowerCase(Locale.ROOT).endsWith(".zip")) {
        continue;
      }
      try (ZipInputStream zin = new ZipInputStream(zf.getInputStream(entry))) {
        for (ZipEntry inner = zin.getNextEntry(); inner != null; inner = zin.getNextEntry()) {
          if (isWorksheet2(inner.getName().toLowerCase(Locale.ROOT))) {
            parseCsv(zin, agg);
            return true;
          }
        }
      }
    }
    return false;
  }

  private static boolean isWorksheet2(String lowerName) {
    return lowerName.endsWith(".csv") && lowerName.contains("wksh2");
  }

  private static void download(String url, Path target) throws IOException {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMinutes(10))
        .GET()
        .build();
    HttpResponse<InputStream> response;
    try {
      response = CLIENT.send(request, HttpResponse.BodyHandlers.ofInputStream());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted fetching ACA rate review file: " + url, e);
    }
    int code = response.statusCode();
    try (InputStream body = response.body()) {
      if (code == 404) {
        throw new SkippedBatchException("ACA rate review file not published (HTTP 404): " + url);
      }
      if (code != 200) {
        throw new IOException("ACA rate review download HTTP " + code + ": " + url);
      }
      Files.copy(body, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  // ---------------------------------------------------------------------------------------
  // CSV
  // ---------------------------------------------------------------------------------------

  /** Reads RFC 4180 records (quoted fields may hold commas, quotes and newlines). */
  static void parseCsv(InputStream in, Aggregator agg) throws IOException {
    try (Reader r = new BufferedReader(
        new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16)) {
      List<String> row = new ArrayList<String>();
      StringBuilder field = new StringBuilder();
      boolean quoted = false;
      boolean pendingRecord = false;
      int c = r.read();
      if (c == 0xFEFF) {
        c = r.read();
      }
      while (c != -1) {
        char ch = (char) c;
        if (quoted) {
          if (ch == '"') {
            int next = r.read();
            if (next == '"') {
              field.append('"');
              c = r.read();
              continue;
            }
            quoted = false;
            c = next;
            continue;
          }
          field.append(ch);
        } else if (ch == '"') {
          quoted = true;
          pendingRecord = true;
        } else if (ch == ',') {
          row.add(field.toString());
          field.setLength(0);
          pendingRecord = true;
        } else if (ch == '\n' || ch == '\r') {
          if (pendingRecord || field.length() > 0) {
            row.add(field.toString());
            field.setLength(0);
            agg.accept(row);
            row = new ArrayList<String>();
            pendingRecord = false;
          }
        } else {
          field.append(ch);
          pendingRecord = true;
        }
        c = r.read();
      }
      if (quoted) {
        throw new IOException("ACA rate review csv ends inside a quoted field");
      }
      if (pendingRecord || field.length() > 0) {
        row.add(field.toString());
        agg.accept(row);
      }
    }
  }

  // ---------------------------------------------------------------------------------------
  // Aggregation
  // ---------------------------------------------------------------------------------------

  /** One (submission, product) accumulation of the plans listed under it. */
  private static final class ProductAcc {
    final String submission;
    final String issuerId;
    final String issuer;
    final String state;
    final String effDate;
    final String productId;
    final String productName;
    final Double productChange;
    final Double submissionChange;
    final String determination;
    final Map<String, Boolean> plans = new HashMap<String, Boolean>();
    long enrollment;

    ProductAcc(String submission, String issuerId, String issuer, String state, String effDate,
        String productId, String productName, Double productChange, Double submissionChange,
        String determination) {
      this.submission = submission;
      this.issuerId = issuerId;
      this.issuer = issuer;
      this.state = state;
      this.effDate = effDate;
      this.productId = productId;
      this.productName = productName;
      this.productChange = productChange;
      this.submissionChange = submissionChange;
      this.determination = determination;
    }
  }

  /** Accumulates WKSH2 rows; the first row it sees is the header. */
  static final class Aggregator {
    private final int planYear;
    private final Map<String, Integer> header = new HashMap<String, Integer>();
    private boolean haveHeader;
    private final Map<String, ProductAcc> products = new LinkedHashMap<String, ProductAcc>();
    private int dataRows;

    Aggregator(int planYear) {
      this.planYear = planYear;
    }

    private String get(List<String> row, String col) {
      int i = header.get(col);
      if (i >= row.size()) {
        return null;
      }
      String v = row.get(i).trim();
      return v.isEmpty() ? null : v;
    }

    void accept(List<String> row) {
      if (!haveHeader) {
        for (int i = 0; i < row.size(); i++) {
          header.put(row.get(i).trim().toUpperCase(Locale.ROOT), i);
        }
        for (String col : REQUIRED) {
          if (!header.containsKey(col)) {
            throw new IllegalStateException("ACA rate review: header column " + col
                + " not found; columns seen: " + header.keySet());
          }
        }
        haveHeader = true;
        return;
      }
      String submission = get(row, COL_SUBMISSION);
      if (submission == null) {
        return; // blank trailing row
      }
      dataRows++;
      if (!INDIVIDUAL.equalsIgnoreCase(get(row, COL_MARKET))) {
        return;
      }
      String productId = get(row, COL_PROD_ID);
      String planId = get(row, COL_PLAN_ID);
      if (productId == null || planId == null) {
        throw new IllegalStateException("ACA rate review: submission " + submission
            + " has a plan row with no product id or plan id");
      }
      String key = submission + "|" + productId;
      ProductAcc acc = products.get(key);
      if (acc == null) {
        acc = new ProductAcc(submission, get(row, COL_ISSUER_ID), get(row, COL_COMPANY),
            get(row, COL_STATE), isoDate(get(row, COL_EFF_DT)), productId,
            get(row, COL_PROD_NAME), number(get(row, COL_RT_PROD)),
            number(get(row, COL_RT_SUB)), get(row, COL_DETERMINATION));
        products.put(key, acc);
      }
      if (acc.plans.put(planId, Boolean.TRUE) != null) {
        return; // the same plan listed twice under the product
      }
      Double enrolled = number(get(row, COL_ENROLL));
      if (enrolled != null) {
        acc.enrollment += Math.round(enrolled);
      }
    }

    List<Map<String, Object>> finish(String url) {
      if (!haveHeader) {
        throw new IllegalStateException("ACA rate review: no header row in " + url);
      }
      if (dataRows == 0) {
        throw new IllegalStateException("ACA rate review: no plan rows in " + url);
      }
      if (products.isEmpty()) {
        throw new IllegalStateException("ACA rate review: no individual-market rows among "
            + dataRows + " plan rows in " + url);
      }
      List<Map<String, Object>> out = new ArrayList<Map<String, Object>>(products.size());
      for (ProductAcc p : products.values()) {
        Map<String, Object> r = new LinkedHashMap<String, Object>();
        r.put("plan_year", planYear);
        r.put("state_abbr", p.state);
        r.put("issuer_id", p.issuerId);
        r.put("issuer_name", p.issuer);
        r.put("submission_id", p.submission);
        r.put("product_id", p.productId);
        r.put("product_name", p.productName);
        r.put("effective_date", p.effDate);
        r.put("rate_change_product_pct", pct(p.productChange));
        r.put("rate_change_submission_pct", pct(p.submissionChange));
        r.put("plan_count", p.plans.size());
        r.put("enrollment", p.enrollment);
        r.put("determination", p.determination);
        out.add(r);
      }
      return out;
    }
  }

  private static Double pct(Double fraction) {
    return fraction == null ? null : Math.round(fraction * 1_000_000d) / 10_000d;
  }

  /** Parses a CMS number such as {@code 0.2373}, {@code 4,148}, {@code $1,000.50}, {@code 5%}. */
  static Double number(String text) {
    if (text == null) {
      return null;
    }
    String t = text.replace("$", "").replace(",", "").trim();
    boolean percent = t.endsWith("%");
    if (percent) {
      t = t.substring(0, t.length() - 1);
    }
    if (t.isEmpty()) {
      return null;
    }
    double v = Double.parseDouble(t);
    return percent ? v / 100d : v;
  }

  /** {@code MM/dd/yyyy} to {@code yyyy-MM-dd}. */
  static String isoDate(String text) {
    if (text == null) {
      return null;
    }
    String[] p = text.split("/");
    if (p.length != 3) {
      throw new IllegalStateException("ACA rate review: unexpected date '" + text + "'");
    }
    return p[2] + "-" + p[0] + "-" + p[1];
  }
}
