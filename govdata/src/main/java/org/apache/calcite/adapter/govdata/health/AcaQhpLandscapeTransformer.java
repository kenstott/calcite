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
// storage-provider-guard:ignore-file - the zip and xlsx are local temp artifacts (POI's OPCPackage
// needs a random-access file); they are deleted in the finally block and never reach storage.
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.SkippedBatchException;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Turns one CMS "QHP Landscape - Individual Market Medical" workbook (one plan year) into
 * {@code health.aca_qhp_county_premiums} rows: one row per plan year, state, county and rating
 * area, aggregated from the plan-by-county rows CMS publishes.
 *
 * <p>The published sheet has one row per (plan, county) — about 90,000 rows and 55-115 MB of XML
 * a year — so the sheet is read with POI's SAX reader and only the per-county aggregates are held
 * in memory. Columns are located by header name, and a missing required header fails the batch
 * instead of shifting to a guessed position.
 *
 * <p>The benchmark is the second-lowest-cost Silver (SLCSP) premium among the distinct Silver
 * plans offered in the county. Plans are counted individually, so two Silver plans tied at the
 * lowest premium make the benchmark equal to that premium. Child-only listings have no adult
 * premium and are skipped. A county with fewer than two Silver plans has no benchmark (null).
 */
public class AcaQhpLandscapeTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AcaQhpLandscapeTransformer.class);

  private static final String COL_STATE = "State Code";
  private static final String COL_FIPS = "FIPS County Code";
  private static final String COL_COUNTY = "County Name";
  private static final String COL_METAL = "Metal Level";
  private static final String COL_ISSUER_ID = "HIOS Issuer ID";
  private static final String COL_PLAN_ID = "Plan ID (Standard Component)";
  private static final String COL_RATING_AREA = "Rating Area";
  private static final String COL_AGE21 = "Premium Adult Individual Age 21";
  private static final String COL_AGE40 = "Premium Adult Individual Age 40";

  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(30))
      .build();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearText = context.getDimensionValues().get("effective_year");
    if (yearText == null) {
      throw new IllegalStateException(
          "ACA QHP landscape: effective_year dimension is required to label the plan year");
    }
    int planYear = Integer.parseInt(yearText.trim());
    String url = context.getUrl();

    Path zip = org.apache.calcite.adapter.file.StagingDirectory
        .newTempFile("aca-qhp-", ".zip").toPath();
    Path workbook = null;
    try {
      download(url, zip);
      try (ZipFile zf = new ZipFile(zip.toFile())) {
        ZipEntry entry = findWorkbook(zf, url);
        workbook = org.apache.calcite.adapter.file.StagingDirectory
            .newTempFile("aca-qhp-", ".xlsx").toPath();
        try (InputStream in = zf.getInputStream(entry)) {
          Files.copy(in, workbook, StandardCopyOption.REPLACE_EXISTING);
        }
      }
      Aggregator agg = new Aggregator(planYear);
      readXlsx(workbook.toFile(), agg);
      List<Map<String, Object>> rows = agg.finish(url);
      LOGGER.debug("ACA QHP landscape {}: {} county rows for plan year {}", url, rows.size(),
          planYear);
      return rows.iterator();
    } finally {
      Files.deleteIfExists(zip);
      if (workbook != null) {
        Files.deleteIfExists(workbook);
      }
    }
  }

  private static ZipEntry findWorkbook(ZipFile zf, String url) throws IOException {
    ZipEntry found = null;
    for (Enumeration<? extends ZipEntry> e = zf.entries(); e.hasMoreElements();) {
      ZipEntry entry = e.nextElement();
      if (entry.getName().toLowerCase(Locale.ROOT).endsWith(".xlsx")) {
        if (found != null) {
          throw new IOException("ACA QHP landscape: more than one workbook in " + url);
        }
        found = entry;
      }
    }
    if (found == null) {
      throw new IOException("ACA QHP landscape: no xlsx workbook in " + url);
    }
    return found;
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
      throw new IOException("Interrupted fetching ACA QHP landscape: " + url, e);
    }
    int code = response.statusCode();
    try (InputStream body = response.body()) {
      if (code == 404) {
        throw new SkippedBatchException("ACA QHP landscape not yet published (HTTP 404): " + url);
      }
      if (code != 200) {
        throw new IOException("ACA QHP landscape download HTTP " + code + ": " + url);
      }
      Files.copy(body, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Aggregation
  // ---------------------------------------------------------------------------------------

  /** Per (state, county, rating area) accumulation of the plans listed there. */
  private static final class CountyAcc {
    final String state;
    final String fips;
    final String county;
    final String ratingArea;
    final Set<String> planIds = new HashSet<String>();
    final Set<String> issuers = new HashSet<String>();
    final Set<String> silverIssuers = new HashSet<String>();
    final List<Double> silver21 = new ArrayList<Double>();
    final List<Double> silver40 = new ArrayList<Double>();
    Double lowestBronze21;
    Double lowestGold21;
    int silverPlans;

    CountyAcc(String state, String fips, String county, String ratingArea) {
      this.state = state;
      this.fips = fips;
      this.county = county;
      this.ratingArea = ratingArea;
    }
  }

  /** Header-aware row consumer; the first row holding {@code State Code} is the header. */
  static final class Aggregator {
    private final int planYear;
    private final Map<String, CountyAcc> counties = new LinkedHashMap<String, CountyAcc>();
    private Map<String, Integer> header;
    private int dataRows;

    Aggregator(int planYear) {
      this.planYear = planYear;
    }

    void accept(List<String> row) {
      if (header == null) {
        if (!row.isEmpty() && COL_STATE.equals(trim(row.get(0)))) {
          Map<String, Integer> h = new HashMap<String, Integer>();
          for (int i = 0; i < row.size(); i++) {
            String name = trim(row.get(i));
            if (name != null) {
              h.put(name, i);
            }
          }
          for (String required : new String[] {COL_STATE, COL_FIPS, COL_COUNTY, COL_METAL,
              COL_ISSUER_ID, COL_PLAN_ID, COL_RATING_AREA, COL_AGE21, COL_AGE40}) {
            if (!h.containsKey(required)) {
              throw new IllegalStateException(
                  "ACA QHP landscape: header has no '" + required + "' column; found "
                      + h.keySet());
            }
          }
          header = h;
        }
        return;
      }
      String state = get(row, COL_STATE);
      String planId = get(row, COL_PLAN_ID);
      if (state == null || planId == null) {
        return; // blank trailing row
      }
      dataRows++;
      String fips = get(row, COL_FIPS);
      String ratingArea = get(row, COL_RATING_AREA);
      String key = state + "|" + fips + "|" + ratingArea;
      CountyAcc acc = counties.get(key);
      if (acc == null) {
        acc = new CountyAcc(state, fips, get(row, COL_COUNTY), ratingArea);
        counties.put(key, acc);
      }
      if (!acc.planIds.add(planId)) {
        return; // the same plan listed twice for the county
      }
      String metal = get(row, COL_METAL);
      String issuer = get(row, COL_ISSUER_ID);
      acc.issuers.add(issuer);
      Double p21 = money(get(row, COL_AGE21));
      Double p40 = money(get(row, COL_AGE40));
      String m = metal == null ? "" : metal.toLowerCase(Locale.ROOT);
      if (m.equals("silver")) {
        acc.silverPlans++;
        acc.silverIssuers.add(issuer);
        if (p21 != null) {
          acc.silver21.add(p21);
        }
        if (p40 != null) {
          acc.silver40.add(p40);
        }
      } else if (m.contains("bronze")) {
        acc.lowestBronze21 = min(acc.lowestBronze21, p21);
      } else if (m.equals("gold")) {
        acc.lowestGold21 = min(acc.lowestGold21, p21);
      }
    }

    List<Map<String, Object>> finish(String url) {
      if (header == null) {
        throw new IllegalStateException("ACA QHP landscape: no header row in " + url);
      }
      if (dataRows == 0) {
        throw new IllegalStateException("ACA QHP landscape: no plan rows in " + url);
      }
      List<Map<String, Object>> out = new ArrayList<Map<String, Object>>(counties.size());
      for (CountyAcc c : counties.values()) {
        Collections.sort(c.silver21);
        Collections.sort(c.silver40);
        Map<String, Object> r = new LinkedHashMap<String, Object>();
        r.put("plan_year", planYear);
        r.put("state_abbr", c.state);
        r.put("county_fips", c.fips);
        r.put("county_name", c.county);
        r.put("rating_area", c.ratingArea);
        r.put("benchmark_silver_premium_age21", second(c.silver21));
        r.put("benchmark_silver_premium_age40", second(c.silver40));
        r.put("lowest_silver_premium_age21", c.silver21.isEmpty() ? null : c.silver21.get(0));
        r.put("lowest_bronze_premium_age21", c.lowestBronze21);
        r.put("lowest_gold_premium_age21", c.lowestGold21);
        r.put("plan_count", c.planIds.size());
        r.put("silver_plan_count", c.silverPlans);
        r.put("issuer_count", c.issuers.size());
        r.put("silver_issuer_count", c.silverIssuers.size());
        out.add(r);
      }
      return out;
    }

    private String get(List<String> row, String column) {
      int i = header.get(column);
      return i < row.size() ? trim(row.get(i)) : null;
    }
  }

  private static Double second(List<Double> sorted) {
    return sorted.size() < 2 ? null : sorted.get(1);
  }

  private static Double min(Double a, Double b) {
    if (b == null) {
      return a;
    }
    return a == null || b < a ? b : a;
  }

  /** Parses "$1,672.24"; the sheet writes NULL (or blank) where a plan has no premium. */
  static Double money(String s) {
    if (s == null || s.equalsIgnoreCase("NULL")) {
      return null;
    }
    String t = s.replace("$", "").replace(",", "").trim();
    return t.isEmpty() ? null : Double.valueOf(t);
  }

  private static String trim(String s) {
    if (s == null) {
      return null;
    }
    String t = s.trim();
    return t.isEmpty() ? null : t;
  }

  // ---------------------------------------------------------------------------------------
  // xlsx (POI SAX)
  // ---------------------------------------------------------------------------------------

  static void readXlsx(File file, Aggregator agg) throws IOException {
    // Sheet XML compresses far below POI's default zip-bomb inflate ratio; the source is a
    // trusted federal publication.
    ZipSecureFile.setMinInflateRatio(0.0);
    try (OPCPackage pkg = OPCPackage.open(file, PackageAccess.READ)) {
      XSSFReader reader = new XSSFReader(pkg);
      ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
      StylesTable styles = reader.getStylesTable();
      XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
      if (!sheets.hasNext()) {
        throw new IOException("ACA QHP landscape: workbook has no sheets: " + file);
      }
      try (InputStream sheet = sheets.next()) {
        XMLReader parser = XMLHelper.newXMLReader();
        // premium cells arrive as formatted text ("$1,672.24"); money() strips the formatting
        parser.setContentHandler(new XSSFSheetXMLHandler(styles, null, strings,
            new RowHandler(agg), new DataFormatter(), false));
        parser.parse(new InputSource(sheet));
      }
    } catch (IOException e) {
      throw e;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("ACA QHP landscape: failed to read xlsx " + file, e);
    }
  }

  private static final class RowHandler implements XSSFSheetXMLHandler.SheetContentsHandler {
    private final Aggregator agg;
    private List<String> current;

    RowHandler(Aggregator agg) {
      this.agg = agg;
    }

    @Override public void startRow(int rowNum) {
      current = new ArrayList<String>();
    }

    @Override public void endRow(int rowNum) {
      agg.accept(current);
    }

    @Override public void cell(String cellReference, String formattedValue,
        XSSFComment comment) {
      int col = new CellReference(cellReference).getCol();
      while (current.size() <= col) {
        current.add(null);
      }
      current.set(col, formattedValue);
    }
  }
}
