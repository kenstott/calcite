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
// storage-provider-guard:ignore-file - audited: only local temp files holding the downloaded workbook.
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.CrossProcessRateLimiter;
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
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the New York Fed "Quarterly Report on Household Debt and Credit" data workbook
 * ({@code hhd_c_report_<YYYY>q<Q>.xlsx}) and emits the {@code econ.household_debt_credit}
 * rows for one calendar year of the history the workbook carries (2003Q1 onward).
 *
 * <p>Every release workbook holds the full series, so the transformer locates the newest
 * published workbook by probing back from the current quarter (a not-yet-published quarter is
 * a 302 or 404), downloads it once per JVM, and emits only the rows whose quarter falls in the
 * {@code year} dimension. Five national data sheets are read, each laid out as a header row
 * (column A empty, series names in B..) followed by one row per quarter labelled {@code YY:Qn}:
 * page 3 (balance in trillions of dollars by loan type), page 11 (share of balance by
 * delinquency status), page 12 (percent of balance 90+ days delinquent), page 13 (flow into
 * early delinquency, 30+) and page 14 (flow into serious delinquency, 90+). An empty cell (a
 * series that does not yet exist for that quarter) yields no row, not a zero.
 */
public class HouseholdDebtCreditTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(HouseholdDebtCreditTransformer.class);

  private static final String RATE_LIMIT_KEY = "www.newyorkfed.org";
  private static final long MIN_REQUEST_INTERVAL_MS = 2_000L;
  /** How many quarters back from the current one to probe for the newest workbook. */
  private static final int MAX_PROBE_QUARTERS = 6;

  private static final Pattern QUARTER_LABEL = Pattern.compile("^(\\d{2}):Q([1-4])$");

  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(30))
      .build();

  /** One data sheet: its name, the metric it carries, and how its header maps to rows. */
  static final class SheetSpec {
    final String sheet;
    final String metric;
    final String unit;
    /** True when the header columns are delinquency statuses rather than loan types. */
    final boolean byStatus;
    final Map<String, String> headerToCategory;

    SheetSpec(String sheet, String metric, String unit, boolean byStatus,
        Map<String, String> headerToCategory) {
      this.sheet = sheet;
      this.metric = metric;
      this.unit = unit;
      this.byStatus = byStatus;
      this.headerToCategory = headerToCategory;
    }
  }

  private static Map<String, String> map(String... kv) {
    Map<String, String> m = new LinkedHashMap<String, String>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  static final List<SheetSpec> SHEETS;

  static {
    List<SheetSpec> l = new ArrayList<SheetSpec>();
    l.add(new SheetSpec("Page 3 Data", "balance", "trillions_usd", false,
        map("mortgage", "mortgage", "he revolving", "heloc", "auto loan", "auto",
            "credit card", "credit_card", "student loan", "student_loan", "other", "other",
            "total", "total")));
    l.add(new SheetSpec("Page 11 Data", "balance_share_by_delinquency_status", "percent", true,
        map("current", "current", "30 days late", "30_days_late", "60 days late",
            "60_days_late", "90 days late", "90_days_late", "120+ days late",
            "120_plus_days_late", "severely derogatory", "severely_derogatory",
            "total", "total")));
    Map<String, String> byLoan = map("mortgage", "mortgage", "heloc", "heloc", "auto", "auto",
        "cc", "credit_card", "student loan", "student_loan", "other", "other", "all", "total");
    l.add(new SheetSpec("Page 12 Data", "pct_balance_90plus_delinquent", "percent", false,
        byLoan));
    // page 13 labels its all-debt column "Total" where pages 12 and 14 say "ALL"
    Map<String, String> byLoanTotal = new LinkedHashMap<String, String>(byLoan);
    byLoanTotal.remove("all");
    byLoanTotal.put("total", "total");
    l.add(new SheetSpec("Page 13 Data", "flow_into_early_delinquency_30plus", "percent", false,
        byLoanTotal));
    l.add(new SheetSpec("Page 14 Data", "flow_into_serious_delinquency_90plus", "percent",
        false, byLoan));
    SHEETS = java.util.Collections.unmodifiableList(l);
  }

  /** Newest workbook, downloaded once per JVM: the file path and the release label. */
  private static Path cachedWorkbook;
  private static String cachedRelease;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearText = context.getDimensionValues().get("year");
    if (yearText == null) {
      throw new IllegalStateException(
          "household_debt_credit: the year dimension is required to select quarters");
    }
    int year = Integer.parseInt(yearText);
    File workbook = ensureWorkbook(context.getUrl(), context.getHeaders()).toFile();
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (SheetSpec spec : SHEETS) {
      SheetCollector collector = new SheetCollector(spec, year, cachedRelease);
      readSheet(workbook, spec.sheet, collector);
      collector.finish();
      rows.addAll(collector.rows);
    }
    LOGGER.debug("household_debt_credit {}: {} rows for {} from {}", cachedRelease, rows.size(),
        year, workbook);
    return rows.iterator();
  }

  private static synchronized Path ensureWorkbook(String baseUrl, Map<String, String> headers)
      throws IOException {
    if (cachedWorkbook != null && Files.exists(cachedWorkbook)) {
      return cachedWorkbook;
    }
    LocalDate today = LocalDate.now(ZoneId.of("America/New_York"));
    int y = today.getYear();
    int q = (today.getMonthValue() - 1) / 3 + 1;
    String base = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
    for (int i = 0; i < MAX_PROBE_QUARTERS; i++) {
      String release = y + "q" + q;
      String url = base + "hhd_c_report_" + release + ".xlsx";
      Path target = org.apache.calcite.adapter.file.StagingDirectory
          .newTempFile("hhdc-" + release + "-", ".xlsx").toPath();
      if (download(url, headers, target)) {
        cachedWorkbook = target;
        cachedRelease = release;
        return target;
      }
      Files.deleteIfExists(target);
      q--;
      if (q == 0) {
        q = 4;
        y--;
      }
    }
    throw new SkippedBatchException("household_debt_credit: no hhd_c_report workbook found "
        + "for the latest " + MAX_PROBE_QUARTERS + " quarters under " + base);
  }

  /** Returns true on HTTP 200 (body written), false on 404 or a redirect, throws on anything else. */
  private static boolean download(String url, Map<String, String> headers, Path target)
      throws IOException {
    CrossProcessRateLimiter.acquire(RATE_LIMIT_KEY, MIN_REQUEST_INTERVAL_MS);
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMinutes(5))
        .GET();
    if (headers != null) {
      for (Map.Entry<String, String> h : headers.entrySet()) {
        builder.header(h.getKey(), h.getValue());
      }
    }
    HttpResponse<InputStream> response;
    try {
      response = CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted fetching household debt workbook: " + url, e);
    }
    int code = response.statusCode();
    try (InputStream body = response.body()) {
      // The NY Fed answers an unpublished quarter with a 302 to its error page (this client
      // does not follow redirects) rather than a 404; both mean "not published yet".
      if (code == 404 || (code >= 300 && code < 400)) {
        return false;
      }
      if (code != 200) {
        throw new IOException("household debt workbook download HTTP " + code + ": " + url);
      }
      Files.copy(body, target, StandardCopyOption.REPLACE_EXISTING);
      return true;
    }
  }

  // ---------------------------------------------------------------------------------------

  /** Converts one data sheet's rows to output rows for the requested year. */
  static final class SheetCollector {
    private final SheetSpec spec;
    private final int year;
    private final String release;
    final List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    private Map<Integer, String> categories;
    private int quarters;

    SheetCollector(SheetSpec spec, int year, String release) {
      this.spec = spec;
      this.year = year;
      this.release = release;
    }

    void accept(List<String> cells) {
      String first = cells.isEmpty() ? null : cells.get(0);
      if (categories == null) {
        if ((first == null || first.trim().isEmpty()) && cells.size() > 1
            && cells.get(1) != null && !cells.get(1).trim().isEmpty()) {
          readHeader(cells);
        }
        return;
      }
      if (first == null) {
        return;
      }
      Matcher m = QUARTER_LABEL.matcher(first.trim());
      if (!m.matches()) {
        return;
      }
      quarters++;
      int rowYear = 2000 + Integer.parseInt(m.group(1));
      if (rowYear != year) {
        return;
      }
      int quarter = Integer.parseInt(m.group(2));
      for (Map.Entry<Integer, String> c : categories.entrySet()) {
        int col = c.getKey().intValue();
        String raw = col < cells.size() ? cells.get(col) : null;
        if (raw == null || raw.trim().isEmpty()) {
          continue;
        }
        double value;
        try {
          value = Double.parseDouble(raw.trim().replace(",", ""));
        } catch (NumberFormatException e) {
          throw new IllegalStateException("household_debt_credit " + spec.sheet
              + ": non-numeric value '" + raw + "' at " + first + " column " + c.getValue(), e);
        }
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("metric", spec.metric);
        row.put("loan_type", spec.byStatus ? "total" : c.getValue());
        row.put("delinquency_status", spec.byStatus ? c.getValue() : null);
        row.put("year", Integer.valueOf(year));
        row.put("quarter", Integer.valueOf(quarter));
        row.put("period", year + "Q" + quarter);
        row.put("value", Double.valueOf(value));
        row.put("unit", spec.unit);
        row.put("release", release);
        rows.add(row);
      }
    }

    private void readHeader(List<String> cells) {
      Map<Integer, String> found = new LinkedHashMap<Integer, String>();
      for (int i = 1; i < cells.size(); i++) {
        String h = cells.get(i);
        if (h == null || h.trim().isEmpty()) {
          continue;
        }
        String cat = spec.headerToCategory.get(h.trim().toLowerCase(Locale.ROOT));
        if (cat == null) {
          throw new IllegalStateException("household_debt_credit " + spec.sheet
              + ": unrecognised header '" + h + "'; known: "
              + spec.headerToCategory.keySet());
        }
        found.put(Integer.valueOf(i), cat);
      }
      if (found.size() != spec.headerToCategory.size()) {
        throw new IllegalStateException("household_debt_credit " + spec.sheet
            + ": header has " + found.size() + " known columns, expected "
            + spec.headerToCategory.size());
      }
      categories = found;
    }

    void finish() {
      if (categories == null) {
        throw new IllegalStateException("household_debt_credit " + spec.sheet
            + ": header row not found");
      }
      if (quarters == 0) {
        throw new IllegalStateException("household_debt_credit " + spec.sheet
            + ": no quarter rows (YY:Qn) found");
      }
    }
  }

  static void readSheet(File file, String sheetName, final SheetCollector collector)
      throws IOException {
    ZipSecureFile.setMinInflateRatio(0.0);
    try (OPCPackage pkg = OPCPackage.open(file, PackageAccess.READ)) {
      XSSFReader reader = new XSSFReader(pkg);
      ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
      StylesTable styles = reader.getStylesTable();
      XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
      boolean found = false;
      while (sheets.hasNext()) {
        try (InputStream sheet = sheets.next()) {
          if (!sheetName.equals(sheets.getSheetName())) {
            continue;
          }
          found = true;
          XMLReader parser = XMLHelper.newXMLReader();
          // The workbook displays values rounded (for example one decimal on page 11), so the
          // formatter returns the stored full-precision number, not the display text.
          parser.setContentHandler(new XSSFSheetXMLHandler(styles, null, strings,
              new RowHandler(collector), new RawNumberFormatter(), false));
          parser.parse(new InputSource(sheet));
        }
        break;
      }
      if (!found) {
        throw new IOException("household_debt_credit: workbook has no sheet '" + sheetName
            + "': " + file);
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("household_debt_credit: failed to read " + file, e);
    }
  }

  /** Returns numeric cells at full stored precision regardless of their display format. */
  private static final class RawNumberFormatter extends DataFormatter {
    @Override public String formatRawCellContents(double value, int formatIndex,
        String formatString) {
      return Double.toString(value);
    }
  }

  private static final class RowHandler implements XSSFSheetXMLHandler.SheetContentsHandler {
    private final SheetCollector collector;
    private List<String> current;

    RowHandler(SheetCollector collector) {
      this.collector = collector;
    }

    @Override public void startRow(int rowNum) {
      current = new ArrayList<String>();
    }

    @Override public void endRow(int rowNum) {
      collector.accept(current);
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
