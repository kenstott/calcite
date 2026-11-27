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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;
import org.apache.calcite.adapter.govdata.HeadedPageSession;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Iterator;

/**
 * TANF caseload by state and month from the ACF Office of Family Assistance workbooks (one
 * {@code tanf-caseload} workbook per fiscal year on {@code acf.gov/ofa/data/tanf-caseload-data-YYYY}).
 * Each workbook carries seven category sheets (families by parent count, recipients by adult and
 * child) with 15 monthly columns; this reads those seven and emits one row per state and month.
 * A month appears in two workbooks (the fiscal year it closes and the next year's first quarter);
 * the later workbook's figure, the revised one, is kept. acf.gov answers non-browser clients with
 * an AWS WAF challenge, so every page and file is fetched through the headed browser session.
 */
public class TanfCaseloadTransformer implements StreamingResponseTransformer {

  private static final String TABLE = "tanf_caseload";
  private static final DateTimeFormatter MONTH_YY =
      DateTimeFormatter.ofPattern("MMM-yy", Locale.US);

  /** Measure column for a category sheet, keyed by a phrase in the sheet's title. */
  private static final String[][] MEASURES = {
      {"two parent families", "two_parent_families"},
      {"one parent families", "one_parent_families"},
      {"no parent families", "no_parent_families"},
      {"total number of families", "total_families"},
      {"adult recipients", "adult_recipients"},
      {"child recipients", "child_recipients"},
      {"total number of recipients", "total_recipients"},
  };

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        TABLE + " is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    int minFy = Integer.parseInt(param(context, "minFiscalYear"));
    int lastFy = currentFiscalYear() + 1;
    HeadedPageSession session = HeadedPageSession.get(context);
    Map<String, Map<String, Object>> rows = new LinkedHashMap<String, Map<String, Object>>();
    int workbooks = 0;
    try {
      for (int fy = minFy; fy <= lastFy; fy++) {
        String pageUrl = AcfWorkbooks.SITE + "/ofa/data/tanf-caseload-data-" + fy;
        String html = session.fetchText(pageUrl);
        if (html == null) {
          // a fiscal year's workbook appears after its first months; only completed years must exist
          if (fy < currentFiscalYear()) {
            throw new IllegalStateException(TABLE + ": " + pageUrl + " answered 404");
          }
          continue;
        }
        String link = tanfWorkbook(AcfWorkbooks.workbookLinks(html), pageUrl);
        byte[] bytes = session.fetchBytes(link);
        if (bytes == null) {
          throw new IllegalStateException(TABLE + ": " + link + " answered 404");
        }
        parse(bytes, link, rows);
        workbooks++;
      }
    } catch (IOException e) {
      throw new RuntimeException(TABLE + ": cannot read ACF caseload workbooks", e);
    }
    if (workbooks == 0 || rows.isEmpty()) {
      throw new IllegalStateException(TABLE + ": no caseload workbooks found from FY" + minFy);
    }
    return rows.values().iterator();
  }

  /** The TANF-only workbook among a page's links (not the SSP-MOE or combined TANF+SSP ones). */
  static String tanfWorkbook(List<String> links, String pageUrl) {
    String found = null;
    for (String url : links) {
      String f = AcfWorkbooks.fileName(url);
      boolean tanfOnly = !f.contains("ssp")
          && (f.contains("tanf-caseload") || f.contains("tanf_caseload") || f.contains("15months_tan"));
      if (tanfOnly) {
        if (found != null) {
          throw new IllegalStateException(TABLE + ": " + pageUrl + " lists two TANF workbooks: "
              + found + " and " + url);
        }
        found = url;
      }
    }
    if (found == null) {
      throw new IllegalStateException(TABLE + ": " + pageUrl + " lists no TANF caseload workbook");
    }
    return found;
  }

  static void parse(byte[] bytes, String source, Map<String, Map<String, Object>> rows)
      throws IOException {
    Workbook wb = AcfWorkbooks.open(bytes);
    try {
      List<String> seen = new ArrayList<String>();
      for (int s = 0; s < wb.getNumberOfSheets(); s++) {
        Sheet sheet = wb.getSheetAt(s);
        String measure = measureFor(AcfWorkbooks.text(sheet.getRow(0), 0));
        if (measure == null || seen.contains(measure)) {
          continue;
        }
        seen.add(measure);
        readSheet(sheet, measure, source, rows);
      }
      if (seen.size() != MEASURES.length) {
        throw new IllegalStateException(TABLE + ": " + source + " has category sheets for "
            + seen + " but " + MEASURES.length + " are expected");
      }
    } finally {
      wb.close();
    }
  }

  private static String measureFor(String title) {
    String t = title.toLowerCase(Locale.ROOT);
    for (String[] m : MEASURES) {
      if (t.contains(m[0])) {
        return m[1];
      }
    }
    return null;
  }

  private static void readSheet(Sheet sheet, String measure, String source,
      Map<String, Map<String, Object>> rows) {
    int header = AcfWorkbooks.findRow(sheet, "State", 10);
    if (header < 0) {
      throw new IllegalStateException(TABLE + ": " + source + " sheet " + sheet.getSheetName()
          + " has no State header row");
    }
    List<LocalDate> months = new ArrayList<LocalDate>();
    Row h = sheet.getRow(header);
    for (int c = 1; c < h.getLastCellNum(); c++) {
      LocalDate d = monthOf(h.getCell(c));
      if (d == null) {
        break;
      }
      months.add(d);
    }
    if (months.isEmpty()) {
      throw new IllegalStateException(TABLE + ": " + source + " sheet " + sheet.getSheetName()
          + " has no month columns");
    }
    for (int r = header + 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      String label = AcfWorkbooks.label(row);
      if (label.isEmpty() || label.toLowerCase(Locale.ROOT).startsWith("notes")) {
        break;
      }
      String code = AcfWorkbooks.stateCode(label);
      for (int i = 0; i < months.size(); i++) {
        LocalDate month = months.get(i);
        String key = code + '|' + month;
        Map<String, Object> out = rows.get(key);
        if (out == null) {
          out = new LinkedHashMap<String, Object>();
          out.put("state_abbr", code);
          out.put("state_name", AcfWorkbooks.stateName(label, code));
          out.put("fiscal_year", month.getMonthValue() >= 10 ? month.getYear() + 1 : month.getYear());
          out.put("year", month.getYear());
          out.put("month", month.getMonthValue());
          rows.put(key, out);
        }
        Double v = AcfWorkbooks.number(row.getCell(1 + i),
            source + " " + sheet.getSheetName() + " row " + (r + 1));
        out.put(measure, v == null ? null : Long.valueOf(Math.round(v)));
        out.put("source_workbook", AcfWorkbooks.fileName(source));
      }
    }
  }

  /** The first day of the month a header cell names (a date cell or text such as "Oct-23"). */
  private static LocalDate monthOf(Cell c) {
    if (c == null) {
      return null;
    }
    CellType type = c.getCellType() == CellType.FORMULA ? c.getCachedFormulaResultType()
        : c.getCellType();
    if (type == CellType.NUMERIC) {
      // the header is an Excel serial date, with or without a date format and sometimes a formula
      java.util.Date serial = DateUtil.getJavaDate(c.getNumericCellValue(),
          java.util.TimeZone.getTimeZone("UTC"));
      LocalDate d = serial.toInstant().atZone(ZoneId.of("UTC")).toLocalDate();
      return d.getDayOfMonth() == 1 ? d : null;
    }
    if (type != CellType.STRING) {
      return null;
    }
    try {
      return YearMonth.parse(AcfWorkbooks.text(c), MONTH_YY).atDay(1);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  private static int currentFiscalYear() {
    LocalDate today = LocalDate.now(ZoneId.of("UTC"));
    return today.getMonthValue() >= 10 ? today.getYear() + 1 : today.getYear();
  }

  private static String param(RequestContext context, String name) {
    String v = context.getParameters().get(name);
    if (v == null || v.trim().isEmpty()) {
      throw new IllegalStateException(TABLE + ": source parameter " + name
          + " is missing from the table source block");
    }
    return v.trim();
  }
}
