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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TANF work participation rates by state and fiscal year from the ACF Office of Family
 * Assistance annual workbook: Table 1A (combined TANF and SSP-MOE rates, adjusted standards and
 * whether the target was met, for all families and two-parent families) and Table 3A (monthly
 * average family counts behind the all-families rate). One row per fiscal year and state, plus
 * the national row. acf.gov answers non-browser clients with an AWS WAF challenge, so pages and
 * files are fetched through the headed browser session.
 */
public class TanfWorkParticipationTransformer implements StreamingResponseTransformer {

  private static final String TABLE = "tanf_work_participation";
  private static final String[] PAGE_PATTERNS = {
      "/ofa/data/work-participation-rates-fiscal-year-",
      "/ofa/data/state-work-participation-rates-fiscal-year-",
  };
  /** The newest fiscal years are not published yet; a gap older than this is an error. */
  private static final int PUBLICATION_LAG_YEARS = 3;

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        TABLE + " is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    int minFy = Integer.parseInt(param(context, "minFiscalYear"));
    int currentFy = currentFiscalYear();
    FiscalBrowserSession session = FiscalBrowserSession.get(context);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try {
      for (int fy = minFy; fy <= currentFy; fy++) {
        String pageUrl = null;
        String html = null;
        for (String pattern : PAGE_PATTERNS) {
          pageUrl = AcfWorkbooks.SITE + pattern + fy;
          html = session.fetchText(pageUrl);
          if (html != null) {
            break;
          }
        }
        if (html == null) {
          if (fy <= currentFy - PUBLICATION_LAG_YEARS) {
            throw new IllegalStateException(TABLE + ": no work participation page for FY" + fy
                + " (last tried " + pageUrl + ")");
          }
          continue;
        }
        String link = workbook(AcfWorkbooks.workbookLinks(html), pageUrl);
        byte[] bytes = session.fetchBytes(link);
        if (bytes == null) {
          throw new IllegalStateException(TABLE + ": " + link + " answered 404");
        }
        rows.addAll(parse(bytes, fy, link));
      }
    } catch (IOException e) {
      throw new RuntimeException(TABLE + ": cannot read ACF work participation workbooks", e);
    }
    if (rows.isEmpty()) {
      throw new IllegalStateException(TABLE + ": no work participation workbooks found from FY"
          + minFy);
    }
    return rows.iterator();
  }

  static String workbook(List<String> links, String pageUrl) {
    String found = null;
    for (String url : links) {
      if (AcfWorkbooks.fileName(url).startsWith("wpr")) {
        if (found != null) {
          throw new IllegalStateException(TABLE + ": " + pageUrl + " lists two workbooks: "
              + found + " and " + url);
        }
        found = url;
      }
    }
    if (found == null) {
      throw new IllegalStateException(TABLE + ": " + pageUrl + " lists no work participation"
          + " workbook");
    }
    return found;
  }

  static List<Map<String, Object>> parse(byte[] bytes, int fiscalYear, String source)
      throws IOException {
    Workbook wb = AcfWorkbooks.open(bytes);
    try {
      Map<String, Map<String, Object>> byState = new LinkedHashMap<String, Map<String, Object>>();
      Sheet t1a = sheet(wb, "1A", source);
      for (List<Cell> v : stateRows(t1a, source)) {
        String label = AcfWorkbooks.text(v.get(0));
        Map<String, Object> out = row(byState, label, fiscalYear, source);
        String where = source + " 1A " + label;
        requireCells(v, 7, 7, where);
        // all-families rate, adjusted standard, met target, then the same three for two-parent
        out.put("all_families_rate", rate(v.get(1), where));
        out.put("all_families_adjusted_standard", rate(v.get(2), where));
        out.put("all_families_met_target", yesNo(v.get(3), where));
        out.put("two_parent_rate", rate(v.get(4), where));
        out.put("two_parent_adjusted_standard", rate(v.get(5), where));
        out.put("two_parent_met_target", yesNo(v.get(6), where));
      }
      Sheet t3a = sheet(wb, "3A", source);
      for (List<Cell> v : stateRows(t3a, source)) {
        String label = AcfWorkbooks.text(v.get(0));
        Map<String, Object> out = byState.get(AcfWorkbooks.stateCode(label));
        if (out == null) {
          throw new IllegalStateException(TABLE + ": " + source + " 3A has '" + label
              + "' which is not in 1A");
        }
        String where = source + " 3A " + label;
        requireCells(v, 8, 9, where);
        int n = v.size();
        out.put("avg_families", AcfWorkbooks.number(v.get(1), where));
        out.put("avg_families_no_work_eligible", AcfWorkbooks.number(v.get(2), where));
        out.put("avg_families_in_rate", AcfWorkbooks.number(v.get(3), where));
        out.put("avg_participating_families", AcfWorkbooks.number(v.get(4), where));
        out.put("avg_disregarded_single_parent_under_1", AcfWorkbooks.number(v.get(n - 3), where));
        out.put("avg_disregarded_sanction", AcfWorkbooks.number(v.get(n - 2), where));
        out.put("avg_disregarded_tribal_work_program", AcfWorkbooks.number(v.get(n - 1), where));
      }
      return new ArrayList<Map<String, Object>>(byState.values());
    } finally {
      wb.close();
    }
  }

  private static Sheet sheet(Workbook wb, String name, String source) {
    Sheet s = wb.getSheet(name);
    if (s == null) {
      throw new IllegalStateException(TABLE + ": " + source + " has no sheet " + name);
    }
    return s;
  }

  /**
   * The state, DC, territory and national rows of a table sheet. Each list starts with the label
   * cell followed by the row's other cells, with the repeated state-label column and the blank
   * cells that merged headers leave removed; callers check the count that remains.
   */
  private static List<List<Cell>> stateRows(Sheet sheet, String source) {
    List<List<Cell>> out = new ArrayList<List<Cell>>();
    for (int r = 0; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      String label = AcfWorkbooks.label(row);
      if (!AcfWorkbooks.isStateLabel(label)) {
        if (!out.isEmpty() && !label.isEmpty()) {
          break;
        }
        continue;
      }
      List<Cell> cells = new ArrayList<Cell>();
      cells.add(row.getCell(0));
      for (int c = 1; c < row.getLastCellNum(); c++) {
        Cell cell = row.getCell(c);
        if (AcfWorkbooks.text(cell).isEmpty() || label.equalsIgnoreCase(AcfWorkbooks.text(cell))) {
          continue;
        }
        cells.add(cell);
      }
      out.add(cells);
    }
    if (out.isEmpty()) {
      throw new IllegalStateException(TABLE + ": " + source + " sheet " + sheet.getSheetName()
          + " has no state rows");
    }
    return out;
  }

  private static void requireCells(List<Cell> v, int min, int max, String where) {
    if (v.size() < min || v.size() > max) {
      throw new IllegalStateException(TABLE + ": " + where + " has " + (v.size() - 1)
          + " value cells, expected " + (min - 1) + " to " + (max - 1));
    }
  }

  private static Map<String, Object> row(Map<String, Map<String, Object>> byState, String label,
      int fiscalYear, String source) {
    String code = AcfWorkbooks.stateCode(label);
    Map<String, Object> out = new LinkedHashMap<String, Object>();
    out.put("fiscal_year", fiscalYear);
    out.put("state_abbr", code);
    out.put("state_name", AcfWorkbooks.stateName(label, code));
    out.put("source_workbook", AcfWorkbooks.fileName(source));
    byState.put(code, out);
    return out;
  }

  /**
   * A rate or standard. The workbooks print "no data" where a rate does not exist (the national
   * row's standard) and a footnote marker such as "1/" where a state has no two-parent rate; both
   * are null.
   */
  private static Double rate(Cell c, String where) {
    String t = AcfWorkbooks.text(c);
    if ("no data".equalsIgnoreCase(t) || t.matches("\\d+/")) {
      return null;
    }
    return AcfWorkbooks.number(c, where);
  }

  private static Boolean yesNo(Cell c, String where) {
    String t = AcfWorkbooks.text(c);
    if (t.isEmpty() || "no data".equalsIgnoreCase(t)) {
      return null;
    }
    if ("yes".equalsIgnoreCase(t)) {
      return Boolean.TRUE;
    }
    if ("no".equalsIgnoreCase(t)) {
      return Boolean.FALSE;
    }
    throw new IllegalStateException(TABLE + ": expected Yes or No at " + where + " but found '"
        + t + "'");
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
