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

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code ssa_benefits_by_geography} — SSA OASDI and SSI
 * beneficiary counts and total benefit dollars by county x program x benefit
 * type x year.
 *
 * <p>SSA publishes these only as multi-sheet XLSX statistical reports on a
 * WAF-protected host that 403s every non-browser client. The un-WAF'd path is the
 * Wayback Machine capture of the same workbooks (see
 * {@link FiscalHttp#fetchViaWayback}). Two workbooks per year:
 * <ul>
 *   <li>{@code oasdi_sc{YY}.xlsx} — one {@code Table 4 - <State>} sheet (county
 *       beneficiary counts) and one {@code Table 5 - <State>} sheet (county
 *       benefit dollars) per state, 14 columns each.</li>
 *   <li>{@code ssi_sc{YY}.xlsx} — one {@code Table 3 - <State>} sheet (county SSI
 *       recipient counts + payment dollars) per state, 11 columns.</li>
 * </ul>
 * The {@code ANSI Code} column carries the county FIPS directly (e.g. 1001 ->
 * 01001), so no name-to-FIPS lookup is needed. Amounts are the December total
 * monthly benefits in thousands of dollars; this provider emits them as whole
 * dollars and derives an average monthly benefit per beneficiary. Counts and
 * dollars are joined per county on the FIPS key. The {@code year} partition comes
 * from the year dimension, so it is not emitted.
 */
public class SsaBenefitsProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SsaBenefitsProvider.class);

  /** Column layout of OASDI Table 4 (counts) and Table 5 (dollars); 0-based. */
  private static final int COL_COUNTY_NAME = 0;
  private static final int COL_ANSI = 2;

  /** OASDI mutually-exclusive benefit-type leaves: {output label, column index}. */
  private static final Object[][] OASDI_TYPES = {
      {"total", Integer.valueOf(3)},
      {"retired_workers", Integer.valueOf(4)},
      {"retired_spouses", Integer.valueOf(5)},
      {"retired_children", Integer.valueOf(6)},
      {"survivors_widows_parents", Integer.valueOf(7)},
      {"survivors_children", Integer.valueOf(8)},
      {"disabled_workers", Integer.valueOf(9)},
      {"disabled_spouses", Integer.valueOf(10)},
      {"disabled_children", Integer.valueOf(11)},
  };

  /** SSI Table 3 count columns: {output label, column index}. Amount is col 10 (total only). */
  private static final Object[][] SSI_TYPES = {
      {"total", Integer.valueOf(3)},
      {"aged", Integer.valueOf(4)},
      {"blind_and_disabled", Integer.valueOf(5)},
  };
  private static final int SSI_AMOUNT_COL = 10;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("ssa_benefits_by_geography: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    String yr = year.trim();
    String yy = FiscalHttp.twoDigitYear(yr);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();

    parseOasdi(yr, yy, rows);
    parseSsi(yr, yy, rows);

    LOGGER.info("ssa_benefits_by_geography: {} county-benefit rows for year {}", rows.size(), yr);
    return rows.iterator();
  }

  private void parseOasdi(String yr, String yy, List<Map<String, Object>> rows) throws IOException {
    String url = "https://www.ssa.gov/policy/docs/statcomps/oasdi_sc/" + yr + "/oasdi_sc" + yy + ".xlsx";
    LOGGER.info("ssa_benefits_by_geography: OASDI via Wayback for {}", url);
    byte[] bytes = FiscalHttp.fetchViaWayback(url);
    Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes));
    try {
      for (int s = 0; s < wb.getNumberOfSheets(); s++) {
        Sheet counts = wb.getSheetAt(s);
        String name = counts.getSheetName();
        if (name == null || !name.startsWith("Table 4 - ")) {
          continue;
        }
        String state = name.substring("Table 4 - ".length()).trim();
        Sheet amounts = wb.getSheet("Table 5 - " + state);
        if (amounts == null) {
          LOGGER.warn("ssa_benefits_by_geography: no Table 5 for {} — skipping", state);
          continue;
        }
        Map<String, double[]> countByFips = readOasdiSheet(counts);
        Map<String, double[]> amountByFips = readOasdiSheet(amounts);
        Map<String, String> nameByFips = readCountyNames(counts);
        for (Map.Entry<String, double[]> e : countByFips.entrySet()) {
          String fips = e.getKey();
          double[] cnt = e.getValue();
          double[] amt = amountByFips.get(fips);
          for (Object[] type : OASDI_TYPES) {
            int col = ((Integer) type[1]).intValue();
            Long num = cnt != null ? toLongVal(cnt, col) : null;
            Double dollars = amt != null ? toDollars(amt, col) : null;
            rows.add(row("OASDI", fips, state, nameByFips.get(fips), (String) type[0], num, dollars));
          }
        }
      }
    } finally {
      wb.close();
    }
  }

  private void parseSsi(String yr, String yy, List<Map<String, Object>> rows) throws IOException {
    String url = "https://www.ssa.gov/policy/docs/statcomps/ssi_sc/" + yr + "/ssi_sc" + yy + ".xlsx";
    LOGGER.info("ssa_benefits_by_geography: SSI via Wayback for {}", url);
    byte[] bytes = FiscalHttp.fetchViaWayback(url);
    Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes));
    try {
      for (int s = 0; s < wb.getNumberOfSheets(); s++) {
        Sheet sheet = wb.getSheetAt(s);
        String name = sheet.getSheetName();
        if (name == null || !name.startsWith("Table 3 - ")) {
          continue;
        }
        String state = name.substring("Table 3 - ".length()).trim();
        int last = sheet.getLastRowNum();
        for (int r = 0; r <= last; r++) {
          Row row = sheet.getRow(r);
          if (row == null) {
            continue;
          }
          String fips = countyFips(row);
          if (fips == null) {
            continue;
          }
          String countyName = str(cell(row, COL_COUNTY_NAME));
          // SSI: total carries payment dollars (col 10); category counts have no per-category $.
          for (Object[] type : SSI_TYPES) {
            int col = ((Integer) type[1]).intValue();
            Long num = toLong(cell(row, col));
            Double dollars = "total".equals(type[0]) ? toDollars1000(cell(row, SSI_AMOUNT_COL)) : null;
            rows.add(row("SSI", fips, state, countyName, (String) type[0], num, dollars));
          }
        }
      }
    } finally {
      wb.close();
    }
  }

  /** Reads an OASDI sheet into county_fips -> per-column values (index 0..13). */
  private Map<String, double[]> readOasdiSheet(Sheet sheet) {
    Map<String, double[]> out = new LinkedHashMap<String, double[]>();
    int last = sheet.getLastRowNum();
    for (int r = 0; r <= last; r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String fips = countyFips(row);
      if (fips == null) {
        continue;
      }
      double[] vals = new double[14];
      java.util.Arrays.fill(vals, Double.NaN);
      for (int c = 3; c < 14; c++) {
        Double v = toDoubleCell(cell(row, c));
        if (v != null) {
          vals[c] = v.doubleValue();
        }
      }
      out.put(fips, vals);
    }
    return out;
  }

  private Map<String, String> readCountyNames(Sheet sheet) {
    Map<String, String> out = new LinkedHashMap<String, String>();
    int last = sheet.getLastRowNum();
    for (int r = 0; r <= last; r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String fips = countyFips(row);
      if (fips != null) {
        out.put(fips, str(cell(row, COL_COUNTY_NAME)));
      }
    }
    return out;
  }

  /**
   * Returns the 5-digit county FIPS for a data row, or null for header / state-total /
   * footnote rows. A county row has a non-blank county name AND an ANSI Code >= 1000
   * (state totals use the 1-2 digit state FIPS; headers are non-numeric).
   */
  private static String countyFips(Row row) {
    String cname = str(cell(row, COL_COUNTY_NAME));
    if (cname == null) {
      return null;
    }
    Double ansi = toDoubleCell(cell(row, COL_ANSI));
    if (ansi == null) {
      return null;
    }
    long code = (long) ansi.doubleValue();
    if (code < 1000) {
      return null;  // state total (ANSI = state FIPS) or non-county aggregate
    }
    return FiscalHttp.pad(String.valueOf(code), 5);
  }

  private Map<String, Object> row(String program, String fips, String state, String countyName,
      String type, Long num, Double dollars) {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("program", program);
    m.put("state_fips", fips.substring(0, 2));
    m.put("state_name", state);
    m.put("county_fips", fips);
    m.put("county_name", countyName);
    m.put("beneficiary_type", type);
    m.put("num_beneficiaries", num);
    m.put("total_benefits_usd", dollars);
    Double avg = null;
    if (dollars != null && num != null && num.longValue() > 0) {
      avg = Double.valueOf(dollars.doubleValue() / num.doubleValue());
    }
    m.put("avg_monthly_benefit_usd", avg);
    return m;
  }

  private static Long toLongVal(double[] vals, int col) {
    if (col >= vals.length || Double.isNaN(vals[col])) {
      return null;
    }
    return Long.valueOf((long) vals[col]);
  }

  /** OASDI amounts are in thousands of dollars; return whole dollars. */
  private static Double toDollars(double[] vals, int col) {
    if (col >= vals.length || Double.isNaN(vals[col])) {
      return null;
    }
    return Double.valueOf(vals[col] * 1000.0);
  }

  private static Double toDollars1000(Cell cell) {
    Double v = toDoubleCell(cell);
    return v == null ? null : Double.valueOf(v.doubleValue() * 1000.0);
  }

  private static Cell cell(Row row, int i) {
    return row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
  }

  private static String str(Cell cell) {
    if (cell == null) {
      return null;
    }
    if (cell.getCellType() == CellType.STRING) {
      String s = cell.getStringCellValue();
      return (s == null || s.trim().isEmpty()) ? null : s.trim();
    }
    if (cell.getCellType() == CellType.NUMERIC) {
      return String.valueOf(cell.getNumericCellValue());
    }
    return null;
  }

  private static Long toLong(Cell cell) {
    Double v = toDoubleCell(cell);
    return v == null ? null : Long.valueOf(v.longValue());
  }

  /** Numeric value of a cell, tolerating numeric-as-text; blank/suppressed -> null. */
  private static Double toDoubleCell(Cell cell) {
    if (cell == null) {
      return null;
    }
    try {
      if (cell.getCellType() == CellType.NUMERIC) {
        return Double.valueOf(cell.getNumericCellValue());
      }
      if (cell.getCellType() == CellType.STRING) {
        String s = cell.getStringCellValue().replace(",", "").trim();
        if (s.isEmpty() || "-".equals(s) || "(X)".equals(s) || "N/A".equalsIgnoreCase(s)) {
          return null;  // SSA suppression markers
        }
        return Double.valueOf(Double.parseDouble(s));
      }
      if (cell.getCellType() == CellType.FORMULA) {
        return Double.valueOf(cell.getNumericCellValue());
      }
    } catch (Exception e) {
      return null;
    }
    return null;
  }
}
