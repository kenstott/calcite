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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * DataProvider for {@code snap_benefits_by_geography} — SNAP persons/households
 * participating and total/average monthly benefits, by state x year x month.
 *
 * <p>USDA FNA publishes one ZIP ({@link #ZIP_URL}) covering FY1989-present (a
 * separate national-only file covers FY1969-88, not loaded here — it has no
 * state breakdown; confirmed live 2026-08-24). The ZIP contains one workbook
 * per fiscal year, named {@code FY<2-digit-year>.xls[x]} — 89-99 is 1900s, 00-..
 * is 2000s (see the {@code twoDigit >= 89} check in {@link #fetch}). FY89-FY19
 * are legacy {@code .xls} (BIFF), FY20-present are {@code .xlsx} (OOXML) —
 * POI's {@link WorkbookFactory} handles both transparently. Each workbook has
 * (in the vintages inspected) 8 sheets: 7 FNS regional offices (NERO, MARO,
 * SERO, MWRO, SWRO, MPRO, WRO — some vintages carry a trailing space in the
 * sheet name) plus "US Summary" (skipped here — it is a national rollup, not a
 * state); some older vintages use a different sheet layout, which the
 * state-name-membership guard below tolerates (unrecognized headers are logged
 * and skipped rather than mis-parsed).
 *
 * <p>Within a sheet, states are stacked as blocks, not one-per-sheet: a row
 * with just the state name in column A (and every other tracked column blank)
 * starts a block, followed by 12 monthly rows ({@code "Oct 2023"} ..
 * {@code "Sep 2024"}) and then a {@code "Total"} fiscal-year rollup row (emitted
 * here as month=null). The value-column order is NOT stable across vintages:
 * FY89-FY19 is Household, Persons, Cost/Household, Cost/Person, Cost; FY20+ is
 * Household, Persons, Cost, Cost/Household, Cost/Person — {@link #buildRow}
 * branches on the {@code newLayout} flag, set from the entry's file extension.
 * Not-yet-reported months in the current FY carry the literal string
 * {@code "--"} in every numeric cell.
 *
 * <p>The source has no FIPS or USPS-abbreviation column anywhere, only the
 * English state name as the block header — {@link #STATE_FIPS} resolves it (the
 * mirror image of econ's {@code state_wages} FIPS-&gt;name mapping). A row whose
 * column-A text is non-blank with every value column blank but that is NOT a
 * known state name (title/header preamble text has this same shape in some
 * vintages) is logged and skipped rather than mis-parsed as a state block.
 */
public class SnapBenefitsProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapBenefitsProvider.class);

  private static final String ZIP_URL =
      "https://www.fna.usda.gov/sites/default/files/resource-files/snap-zip-fy69tocurrent-8.zip";

  /** Matches a per-fiscal-year workbook entry inside the zip, e.g. {@code FY24.xlsx}. */
  private static final Pattern FY_ENTRY = Pattern.compile("^FY(\\d{2})\\.xlsx?$");

  /** Matches a monthly data row's label, e.g. {@code "Oct 2023"}. */
  private static final Pattern MONTH_ROW = Pattern.compile(
      "^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \\d{4}$");

  private static final Map<String, Integer> MONTH_NUM = buildMonthMap();

  private static final Map<String, String> STATE_FIPS = buildStateFips();
  private static final Map<String, String> STATE_ABBR = buildStateAbbr();

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    LOGGER.info("snap_benefits_by_geography: fetching {}", ZIP_URL);
    byte[] zipBytes = readBytes(FiscalHttp.openGetWithRetry(ZIP_URL).getInputStream());

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Set<String> unknownNames = new HashSet<String>();
    ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(zipBytes));
    try {
      ZipEntry entry;
      while ((entry = zin.getNextEntry()) != null) {
        Matcher m = FY_ENTRY.matcher(entry.getName());
        if (!m.matches()) {
          continue; // e.g. the FY1969-88 national-only file — no state breakdown, skip
        }
        int twoDigit = Integer.parseInt(m.group(1));
        // The zip goes back to FY89, not just FY00 — 89-99 is 1900s, 00-.. is 2000s.
        int fiscalYear = (twoDigit >= 89 ? 1900 : 2000) + twoDigit;
        boolean newLayout = entry.getName().toLowerCase(Locale.ROOT).endsWith(".xlsx");
        byte[] entryBytes = readBytes(zin);
        parseWorkbook(entryBytes, fiscalYear, newLayout, rows, unknownNames);
      }
    } finally {
      zin.close();
    }
    for (String name : unknownNames) {
      LOGGER.warn("snap_benefits_by_geography: unrecognized state-like block header '{}' "
          + "— skipped (add to STATE_FIPS if this is a real territory)", name);
    }
    LOGGER.info("snap_benefits_by_geography: {} rows", rows.size());
    return rows.iterator();
  }

  private void parseWorkbook(byte[] bytes, int fiscalYear, boolean newLayout,
      List<Map<String, Object>> rows, Set<String> unknownNames) throws IOException {
    Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes));
    try {
      for (int s = 0; s < wb.getNumberOfSheets(); s++) {
        Sheet sheet = wb.getSheetAt(s);
        String sheetName = sheet.getSheetName() == null ? "" : sheet.getSheetName().trim();
        if ("US Summary".equals(sheetName)) {
          continue; // national rollup, not a state
        }
        parseSheet(sheet, fiscalYear, newLayout, rows, unknownNames);
      }
    } finally {
      wb.close();
    }
  }

  private void parseSheet(Sheet sheet, int fiscalYear, boolean newLayout,
      List<Map<String, Object>> rows, Set<String> unknownNames) {
    String currentState = null;
    int last = sheet.getLastRowNum();
    for (int r = 0; r <= last; r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String col0 = str(cell(row, 0));
      if (col0 == null) {
        continue;
      }
      if (isBlank(cell(row, 1)) && isBlank(cell(row, 2)) && isBlank(cell(row, 3))
          && isBlank(cell(row, 4)) && isBlank(cell(row, 5))) {
        // Candidate state-block header — only accept if it's a recognized state name;
        // otherwise it's title/preamble text with the same "label, then blanks" shape.
        if (STATE_FIPS.containsKey(col0.toUpperCase(Locale.ROOT))) {
          currentState = col0;
        } else {
          currentState = null;
          unknownNames.add(col0);
        }
        continue;
      }
      if (currentState == null) {
        continue;
      }
      Matcher mm = MONTH_ROW.matcher(col0);
      Integer month = null;
      if (mm.matches()) {
        month = MONTH_NUM.get(mm.group(1));
      } else if (!"Total".equals(col0)) {
        continue; // not a data row for the current state block
      }
      rows.add(buildRow(currentState, fiscalYear, month, row, newLayout));
    }
  }

  private Map<String, Object> buildRow(String state, int fiscalYear, Integer month, Row row,
      boolean newLayout) {
    // FY00-19 (.xls): Household(1), Persons(2), Cost/Household(3), Cost/Person(4), Cost(5).
    // FY20+  (.xlsx): Household(1), Persons(2), Cost(3), Cost/Household(4), Cost/Person(5).
    Long household = toLong(cell(row, 1));
    Long persons = toLong(cell(row, 2));
    Double cost;
    Double perHousehold;
    Double perPerson;
    if (newLayout) {
      cost = toDouble(cell(row, 3));
      perHousehold = toDouble(cell(row, 4));
      perPerson = toDouble(cell(row, 5));
    } else {
      perHousehold = toDouble(cell(row, 3));
      perPerson = toDouble(cell(row, 4));
      cost = toDouble(cell(row, 5));
    }
    String key = state.toUpperCase(Locale.ROOT);
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("state_fips", STATE_FIPS.get(key));
    m.put("state_abbr", STATE_ABBR.get(key));
    m.put("state_name", state);
    m.put("year", Integer.valueOf(fiscalYear));
    m.put("month", month);
    m.put("households_participating", household);
    m.put("persons_participating", persons);
    m.put("total_benefits_usd", cost);
    m.put("avg_monthly_benefit_per_household_usd", perHousehold);
    m.put("avg_monthly_benefit_per_person_usd", perPerson);
    return m;
  }

  private static boolean isBlank(Cell c) {
    return str(c) == null && toDouble(c) == null;
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
    return null;
  }

  private static Long toLong(Cell cell) {
    Double v = toDouble(cell);
    return v == null ? null : Long.valueOf(v.longValue());
  }

  /** Numeric value of a cell; blank or the "--" not-yet-reported sentinel becomes null. */
  private static Double toDouble(Cell cell) {
    if (cell == null) {
      return null;
    }
    try {
      if (cell.getCellType() == CellType.NUMERIC) {
        return Double.valueOf(cell.getNumericCellValue());
      }
      if (cell.getCellType() == CellType.STRING) {
        String s = cell.getStringCellValue().replace(",", "").trim();
        if (s.isEmpty() || "--".equals(s) || "-".equals(s) || "N/A".equalsIgnoreCase(s)) {
          return null; // not-yet-reported / suppressed
        }
        return Double.valueOf(Double.parseDouble(s));
      }
      if (cell.getCellType() == CellType.FORMULA) {
        return Double.valueOf(cell.getNumericCellValue());
      }
    // fallback-guard: allow — non-numeric/unreadable cell shapes fall through to the
    // documented null-for-unavailable contract, same as the blank/"--"/suppressed cases above
    } catch (Exception e) {
      return null;
    }
    return null;
  }

  private static byte[] readBytes(InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int n;
    while ((n = in.read(buf)) > 0) {
      baos.write(buf, 0, n);
    }
    return baos.toByteArray();
  }

  private static Map<String, Integer> buildMonthMap() {
    Map<String, Integer> m = new HashMap<String, Integer>();
    String[] names = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    for (int i = 0; i < names.length; i++) {
      m.put(names[i], Integer.valueOf(i + 1));
    }
    return Collections.unmodifiableMap(m);
  }

  /** State/DC/PR/VI name (upper-cased) -> 2-digit FIPS. Mirrors econ's state_wages FIPS->name map. */
  private static Map<String, String> buildStateFips() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("ALABAMA", "01"); m.put("ALASKA", "02"); m.put("ARIZONA", "04"); m.put("ARKANSAS", "05");
    m.put("CALIFORNIA", "06"); m.put("COLORADO", "08"); m.put("CONNECTICUT", "09"); m.put("DELAWARE", "10");
    m.put("DISTRICT OF COLUMBIA", "11"); m.put("FLORIDA", "12"); m.put("GEORGIA", "13"); m.put("HAWAII", "15");
    m.put("IDAHO", "16"); m.put("ILLINOIS", "17"); m.put("INDIANA", "18"); m.put("IOWA", "19");
    m.put("KANSAS", "20"); m.put("KENTUCKY", "21"); m.put("LOUISIANA", "22"); m.put("MAINE", "23");
    m.put("MARYLAND", "24"); m.put("MASSACHUSETTS", "25"); m.put("MICHIGAN", "26"); m.put("MINNESOTA", "27");
    m.put("MISSISSIPPI", "28"); m.put("MISSOURI", "29"); m.put("MONTANA", "30"); m.put("NEBRASKA", "31");
    m.put("NEVADA", "32"); m.put("NEW HAMPSHIRE", "33"); m.put("NEW JERSEY", "34"); m.put("NEW MEXICO", "35");
    m.put("NEW YORK", "36"); m.put("NORTH CAROLINA", "37"); m.put("NORTH DAKOTA", "38"); m.put("OHIO", "39");
    m.put("OKLAHOMA", "40"); m.put("OREGON", "41"); m.put("PENNSYLVANIA", "42"); m.put("RHODE ISLAND", "44");
    m.put("SOUTH CAROLINA", "45"); m.put("SOUTH DAKOTA", "46"); m.put("TENNESSEE", "47"); m.put("TEXAS", "48");
    m.put("UTAH", "49"); m.put("VERMONT", "50"); m.put("VIRGINIA", "51"); m.put("WASHINGTON", "53");
    m.put("WEST VIRGINIA", "54"); m.put("WISCONSIN", "55"); m.put("WYOMING", "56");
    m.put("PUERTO RICO", "72"); m.put("VIRGIN ISLANDS", "78"); m.put("GUAM", "66");
    m.put("AMERICAN SAMOA", "60"); m.put("COMMONWEALTH OF NORTHERN MARIANA ISLANDS", "69");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, String> buildStateAbbr() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("ALABAMA", "AL"); m.put("ALASKA", "AK"); m.put("ARIZONA", "AZ"); m.put("ARKANSAS", "AR");
    m.put("CALIFORNIA", "CA"); m.put("COLORADO", "CO"); m.put("CONNECTICUT", "CT"); m.put("DELAWARE", "DE");
    m.put("DISTRICT OF COLUMBIA", "DC"); m.put("FLORIDA", "FL"); m.put("GEORGIA", "GA"); m.put("HAWAII", "HI");
    m.put("IDAHO", "ID"); m.put("ILLINOIS", "IL"); m.put("INDIANA", "IN"); m.put("IOWA", "IA");
    m.put("KANSAS", "KS"); m.put("KENTUCKY", "KY"); m.put("LOUISIANA", "LA"); m.put("MAINE", "ME");
    m.put("MARYLAND", "MD"); m.put("MASSACHUSETTS", "MA"); m.put("MICHIGAN", "MI"); m.put("MINNESOTA", "MN");
    m.put("MISSISSIPPI", "MS"); m.put("MISSOURI", "MO"); m.put("MONTANA", "MT"); m.put("NEBRASKA", "NE");
    m.put("NEVADA", "NV"); m.put("NEW HAMPSHIRE", "NH"); m.put("NEW JERSEY", "NJ"); m.put("NEW MEXICO", "NM");
    m.put("NEW YORK", "NY"); m.put("NORTH CAROLINA", "NC"); m.put("NORTH DAKOTA", "ND"); m.put("OHIO", "OH");
    m.put("OKLAHOMA", "OK"); m.put("OREGON", "OR"); m.put("PENNSYLVANIA", "PA"); m.put("RHODE ISLAND", "RI");
    m.put("SOUTH CAROLINA", "SC"); m.put("SOUTH DAKOTA", "SD"); m.put("TENNESSEE", "TN"); m.put("TEXAS", "TX");
    m.put("UTAH", "UT"); m.put("VERMONT", "VT"); m.put("VIRGINIA", "VA"); m.put("WASHINGTON", "WA");
    m.put("WEST VIRGINIA", "WV"); m.put("WISCONSIN", "WI"); m.put("WYOMING", "WY");
    m.put("PUERTO RICO", "PR"); m.put("VIRGIN ISLANDS", "VI"); m.put("GUAM", "GU");
    m.put("AMERICAN SAMOA", "AS"); m.put("COMMONWEALTH OF NORTHERN MARIANA ISLANDS", "MP");
    return Collections.unmodifiableMap(m);
  }
}
