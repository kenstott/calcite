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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Parses the FHWA Highway Statistics <b>MV-1</b> table (state motor-vehicle
 * registrations) into {@code vehicle_registrations} rows.
 *
 * <p>FHWA publishes MV-1 only as a single-sheet Excel workbook with a 9-row
 * merged multi-level header (verified 2026-07-13, sheet {@code CY MV-1 Publish};
 * data starts at row 10, 1-indexed). Column layout is fixed and positional:
 * <pre>
 *   A(0)  STATE          (name, with footnote markers like "Alabama (2)")
 *   D(3)  automobiles total   G(6)  buses total     J(9)  trucks total
 *   M(12) motorcycles total   P(15) all motor vehicles total
 * </pre>
 *
 * <p>The workbook is located and downloaded by {@link FhwaHighwayStatistics}; the source URL of
 * the table is not read. It is parsed with POI via {@link WorkbookFactory} (content-sniffing, so
 * it handles {@code .xls} and {@code .xlsx}). Some years store the measures as formulas, whose
 * cached results are the published values.
 *
 * <p>State FIPS is derived from the cleaned state name for the geo join; the
 * {@code year} partition comes from the {@code effective_year} dimension, so it
 * is not emitted here.
 */
public class FhwaVehicleRegistrationsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FhwaVehicleRegistrationsTransformer.class);

  private static final String SHEET_NAME = "CY MV-1 Publish";
  private static final int FIRST_DATA_ROW = 9;   // 0-indexed row 10
  private static final int COL_STATE = 0;
  private static final int COL_AUTOMOBILES = 3;
  private static final int COL_BUSES = 6;
  private static final int COL_TRUCKS = 9;
  private static final int COL_MOTORCYCLES = 12;
  private static final int COL_ALL = 15;

  /** Strips a trailing footnote marker such as " (2)" from a state name. */
  private static final Pattern FOOTNOTE = Pattern.compile("\\s*\\(\\d+\\)\\s*$");

  static final Map<String, String> STATE_FIPS = buildStateFips();

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "vehicle_registrations is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    int year = Integer.parseInt(context.getDimensionValues().get("effective_year"));
    FhwaHighwayStatistics.Workbook source =
        FhwaHighwayStatistics.fetch(context, "mv1", "xlsx", year);
    try {
      List<Map<String, Object>> rows = parseWorkbook(source.bytes, source.url);
      LOGGER.debug("vehicle_registrations: {} state rows from {}", rows.size(), source.url);
      return rows.iterator();
    } catch (IOException e) {
      throw new RuntimeException("vehicle_registrations: failed to parse MV-1 from " + source.url
          + ": " + e.getMessage(), e);
    }
  }

  static List<Map<String, Object>> parseWorkbook(byte[] bytes, String url) throws IOException {
    Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes));
    try {
      Sheet sheet = wb.getSheet(SHEET_NAME);
      if (sheet == null) {
        sheet = wb.getSheetAt(0);
      }
      List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
      int last = sheet.getLastRowNum();
      for (int r = FIRST_DATA_ROW; r <= last; r++) {
        Row row = sheet.getRow(r);
        if (row == null) {
          continue;
        }
        String rawState = cellString(row.getCell(COL_STATE));
        if (rawState == null) {
          continue;
        }
        String stateName = FOOTNOTE.matcher(rawState.trim()).replaceAll("").trim();
        String fips = STATE_FIPS.get(stateName.toUpperCase(Locale.ROOT));
        if (fips == null) {
          // Skips the "Total" row, footnote text rows, and any non-state row.
          continue;
        }
        Map<String, Object> o = new LinkedHashMap<String, Object>();
        o.put("state_fips", fips);
        o.put("state_name", stateName);
        o.put("automobiles", measure(row.getCell(COL_AUTOMOBILES), stateName, url));
        o.put("buses", measure(row.getCell(COL_BUSES), stateName, url));
        o.put("trucks", measure(row.getCell(COL_TRUCKS), stateName, url));
        o.put("motorcycles", measure(row.getCell(COL_MOTORCYCLES), stateName, url));
        o.put("all_motor_vehicles", measure(row.getCell(COL_ALL), stateName, url));
        out.add(o);
      }
      if (out.isEmpty()) {
        throw new IOException("no state rows in " + url);
      }
      return out;
    } finally {
      wb.close();
    }
  }

  private static String cellString(Cell cell) {
    if (cell == null) {
      return null;
    }
    if (cell.getCellType() == CellType.STRING) {
      String s = cell.getStringCellValue();
      return s == null || s.trim().isEmpty() ? null : s;
    }
    if (cell.getCellType() == CellType.NUMERIC) {
      return String.valueOf(cell.getNumericCellValue());
    }
    return null;
  }

  /** The count in a measure cell; null only when the cell is blank. */
  private static Long measure(Cell cell, String state, String url) throws IOException {
    if (cell == null || cell.getCellType() == CellType.BLANK) {
      return null;
    }
    CellType type = cell.getCellType() == CellType.FORMULA
        ? cell.getCachedFormulaResultType() : cell.getCellType();
    if (type == CellType.NUMERIC) {
      return Math.round(cell.getNumericCellValue());
    }
    throw new IOException("non-numeric measure in cell " + cell.getAddress() + " for " + state
        + " in " + url);
  }

  private static Map<String, String> buildStateFips() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("ALABAMA", "01");
    m.put("ALASKA", "02");
    m.put("ARIZONA", "04");
    m.put("ARKANSAS", "05");
    m.put("CALIFORNIA", "06");
    m.put("COLORADO", "08");
    m.put("CONNECTICUT", "09");
    m.put("DELAWARE", "10");
    m.put("DISTRICT OF COLUMBIA", "11");
    m.put("DIST. OF COL.", "11");
    m.put("FLORIDA", "12");
    m.put("GEORGIA", "13");
    m.put("HAWAII", "15");
    m.put("IDAHO", "16");
    m.put("ILLINOIS", "17");
    m.put("INDIANA", "18");
    m.put("IOWA", "19");
    m.put("KANSAS", "20");
    m.put("KENTUCKY", "21");
    m.put("LOUISIANA", "22");
    m.put("MAINE", "23");
    m.put("MARYLAND", "24");
    m.put("MASSACHUSETTS", "25");
    m.put("MICHIGAN", "26");
    m.put("MINNESOTA", "27");
    m.put("MISSISSIPPI", "28");
    m.put("MISSOURI", "29");
    m.put("MONTANA", "30");
    m.put("NEBRASKA", "31");
    m.put("NEVADA", "32");
    m.put("NEW HAMPSHIRE", "33");
    m.put("NEW JERSEY", "34");
    m.put("NEW MEXICO", "35");
    m.put("NEW YORK", "36");
    m.put("NORTH CAROLINA", "37");
    m.put("NORTH DAKOTA", "38");
    m.put("OHIO", "39");
    m.put("OKLAHOMA", "40");
    m.put("OREGON", "41");
    m.put("PENNSYLVANIA", "42");
    m.put("RHODE ISLAND", "44");
    m.put("SOUTH CAROLINA", "45");
    m.put("SOUTH DAKOTA", "46");
    m.put("TENNESSEE", "47");
    m.put("TEXAS", "48");
    m.put("UTAH", "49");
    m.put("VERMONT", "50");
    m.put("VIRGINIA", "51");
    m.put("WASHINGTON", "53");
    m.put("WEST VIRGINIA", "54");
    m.put("WISCONSIN", "55");
    m.put("WYOMING", "56");
    m.put("PUERTO RICO", "72");
    return Collections.unmodifiableMap(m);
  }
}
