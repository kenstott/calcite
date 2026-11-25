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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.energy.EiaBulkXlsxTransformer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Transforms the SSA "OASDI Beneficiaries by State and County" XLSX files into
 * {@code health.ssa_oasdi_county} rows — one row per county or independent city (plus one
 * state-total row) per state per year, wide across the 8 beneficiary-type breakdowns the tables
 * publish. The {@code area_file} dimension names the workbook being read.
 *
 * <p>A state workbook carries Table 4 (counts) and Table 5 (amounts) with an identical row layout:
 * column A = county name (blank on the state-total row), column B = "Total, &lt;State&gt;" label
 * (state-total row only), column C = SSA's ANSI/FIPS code (the state's own FIPS number, or that
 * number times 1000, on the total row; the 5-digit state+county FIPS without a leading zero on a
 * county row), then the 8 beneficiary-type columns. States with independent cities put
 * "Counties" and "Independent city" sub-header rows between the data rows. Rows are joined between
 * the two sheets by the ANSI code.
 *
 * <p>SSA publishes no District of Columbia workbook. DC is read from its row in the national
 * state-level files, Table 2 (counts, {@code table02.xlsx}) and Table 3 (amounts,
 * {@code table03.xlsx}), and emitted as its state-total row plus its single county equivalent
 * (FIPS 11001).
 *
 * <p>Deliberately excludes the "Aged 65 or older, by sex" (Men/Women) columns the same tables
 * publish — a different demographic cross-tab of the same totals, not part of the
 * total/beneficiary-type breakdown this table carries.
 */
public class SsaOasdiCountyTransformer extends EiaBulkXlsxTransformer {

  private static final int DATA_START_ROW = 4; // 0-indexed; row 5 in the spreadsheet

  /** {@code area_file} value of the national workbook DC is read from. */
  private static final String DC_AREA_FILE = "table02";
  private static final String DC_NAME = "District of Columbia";
  private static final String DC_COUNTY_FIPS = "11001";

  @Override
  public String transform(String response, RequestContext context) {
    // www.ssa.gov's Akamai gate 403s both a blank UA and the base class's own "GovData/1.0"
    // (confirmed live) but passes "Wget/1.21.3" — the opposite gotcha from CDC WONDER/BLS, which
    // block a self-identifying UA. Overriding the whole download here (not just parseWorkbook)
    // to control the User-Agent, since EiaBulkXlsxTransformer.downloadBytes() hardcodes its own.
    String url = context.getUrl();
    try {
      byte[] bytes = downloadWithWgetUa(url);
      XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(bytes));
      try {
        return parseWorkbook(workbook, context);
      } finally {
        workbook.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("SSA OASDI county: failed to parse XLSX from " + url, e);
    }
  }

  private byte[] downloadWithWgetUa(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "Wget/1.21.3");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (InputStream is = conn.getInputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
    }
    return baos.toByteArray();
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception {
    String areaFile = context.getDimensionValues().get("area_file");
    int year = Integer.parseInt(context.getDimensionValues().get("effective_year").trim());

    ArrayNode result;
    if (DC_AREA_FILE.equals(areaFile)) {
      String amountsUrl = context.getUrl().replace("table02.xlsx", "table03.xlsx");
      XSSFWorkbook amounts =
          new XSSFWorkbook(new ByteArrayInputStream(downloadWithWgetUa(amountsUrl)));
      try {
        result = dcRows(workbook, amounts, year);
      } finally {
        amounts.close();
      }
    } else {
      result = stateRows(workbook, areaFile.toUpperCase(Locale.ROOT), year);
    }
    LOGGER.debug("SSA OASDI county: emitted {} rows for area_file={} year={}",
        result.size(), areaFile, year);
    return result.toString();
  }

  /** Rows of one state workbook. */
  ArrayNode stateRows(Workbook workbook, String stateAbbr, int year) {
    Map<Long, CountyRow> counts = parseSheet(workbook.getSheetAt(3));   // Table 4
    Map<Long, CountyRow> amounts = parseSheet(workbook.getSheetAt(4));  // Table 5
    if (!counts.keySet().equals(amounts.keySet())) {
      throw new IllegalStateException("SSA OASDI county: Tables 4 and 5 list different areas for "
          + stateAbbr + " " + year);
    }
    ArrayNode result = MAPPER.createArrayNode();
    for (Map.Entry<Long, CountyRow> e : counts.entrySet()) {
      CountyRow c = e.getValue();
      addRow(result, stateAbbr, c.countyFips, c.name, year, c, amounts.get(e.getKey()));
    }
    return result;
  }

  /** DC's state-total and county rows, from the Table 2 and Table 3 workbooks. */
  ArrayNode dcRows(Workbook countsWorkbook, Workbook amountsWorkbook, int year) {
    CountyRow counts = findDc(countsWorkbook.getSheetAt(0));
    CountyRow amounts = findDc(amountsWorkbook.getSheetAt(0));
    ArrayNode result = MAPPER.createArrayNode();
    addRow(result, "DC", null, "Total, " + DC_NAME, year, counts, amounts);
    addRow(result, "DC", DC_COUNTY_FIPS, DC_NAME, year, counts, amounts);
    return result;
  }

  private CountyRow findDc(Sheet sheet) {
    for (Row row : sheet) {
      if (DC_NAME.equals(label(row.getCell(0)))) {
        return readMeasures(row);
      }
    }
    throw new IllegalStateException("SSA OASDI county: no " + DC_NAME + " row in sheet "
        + sheet.getSheetName());
  }

  private void addRow(ArrayNode result, String stateAbbr, String countyFips, String name,
      int year, CountyRow counts, CountyRow amounts) {
    ObjectNode out = result.addObject();
    out.put("state_abbr", stateAbbr);
    if (countyFips != null) {
      out.put("county_fips", countyFips);
    } else {
      out.putNull("county_fips");
    }
    out.put("county_name", name);
    out.put("data_year", year);
    putMeasures(out, "count", counts);
    putMeasures(out, "amount", amounts);
  }

  private void putMeasures(ObjectNode out, String suffix, CountyRow row) {
    putNum(out, "total_" + suffix, row.total);
    putNum(out, "retired_workers_" + suffix, row.retiredWorkers);
    putNum(out, "spouses_retirement_" + suffix, row.spousesRetirement);
    putNum(out, "children_retirement_" + suffix, row.childrenRetirement);
    putNum(out, "widowers_parents_" + suffix, row.widowersParents);
    putNum(out, "children_survivors_" + suffix, row.childrenSurvivors);
    putNum(out, "disabled_workers_" + suffix, row.disabledWorkers);
    putNum(out, "spouses_disability_" + suffix, row.spousesDisability);
    putNum(out, "children_disability_" + suffix, row.childrenDisability);
  }

  private void putNum(ObjectNode out, String field, Double v) {
    if (v != null) {
      out.put(field, v);
    } else {
      out.putNull(field);
    }
  }

  /**
   * Parses one sheet (Table 4 or Table 5) into a map keyed by the ANSI code in column C. A row
   * without a numeric code is a sub-header, spacer or footnote and carries no figures; the sheet
   * must yield exactly one state-total row and at least one county row, all in that state.
   */
  private Map<Long, CountyRow> parseSheet(Sheet sheet) {
    Map<Long, CountyRow> out = new LinkedHashMap<Long, CountyRow>();
    Long stateCode = null;
    for (int r = DATA_START_ROW; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      Double ansi = cellDouble(row.getCell(2));
      String nameA = label(row.getCell(0));
      if (ansi == null) {
        if (nameA != null && cellDouble(row.getCell(3)) != null) {
          throw new IllegalStateException("SSA OASDI county: area '" + nameA
              + "' has figures but no ANSI code, sheet " + sheet.getSheetName() + " row " + (r + 1));
        }
        continue;
      }
      String nameB = label(row.getCell(1));
      boolean isTotal = nameA == null && nameB != null;
      if (!isTotal && nameA == null) {
        throw new IllegalStateException("SSA OASDI county: unnamed area, sheet "
            + sheet.getSheetName() + " row " + (r + 1));
      }
      CountyRow cr = readMeasures(row);
      // A listed area with no figure in any column (a county equivalent dissolved before the
      // snapshot, e.g. Bedford city) is not a data row.
      if (!isTotal && cr.hasNoFigures()) {
        continue;
      }
      long code = ansi.longValue();
      if (isTotal) {
        if (stateCode != null) {
          throw new IllegalStateException("SSA OASDI county: second state-total row, sheet "
              + sheet.getSheetName() + " row " + (r + 1));
        }
        stateCode = code >= 1000 ? code / 1000 : code;
        cr.name = nameB;
      } else {
        cr.name = nameA;
        cr.countyFips = leftPad(String.valueOf(code), 5, '0');
      }
      if (out.put(code, cr) != null) {
        throw new IllegalStateException("SSA OASDI county: duplicate ANSI code " + code
            + ", sheet " + sheet.getSheetName());
      }
    }
    if (stateCode == null || out.size() < 2) {
      throw new IllegalStateException("SSA OASDI county: sheet " + sheet.getSheetName()
          + " lacks a state-total row or county rows");
    }
    for (CountyRow cr : out.values()) {
      if (cr.countyFips != null && Long.parseLong(cr.countyFips) / 1000 != stateCode) {
        throw new IllegalStateException("SSA OASDI county: code " + cr.countyFips + " (" + cr.name
            + ") is outside state " + stateCode + ", sheet " + sheet.getSheetName());
      }
    }
    return out;
  }

  /** The 8 beneficiary-type figures and their total, columns D to L in every source table. */
  private CountyRow readMeasures(Row row) {
    CountyRow cr = new CountyRow();
    cr.total = cellDouble(row.getCell(3));
    cr.retiredWorkers = cellDouble(row.getCell(4));
    cr.spousesRetirement = cellDouble(row.getCell(5));
    cr.childrenRetirement = cellDouble(row.getCell(6));
    cr.widowersParents = cellDouble(row.getCell(7));
    cr.childrenSurvivors = cellDouble(row.getCell(8));
    cr.disabledWorkers = cellDouble(row.getCell(9));
    cr.spousesDisability = cellDouble(row.getCell(10));
    cr.childrenDisability = cellDouble(row.getCell(11));
    return cr;
  }

  /**
   * A label cell on one line, without layout artifacts: wrapped lines are rejoined, non-breaking
   * and repeated spaces become one space, and a trailing footnote letter ("Total, Maryland b") is
   * dropped. Null for an empty cell.
   */
  private String label(Cell cell) {
    String s = cellString(cell);
    if (s == null) {
      return null;
    }
    s = s.replace('\u00a0', ' ').replaceAll("-\\s*\\n\\s*", "-").replaceAll("\\s+", " ").trim()
        .replaceFirst(" [a-z]$", "");
    return s.isEmpty() ? null : s;
  }

  private static String leftPad(String s, int len, char pad) {
    StringBuilder sb = new StringBuilder();
    for (int i = s.length(); i < len; i++) {
      sb.append(pad);
    }
    sb.append(s);
    return sb.toString();
  }

  private static final class CountyRow {
    String name;
    String countyFips;
    Double total;
    Double retiredWorkers;
    Double spousesRetirement;
    Double childrenRetirement;
    Double widowersParents;
    Double childrenSurvivors;
    Double disabledWorkers;
    Double spousesDisability;
    Double childrenDisability;

    boolean hasNoFigures() {
      return total == null && retiredWorkers == null && spousesRetirement == null
          && childrenRetirement == null && widowersParents == null && childrenSurvivors == null
          && disabledWorkers == null && spousesDisability == null && childrenDisability == null;
    }
  }
}
