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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.energy.EiaBulkXlsxTransformer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Transforms one SSA "OASDI Beneficiaries by State and County" per-state XLSX (Table 4 = counts,
 * Table 5 = dollar amounts) into {@code health.ssa_oasdi_county} rows — one row per county (plus
 * one state-total row) per state per year, wide across the 8 beneficiary-type breakdowns both
 * tables publish.
 *
 * <p>Both tables share an identical row layout per state file (confirmed live across AL and CA):
 * column A = county name (blank on the state-total row), column B = "Total, &lt;State&gt;" label
 * (populated only on the state-total row), column C = SSA's own numeric ANSI/FIPS-style code (the
 * state's own 1-2 digit FIPS number on the total row, e.g. 6 for California; a 4-5 digit
 * state+county FIPS-without-leading-zero on every real county row, e.g. 6001), then the 8
 * beneficiary-type count/amount columns. Rows are joined between the two sheets by that ANSI code
 * rather than assumed row-position alignment.
 *
 * <p>Deliberately excludes the "Aged 65 or older, by sex" (Men/Women) columns the same tables
 * publish — a different demographic cross-tab of the same totals, not part of the
 * total/beneficiary-type breakdown this table carries.
 */
public class SsaOasdiCountyTransformer extends EiaBulkXlsxTransformer {

  private static final int DATA_START_ROW = 4; // 0-indexed; row 5 in the spreadsheet

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
    String state = context.getDimensionValues().get("state");
    String year = context.getDimensionValues().get("effective_year");
    String stateAbbr = state == null ? null : state.toUpperCase(java.util.Locale.ROOT);

    Sheet countsSheet = workbook.getSheetAt(3);   // Table 4
    Sheet amountsSheet = workbook.getSheetAt(4);  // Table 5
    Map<Long, CountyRow> counts = parseSheet(countsSheet);
    Map<Long, CountyRow> amounts = parseSheet(amountsSheet);

    ArrayNode result = MAPPER.createArrayNode();
    for (Map.Entry<Long, CountyRow> e : counts.entrySet()) {
      CountyRow c = e.getValue();
      CountyRow a = amounts.get(e.getKey());
      ObjectNode out = MAPPER.createObjectNode();
      out.put("state_abbr", stateAbbr);
      if (c.countyFips != null) {
        out.put("county_fips", c.countyFips);
      } else {
        out.putNull("county_fips");
      }
      out.put("county_name", c.name);
      out.put("data_year", year == null ? null : Integer.parseInt(year.trim()));
      putMeasures(out, "count", c);
      putMeasures(out, "amount", a);
      result.add(out);
    }
    LOGGER.debug("SSA OASDI county: emitted {} rows for state={} year={}", result.size(), state, year);
    return result.toString();
  }

  private void putMeasures(ObjectNode out, String suffix, CountyRow row) {
    putNum(out, "total_" + suffix, row == null ? null : row.total);
    putNum(out, "retired_workers_" + suffix, row == null ? null : row.retiredWorkers);
    putNum(out, "spouses_retirement_" + suffix, row == null ? null : row.spousesRetirement);
    putNum(out, "children_retirement_" + suffix, row == null ? null : row.childrenRetirement);
    putNum(out, "widowers_parents_" + suffix, row == null ? null : row.widowersParents);
    putNum(out, "children_survivors_" + suffix, row == null ? null : row.childrenSurvivors);
    putNum(out, "disabled_workers_" + suffix, row == null ? null : row.disabledWorkers);
    putNum(out, "spouses_disability_" + suffix, row == null ? null : row.spousesDisability);
    putNum(out, "children_disability_" + suffix, row == null ? null : row.childrenDisability);
  }

  private void putNum(ObjectNode out, String field, Double v) {
    if (v != null) {
      out.put(field, v);
    } else {
      out.putNull(field);
    }
  }

  /**
   * Parses one sheet (Table 4 or Table 5) into a map keyed by SSA's own ANSI/FIPS-style numeric
   * code — the state's own FIPS number for the state-total row, a state+county FIPS-without-
   * leading-zero value for every real county row. Stops at the first row with no numeric code in
   * column C (the blank separator rows immediately before the footnote block).
   */
  private Map<Long, CountyRow> parseSheet(Sheet sheet) {
    Map<Long, CountyRow> out = new HashMap<Long, CountyRow>();
    for (int r = DATA_START_ROW; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        break;
      }
      Double ansi = cellDouble(row.getCell(2));
      if (ansi == null) {
        break;
      }
      String nameA = cellString(row.getCell(0));
      String nameB = cellString(row.getCell(1));
      boolean isTotal = nameA == null && nameB != null;
      CountyRow cr = new CountyRow();
      cr.name = isTotal ? nameB : nameA;
      cr.countyFips = isTotal ? null : leftPad(String.valueOf(ansi.longValue()), 5, '0');
      cr.total = cellDouble(row.getCell(3));
      cr.retiredWorkers = cellDouble(row.getCell(4));
      cr.spousesRetirement = cellDouble(row.getCell(5));
      cr.childrenRetirement = cellDouble(row.getCell(6));
      cr.widowersParents = cellDouble(row.getCell(7));
      cr.childrenSurvivors = cellDouble(row.getCell(8));
      cr.disabledWorkers = cellDouble(row.getCell(9));
      cr.spousesDisability = cellDouble(row.getCell(10));
      cr.childrenDisability = cellDouble(row.getCell(11));
      out.put(ansi.longValue(), cr);
    }
    return out;
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
  }
}
