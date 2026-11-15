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
import org.apache.calcite.adapter.govdata.energy.EiaBulkXlsxTransformer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms OMB's budget-by-account supplemental workbooks ({@code budauth_fyNNNN.xlsx} and
 * {@code outlays_fyNNNN.xlsx}) into {@code fiscal.omb_budget_by_account} rows: one row per
 * account line per fiscal year, long format.
 *
 * <p>The request URL is OMB's supplemental-materials landing page. Its response (the HTML) is
 * searched for the workbook link whose file name matches the {@code measure} dimension, so a new
 * budget vintage (the file name carries the budget year, the upload path carries the publication
 * month) needs no URL edit. The workbooks have different leading columns (outlays carry a
 * grant/non-grant split) and different first years, so the header row is read by name: every
 * header that is a four-digit year or {@code TQ} is a value column, every other header is an
 * attribute column.
 */
public class OmbBudgetByAccountTransformer extends EiaBulkXlsxTransformer {

  private static final Pattern YEAR_HEADER = Pattern.compile("^\\d{4}$");

  private static final Map<String, String> ATTRIBUTE_FIELDS = buildAttributeFields();

  private static Map<String, String> buildAttributeFields() {
    Map<String, String> m = new LinkedHashMap<String, String>();
    m.put("Agency Code", "agency_code");
    m.put("Agency Name", "agency_name");
    m.put("Bureau Code", "bureau_code");
    m.put("Bureau Name", "bureau_name");
    m.put("Account Code", "account_code");
    m.put("Account Name", "account_name");
    m.put("Treasury Agency Code", "treasury_agency_code");
    m.put("CGAC Agency Code", "cgac_agency_code");
    m.put("Subfunction Code", "subfunction_code");
    m.put("Subfunction Title", "subfunction_title");
    m.put("BEA Category", "bea_category");
    m.put("Grant/non-grant split", "grant_split");
    m.put("On- or Off- Budget", "on_off_budget");
    return m;
  }

  @Override public String transform(String response, RequestContext context) {
    String measure = context.getDimensionValues().get("measure");
    String filePrefix;
    if ("budget_authority".equals(measure)) {
      filePrefix = "budauth";
    } else if ("outlays".equals(measure)) {
      filePrefix = "outlays";
    } else {
      throw new IllegalStateException("OMB budget by account: unknown measure '" + measure + "'");
    }
    Matcher m = Pattern.compile("href=\"([^\"]*/" + filePrefix + "_fy(\\d{4})\\.xlsx)\"").matcher(response);
    if (!m.find()) {
      throw new IllegalStateException("OMB budget by account: no " + filePrefix
          + "_fyNNNN.xlsx link on " + context.getUrl());
    }
    String workbookUrl = m.group(1);
    int vintage = Integer.parseInt(m.group(2));
    try {
      byte[] bytes = downloadBytes(workbookUrl);
      XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(bytes));
      try {
        return parse(workbook, measure, vintage);
      } finally {
        workbook.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("OMB budget by account: failed to parse " + workbookUrl, e);
    }
  }

  @Override protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) {
    throw new UnsupportedOperationException("transform() resolves the workbook itself");
  }

  private String parse(XSSFWorkbook workbook, String measure, int vintage) {
    Sheet sheet = workbook.getSheetAt(0);
    Row header = sheet.getRow(0);
    int lastCol = header.getLastCellNum();
    String[] attrField = new String[lastCol];
    int[] yearOf = new int[lastCol];
    boolean[] isTq = new boolean[lastCol];
    for (int c = 0; c < lastCol; c++) {
      String h = cellString(header.getCell(c));
      if (h == null) {
        continue;
      }
      h = h.trim();
      if (YEAR_HEADER.matcher(h).matches()) {
        yearOf[c] = Integer.parseInt(h);
      } else if ("TQ".equals(h)) {
        isTq[c] = true;
      } else {
        attrField[c] = ATTRIBUTE_FIELDS.get(h);
        if (attrField[c] == null) {
          throw new IllegalStateException("OMB budget by account: unrecognised header '" + h + "'");
        }
      }
    }
    ArrayNode result = MAPPER.createArrayNode();
    for (int r = 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      Map<String, String> attrs = new LinkedHashMap<String, String>();
      for (int c = 0; c < lastCol; c++) {
        if (attrField[c] != null) {
          String v = cellString(row.getCell(c));
          attrs.put(attrField[c], v == null || v.trim().isEmpty() ? null : v.trim());
        }
      }
      int lastYear = 0;
      for (int c = 0; c < lastCol; c++) {
        if (yearOf[c] != 0) {
          lastYear = yearOf[c];
          emit(result, attrs, measure, vintage, r + 1, yearOf[c], false, row.getCell(c));
        } else if (isTq[c]) {
          // the transition quarter (Jul-Sep 1976) is its own period, flagged rather than
          // folded into FY1976 or FY1977
          emit(result, attrs, measure, vintage, r + 1, lastYear, true, row.getCell(c));
        }
      }
    }
    LOGGER.debug("OMB budget by account: emitted {} {} rows from FY{} workbook", result.size(), measure, vintage);
    return result.toString();
  }

  private void emit(ArrayNode out, Map<String, String> attrs, String measure, int vintage,
      int sourceRow, int fiscalYear, boolean transitionQuarter, Cell cell) {
    Double v = cellDouble(cell);
    if (v == null) {
      return;
    }
    ObjectNode o = MAPPER.createObjectNode();
    for (Map.Entry<String, String> e : attrs.entrySet()) {
      if (e.getValue() == null) {
        o.putNull(e.getKey());
      } else {
        o.put(e.getKey(), e.getValue());
      }
    }
    if (!attrs.containsKey("grant_split")) {
      o.putNull("grant_split");
    }
    o.put("measure", measure);
    o.put("source_row", sourceRow);
    o.put("fiscal_year", fiscalYear);
    o.put("is_transition_quarter", transitionQuarter);
    o.put("is_estimate", fiscalYear > vintage - 2);
    o.put("budget_vintage", vintage);
    o.put("amount_thousands", v);
    out.add(o);
  }
}
