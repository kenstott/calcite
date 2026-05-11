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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

public class Eia814CrudeImportsTransformer extends EiaBulkXlsxTransformer {

  // EIA-814 switched from .xls to .xlsx in 2017.
  private static final int XLSX_START_YEAR = 2017;
  private static final String URL_PATTERN =
      "https://www.eia.gov/petroleum/imports/companylevel/archive/%d/%d_%s/data/import.%s";

  @Override
  public String transform(String response, RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String yearStr = dims != null ? dims.get("year") : null;
    if (yearStr == null || yearStr.isEmpty()) {
      LOGGER.warn("EIA-814: no year dimension; cannot build monthly URLs");
      return "[]";
    }

    int year;
    try {
      year = Integer.parseInt(yearStr);
    } catch (NumberFormatException e) {
      LOGGER.warn("EIA-814: invalid year dimension value: {}", yearStr);
      return "[]";
    }

    boolean isXlsx = year >= XLSX_START_YEAR;
    String ext = isXlsx ? "xlsx" : "xls";
    ArrayNode result = MAPPER.createArrayNode();

    for (int month = 1; month <= 12; month++) {
      String monthStr = String.format("%02d", month);
      String url = String.format(URL_PATTERN, year, year, monthStr, ext);
      try {
        byte[] bytes = downloadBytes(url);
        Workbook wb = isXlsx
            ? new XSSFWorkbook(new ByteArrayInputStream(bytes))
            : new HSSFWorkbook(new ByteArrayInputStream(bytes));
        try {
          parseMonthlySheet(wb, year, month, result);
        } finally {
          wb.close();
        }
      } catch (Exception e) {
        LOGGER.warn("EIA-814: failed to download/parse month {} for year {}: {}",
            month, year, e.getMessage());
      }
    }

    LOGGER.debug("EIA-814: parsed {} crude import records for year {}", result.size(), year);
    return result.toString();
  }

  private void parseMonthlySheet(Workbook wb, int year, int month, ArrayNode result) {
    Sheet sheet = wb.getSheet("Sheet1");
    if (sheet == null) {
      sheet = wb.getSheetAt(0);
    }
    if (sheet == null) {
      return;
    }

    Row headerRow = findHeaderRow(sheet);
    if (headerRow == null) {
      LOGGER.warn("EIA-814: no header row found for year={} month={}", year, month);
      return;
    }

    Map<String, Integer> colIndex = buildColumnIndex(headerRow);
    int startRow = headerRow.getRowNum() + 1;

    for (int r = startRow; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }

      // Filter: only crude oil (PROD_CODE = '025')
      String prodCode = colIndex.containsKey("PROD_CODE")
          ? cellString(row.getCell(colIndex.get("PROD_CODE"))) : null;
      if (!"025".equals(prodCode != null ? prodCode.trim() : null)) {
        continue;
      }

      ObjectNode out = MAPPER.createObjectNode();
      out.put("import_year", year);
      out.put("import_month", month);

      putStringField(out, "rpt_period", row, colIndex, "RPT_PERIOD");
      putStringField(out, "importer_name", row, colIndex, "R_S_NAME");
      putStringField(out, "origin_country", row, colIndex, "CNTRY_NAME");
      putIntField(out, "origin_country_code", row, colIndex, "GCTRY_CODE");
      putStringField(out, "entry_port_code", row, colIndex, "PORT_CODE");
      putStringField(out, "entry_port_city", row, colIndex, "PORT_CITY");
      putIntField(out, "entry_padd", row, colIndex, "PORT_PADD");
      putIntField(out, "dest_padd", row, colIndex, "PCOMP_PADD");
      putStringField(out, "receiving_company", row, colIndex, "PCOMP_RNAM");
      putIntField(out, "refinery_site_id", row, colIndex, "PCOMP_SITEID");
      putStringField(out, "refinery_site_name", row, colIndex, "PCOMP_SNAM");
      putStringField(out, "refinery_state_abbr", row, colIndex, "PCOMP_STAT");
      putDoubleField(out, "api_gravity", row, colIndex, "APIGRAVITY");
      putDoubleField(out, "sulfur_content_pct", row, colIndex, "SULFUR");
      putIntField(out, "volume_kbbl", row, colIndex, "QUANTITY");

      result.add(out);
    }
  }

  private Row findHeaderRow(Sheet sheet) {
    for (int r = 0; r <= Math.min(sheet.getLastRowNum(), 10); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      for (int c = 0; c <= row.getLastCellNum(); c++) {
        String val = cellString(row.getCell(c));
        if ("RPT_PERIOD".equals(val) || "CNTRY_NAME".equals(val)) {
          return row;
        }
      }
    }
    return null;
  }

  private Map<String, Integer> buildColumnIndex(Row headerRow) {
    Map<String, Integer> index = new HashMap<>();
    for (int c = 0; c <= headerRow.getLastCellNum(); c++) {
      String hdr = cellString(headerRow.getCell(c));
      if (hdr != null && !hdr.trim().isEmpty()) {
        index.put(hdr.trim(), c);
      }
    }
    return index;
  }

  private void putStringField(ObjectNode out, String targetKey, Row row,
      Map<String, Integer> colIndex, String colName) {
    if (!colIndex.containsKey(colName)) {
      out.putNull(targetKey);
      return;
    }
    String val = cellString(row.getCell(colIndex.get(colName)));
    if (val != null && !val.isEmpty()) {
      out.put(targetKey, val);
    } else {
      out.putNull(targetKey);
    }
  }

  private void putDoubleField(ObjectNode out, String targetKey, Row row,
      Map<String, Integer> colIndex, String colName) {
    if (!colIndex.containsKey(colName)) {
      out.putNull(targetKey);
      return;
    }
    Double val = cellDouble(row.getCell(colIndex.get(colName)));
    if (val != null) {
      out.put(targetKey, val);
    } else {
      out.putNull(targetKey);
    }
  }

  private void putIntField(ObjectNode out, String targetKey, Row row,
      Map<String, Integer> colIndex, String colName) {
    if (!colIndex.containsKey(colName)) {
      out.putNull(targetKey);
      return;
    }
    Integer val = cellInt(row.getCell(colIndex.get(colName)));
    if (val != null) {
      out.put(targetKey, val);
    } else {
      out.putNull(targetKey);
    }
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception { // NOSONAR - required by base class
    // Not used — transform() is overridden to handle multi-month downloads
    return "[]";
  }
}
