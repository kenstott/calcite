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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.energy.EiaBulkXlsxTransformer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms OMB's Historical Table 4.1 ("Outlays by Agency") XLSX into one JSON row per
 * (year, department or other unit).
 *
 * <p>The sheet is flat: one row per agency, one column per fiscal year (plus a bare "TQ"
 * column for the 1976 transition quarter, which is not a fiscal year and is skipped). Two
 * "(On-budget)" / "(Off-budget)" lines follow "Undistributed Offsetting Receipts" and are its
 * components; they are emitted as "Undistributed Offsetting Receipts (On-budget)" etc. with
 * {@code line_type = 'component'} so a sum over {@code line_type = 'agency'} reconciles to the
 * {@code 'total'} row.
 */
public class FederalOutlaysByAgencyTransformer extends EiaBulkXlsxTransformer {

  private static final String PARENT_OF_COMPONENTS = "Undistributed Offsetting Receipts";
  private static final Pattern YEAR_HEADER =
      Pattern.compile("^(\\d{4})(?:\\s+estimate)?$");

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    XSSFWorkbook workbook = null;
    try {
      byte[] bytes = downloadBytes(url);
      ZipSecureFile.setMinInflateRatio(0.0);
      workbook = new XSSFWorkbook(new ByteArrayInputStream(bytes));
      return parseTable(workbook);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse OMB Historical Table 4.1 XLSX from " + url, e);
    } finally {
      if (workbook != null) {
        try {
          workbook.close();
        } catch (Exception e) {
          LOGGER.warn("Failed to close workbook for {}: {}", url, e.getMessage());
        }
      }
    }
  }

  private String parseTable(XSSFWorkbook workbook) {
    Sheet sheet = workbook.getSheetAt(0);
    Row headerRow = findHeaderRow(sheet);
    if (headerRow == null) {
      throw new IllegalStateException(
          "OMB Table 4.1: header row 'Department or other unit' not found");
    }
    Map<Integer, Integer> yearByCol = new LinkedHashMap<Integer, Integer>();
    Map<Integer, Boolean> estimateByCol = new LinkedHashMap<Integer, Boolean>();
    for (int c = 1; c < headerRow.getLastCellNum(); c++) {
      String h = cellString(headerRow.getCell(c));
      if (h == null) {
        continue;
      }
      Matcher m = YEAR_HEADER.matcher(h.trim());
      if (m.matches()) {
        yearByCol.put(c, Integer.valueOf(m.group(1)));
        estimateByCol.put(c, Boolean.valueOf(h.contains("estimate")));
      }
    }
    if (yearByCol.isEmpty()) {
      throw new IllegalStateException("OMB Table 4.1: no fiscal-year columns found");
    }

    ArrayNode result = MAPPER.createArrayNode();
    for (int r = headerRow.getRowNum() + 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String label = cellString(row.getCell(0));
      if (label == null || label.trim().isEmpty()) {
        continue;
      }
      label = label.trim();
      String lineType = "agency";
      String name = label;
      if ("(On-budget)".equals(label) || "(Off-budget)".equals(label)) {
        lineType = "component";
        name = PARENT_OF_COMPONENTS + " " + label;
      } else if ("Total outlays".equals(label)) {
        lineType = "total";
      }
      boolean anyValue = false;
      for (Integer col : yearByCol.keySet()) {
        if (cellDouble(row.getCell(col)) != null) {
          anyValue = true;
          break;
        }
      }
      if (!anyValue) {
        continue; // footnotes and blank spacer rows carry no figures
      }
      for (Map.Entry<Integer, Integer> e : yearByCol.entrySet()) {
        Double value = cellDouble(row.getCell(e.getKey()));
        ObjectNode out = MAPPER.createObjectNode();
        out.put("year", e.getValue().intValue());
        out.put("is_estimate", estimateByCol.get(e.getKey()).booleanValue());
        out.put("agency", name);
        out.put("line_type", lineType);
        if (value == null) {
          out.putNull("outlays_millions");
        } else {
          out.put("outlays_millions", value.doubleValue());
        }
        result.add(out);
      }
    }
    return result.toString();
  }

  /** Header row is located by content, since its row index shifts across budget cycles. */
  private Row findHeaderRow(Sheet sheet) {
    for (int r = 0; r <= Math.min(10, sheet.getLastRowNum()); r++) {
      Row row = sheet.getRow(r);
      if (row != null && "Department or other unit".equals(cellString(row.getCell(0)))) {
        return row;
      }
    }
    return null;
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) { // NOSONAR - required by base class
    // Not used — transform() is overridden.
    return "[]";
  }
}
