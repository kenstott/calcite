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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms OMB's Historical Table 3.2 ("Outlays by Function and Subfunction") XLSX into
 * one JSON row per (year, budget function).
 *
 * <p>The sheet is a hierarchical wide table: each budget function ("050 National Defense:")
 * introduces a block of subfunction rows, most blocks ending in a "Total, &lt;Function&gt;"
 * rollup row that carries the function-level figure this table wants. A minority of functions
 * (570 Medicare, 650 Social Security) have exactly one subfunction and OMB omits the redundant
 * Total line for those — the sole subfunction row IS the function total in that case, so the
 * parser falls back to the last data-bearing row seen in a function's block when no explicit
 * "Total, " row appeared.
 *
 * <p>Some function-level headers ("050 National Defense:") introduce further-nested
 * subfunction-level sub-headers that share the identical "NNN Name:" shape (e.g. "051
 * Department of Defense-Military:", which itself groups unlabeled line items like "Military
 * Personnel" before its own "051 Subtotal, ..." row). A fixed whitelist of OMB's 20 canonical
 * top-level function codes distinguishes a real function boundary from this nested case —
 * only a header whose code is in the whitelist resets which function subsequent rows belong
 * to; "051 Department of Defense-Military:" is not in the whitelist, so 050's block correctly
 * continues past it to "Total, National Defense" rather than misattributing that total to 051.
 */
public class FederalOutlaysByFunctionTransformer extends EiaBulkXlsxTransformer {

  private static final Set<String> TOP_LEVEL_FUNCTION_CODES = Set.of(
      "050", "150", "250", "270", "300", "350", "370", "400", "450", "500",
      "550", "570", "600", "650", "700", "750", "800", "900", "920", "950");

  private static final Pattern FUNCTION_HEADER =
      Pattern.compile("^(\\d{3})\\s+(.+):$");
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
      throw new RuntimeException("Failed to parse OMB Historical Table 3.2 XLSX from " + url, e);
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
    if (sheet == null) {
      LOGGER.error("Federal outlays by function: workbook has no sheets");
      return "[]";
    }

    // Header row: "Function and Subfunction" in column 0, then one column per fiscal year
    // (a bare "TQ" column for the 1976 transition quarter is skipped — not a standard FY).
    Row headerRow = findHeaderRow(sheet);
    if (headerRow == null) {
      LOGGER.error("Federal outlays by function: header row not found");
      return "[]";
    }
    Map<Integer, YearColumn> yearColumns = new LinkedHashMap<Integer, YearColumn>();
    int lastCol = headerRow.getLastCellNum();
    for (int c = 1; c < lastCol; c++) {
      String h = cellString(headerRow.getCell(c));
      if (h == null) {
        continue;
      }
      Matcher m = YEAR_HEADER.matcher(h.trim());
      if (m.matches()) {
        int year = Integer.parseInt(m.group(1));
        boolean isEstimate = h.contains("estimate");
        yearColumns.put(c, new YearColumn(year, isEstimate));
      }
    }

    ArrayNode result = MAPPER.createArrayNode();
    String currentCode = null;
    String currentName = null;
    LastRow lastRow = null;

    for (int r = headerRow.getRowNum() + 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String label = cellString(row.getCell(0));
      if (label == null) {
        continue;
      }
      label = label.trim();

      Matcher fm = FUNCTION_HEADER.matcher(label);
      if (fm.matches() && TOP_LEVEL_FUNCTION_CODES.contains(fm.group(1))) {
        emitFunctionTotal(result, currentCode, currentName, lastRow, yearColumns);
        currentCode = fm.group(1);
        currentName = fm.group(2);
        lastRow = null;
        continue;
      }
      if ("(On-budget)".equals(label) || "(Off-budget)".equals(label)
          || "Total outlays".equals(label) || label.startsWith("N/A")
          || label.startsWith("On-budget unless") || fm.matches()) {
        // fm.matches() here means a nested sub-header (e.g. "051 Department of
        // Defense-Military:") whose code isn't in the top-level whitelist — not a new
        // function boundary, and it carries no data of its own, so just skip it.
        continue;
      }
      if (!hasAnyValue(row, yearColumns)) {
        continue;
      }
      boolean isTotal = label.startsWith("Total, ");
      String rowName = isTotal ? label.substring("Total, ".length()) : label;
      if (isTotal) {
        lastRow = new LastRow(rowName, row, true);
      } else if (lastRow == null || !lastRow.isTotal) {
        lastRow = new LastRow(rowName, row, false);
      }
    }
    emitFunctionTotal(result, currentCode, currentName, lastRow, yearColumns);

    LOGGER.debug("Federal outlays by function: parsed {} rows", result.size());
    return result.toString();
  }

  private void emitFunctionTotal(ArrayNode result, String code, String name, LastRow lastRow,
      Map<Integer, YearColumn> yearColumns) {
    if (code == null || lastRow == null) {
      return;
    }
    for (Map.Entry<Integer, YearColumn> e : yearColumns.entrySet()) {
      YearColumn yc = e.getValue();
      Double value = cellDouble(lastRow.row.getCell(e.getKey()));
      ObjectNode out = MAPPER.createObjectNode();
      out.put("year", yc.year);
      out.put("is_estimate", yc.isEstimate);
      out.put("function_code", code);
      out.put("function_name", name);
      if (value == null) {
        out.putNull("outlays_millions");
      } else {
        out.put("outlays_millions", value.doubleValue());
      }
      result.add(out);
    }
  }

  /** Locates the row whose first cell is exactly "Function and Subfunction" — the true header
   * row position shifts slightly across budget cycles as OMB adds a new fiscal year's worth of
   * front-matter, so this is found by content rather than a hardcoded row index. */
  private Row findHeaderRow(Sheet sheet) {
    for (int r = 0; r <= Math.min(10, sheet.getLastRowNum()); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String label = cellString(row.getCell(0));
      if ("Function and Subfunction".equals(label)) {
        return row;
      }
    }
    return null;
  }

  private boolean hasAnyValue(Row row, Map<Integer, YearColumn> yearColumns) {
    for (Integer col : yearColumns.keySet()) {
      Cell cell = row.getCell(col);
      if (cell != null && cellDouble(cell) != null) {
        return true;
      }
    }
    return false;
  }

  private static final class YearColumn {
    final int year;
    final boolean isEstimate;

    YearColumn(int year, boolean isEstimate) {
      this.year = year;
      this.isEstimate = isEstimate;
    }
  }

  private static final class LastRow {
    final String name; // NOSONAR - kept for debugging/future use, not currently read
    final Row row;
    final boolean isTotal;

    LastRow(String name, Row row, boolean isTotal) {
      this.name = name;
      this.row = row;
      this.isTotal = isTotal;
    }
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) { // NOSONAR - required by base class
    // Not used — transform() is overridden for this table's hierarchical row-block layout.
    return "[]";
  }
}
