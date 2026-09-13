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
package org.apache.calcite.adapter.govdata.lands;

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

/**
 * Transforms BLM's combined "All Federal Oil & Gas Statistics" XLSX into
 * {@code lands.blm_oil_gas_acreage} rows — one row per (state, fiscal_year) with both
 * leased_acres (Table 2) and producing_acres (Table 6) joined side-by-side.
 *
 * <p>Both source sheets are wide (one row per state, one column per fiscal year, header
 * strings like {@code "FY 2001"}). The state list differs slightly across the two sheets
 * — this transformer unpivots each and full-outer-joins on (state, fiscal_year), so a
 * state present in only one sheet still gets a row with the other measure NULL. Trailing
 * whitespace and Note/TOTAL summary rows are stripped verbatim from the source.
 */
public class BlmOilGasAcreageTransformer extends EiaBulkXlsxTransformer {

  private static final String LEASED_SHEET = "Table 2 Acreage in Effect";
  private static final String PRODUCING_SHEET = "Table 6 Producing Acres";
  private static final int HEADER_ROW = 2;   // 0-indexed: title row 0, blank row 1, header row 2
  private static final int DATA_START_ROW = 3;

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    XSSFWorkbook workbook = null;
    try {
      byte[] bytes = downloadBytes(url);
      // BLM data-table XLSX are highly compressed, tripping POI's zip-bomb guard —
      // same pattern as NsfRdByFieldTransformer for another trusted federal-publication XLSX.
      ZipSecureFile.setMinInflateRatio(0.0);
      workbook = new XSSFWorkbook(new ByteArrayInputStream(bytes));
      Map<StateYear, Double> leased = parseWideSheet(workbook, LEASED_SHEET);
      Map<StateYear, Double> producing = parseWideSheet(workbook, PRODUCING_SHEET);
      return joinAndEmit(leased, producing);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse BLM oil-gas acreage XLSX from " + url, e);
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

  private Map<StateYear, Double> parseWideSheet(XSSFWorkbook workbook, String sheetName) {
    Sheet sheet = workbook.getSheet(sheetName);
    if (sheet == null) {
      LOGGER.error("BLM oil-gas acreage: sheet '{}' missing", sheetName);
      return new LinkedHashMap<StateYear, Double>();
    }
    Row header = sheet.getRow(HEADER_ROW);
    if (header == null) {
      LOGGER.error("BLM oil-gas acreage: sheet '{}' has no header row at index {}",
          sheetName, HEADER_ROW);
      return new LinkedHashMap<StateYear, Double>();
    }
    int lastCol = header.getLastCellNum();
    Map<StateYear, Double> out = new LinkedHashMap<StateYear, Double>();
    for (int r = DATA_START_ROW; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String state = cellString(row.getCell(0));
      if (state == null) {
        continue;
      }
      state = state.trim();  // strip trailing space (e.g. 'Delaware ' verbatim from source)
      if (state.isEmpty() || isSummaryOrNote(state)) {
        continue;
      }
      for (int c = 1; c < lastCol; c++) {
        Integer year = parseFyHeader(cellString(header.getCell(c)));
        if (year == null) {
          continue;
        }
        Cell cell = row.getCell(c);
        if (cell == null) {
          continue;
        }
        Double value = cellDouble(cell);
        if (value == null) {
          continue;
        }
        out.put(new StateYear(state, year), value);
      }
    }
    return out;
  }

  private boolean isSummaryOrNote(String state) {
    String lower = state.toLowerCase();
    // BLM appends TOTAL and Note rows below the state list — filter them out (Table 2 has
    // 'Total', Table 6 'TOTAL'; both start their notes with 'Note').
    return "total".equals(lower) || lower.startsWith("note");
  }

  /**
   * Parses a BLM fiscal-year column header ("FY 2001" ... "FY 2024") into its integer.
   * Returns null for anything that doesn't match — the summary/total column headers.
   */
  private Integer parseFyHeader(String header) {
    if (header == null) {
      return null;
    }
    String trimmed = header.trim();
    if (!trimmed.startsWith("FY ")) {
      return null;
    }
    try {
      return Integer.parseInt(trimmed.substring(3).trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private String joinAndEmit(Map<StateYear, Double> leased,
      Map<StateYear, Double> producing) {
    ArrayNode result = MAPPER.createArrayNode();
    // Full outer join: iterate leased first (typically the superset), then any producing-only keys.
    java.util.Set<StateYear> keys = new java.util.LinkedHashSet<StateYear>();
    keys.addAll(leased.keySet());
    keys.addAll(producing.keySet());
    for (StateYear k : keys) {
      ObjectNode row = MAPPER.createObjectNode();
      row.put("state", k.state);
      row.put("fiscal_year", k.year);
      Double l = leased.get(k);
      if (l != null) {
        row.put("leased_acres", l);
      } else {
        row.putNull("leased_acres");
      }
      Double p = producing.get(k);
      if (p != null) {
        row.put("producing_acres", p);
      } else {
        row.putNull("producing_acres");
      }
      result.add(row);
    }
    LOGGER.debug("BLM oil-gas acreage: emitted {} (state, fiscal_year) rows", result.size());
    return result.toString();
  }

  private static final class StateYear {
    final String state;
    final int year;
    StateYear(String state, int year) {
      this.state = state;
      this.year = year;
    }
    @Override public boolean equals(Object o) {
      if (!(o instanceof StateYear)) return false;
      StateYear other = (StateYear) o;
      return year == other.year && state.equals(other.state);
    }
    @Override public int hashCode() {
      return state.hashCode() * 31 + year;
    }
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) {
    // Not used — transform() is overridden to handle this table's two-sheet wide layout.
    return "[]";
  }
}
