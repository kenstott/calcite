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
package org.apache.calcite.adapter.govdata.research;

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

/**
 * Transforms the NSF NCSES Survey of Federal Funds for R&amp;D "Federal obligations for
 * research, by detailed field of R&amp;D" XLSX (wide format: one row per field, one
 * column per fiscal year) into tall JSON rows.
 *
 * <p>The source table has a single header row (field name in column 0, fiscal years across
 * the remaining columns) and encodes the field hierarchy via each field-name cell's own
 * indentation level (0 = grand total "All fields", 1 = broad field, 2 = detailed sub-field
 * nested under the preceding level-1 row) rather than any explicit parent-id column. Level-1
 * rows sum to the level-0 total; level-2 rows sum to their enclosing level-1 row. Both levels
 * are emitted — callers pick one level to aggregate on, the same rollup-vs-leaf discipline
 * already applied to energy.eia_electricity_generation's sector/fuel rollup flags.
 */
public class NsfRdByFieldTransformer extends EiaBulkXlsxTransformer {

  private static final int HEADER_ROW = 3;
  private static final int DATA_START_ROW = 4;

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    XSSFWorkbook workbook = null;
    try {
      byte[] bytes = downloadBytes(url);
      // NCSES data-table XLSX are tiny but highly compressed, tripping POI's zip-bomb
      // guard (min inflate ratio). The source is a trusted federal publication, so relax it.
      ZipSecureFile.setMinInflateRatio(0.0);
      workbook = new XSSFWorkbook(new ByteArrayInputStream(bytes));
      return parseFieldTable(workbook);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse NSF R&D-by-field XLSX from " + url, e);
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

  private String parseFieldTable(XSSFWorkbook workbook) {
    Sheet sheet = workbook.getSheetAt(0);
    if (sheet == null) {
      LOGGER.error("NSF R&D by field: workbook has no sheets");
      return "[]";
    }

    Row headerRow = sheet.getRow(HEADER_ROW);
    if (headerRow == null) {
      LOGGER.error("NSF R&D by field: header row {} missing", HEADER_ROW);
      return "[]";
    }

    ArrayNode result = MAPPER.createArrayNode();
    int lastCol = headerRow.getLastCellNum();

    for (int r = DATA_START_ROW; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      Cell fieldCell = row.getCell(0);
      String field = cellString(fieldCell);
      if (field == null || field.trim().isEmpty()) {
        continue;
      }
      field = field.trim();
      int fieldLevel = fieldCell.getCellStyle().getIndention();

      for (int c = 1; c < lastCol; c++) {
        Integer year = parseYearHeader(cellString(headerRow.getCell(c)));
        if (year == null) {
          continue;
        }
        Double value = readValue(row, c);
        if (value == null) {
          continue;
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("year", year.intValue());
        out.put("rd_field", field);
        out.put("field_level", fieldLevel);
        out.put("obligations_usd_million", value);
        result.add(out);
      }
    }

    LOGGER.debug("NSF R&D by field: parsed {} rows", result.size());
    return result.toString();
  }

  /**
   * Reads an obligations cell, handling the survey's special markers: "*" means a
   * nonzero value that rounds to 0.0; "NA" or blank/null means the field did not exist
   * under that year's taxonomy (pre/post the 2021 field-of-R&D revision).
   */
  private Double readValue(Row row, int col) {
    String s = cellString(row.getCell(col));
    if (s == null) {
      return null;
    }
    String trimmed = s.trim();
    if (trimmed.isEmpty() || "NA".equalsIgnoreCase(trimmed)) {
      return null;
    }
    if ("*".equals(trimmed)) {
      return 0.0;
    }
    return cellDouble(row.getCell(col));
  }

  /**
   * Parses a header cell into a fiscal year, tolerating the trailing "(preliminary)"
   * suffix NCSES appends to the most recent column (e.g. "2025 (preliminary)").
   */
  private Integer parseYearHeader(String header) {
    if (header == null) {
      return null;
    }
    String trimmed = header.trim();
    int spaceIdx = trimmed.indexOf(' ');
    String yearPart = spaceIdx > 0 ? trimmed.substring(0, spaceIdx) : trimmed;
    try {
      return Integer.parseInt(yearPart);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception { // NOSONAR - required by base class
    // Not used — transform() is overridden to handle this table's wide fiscal-year layout.
    return "[]";
  }
}
