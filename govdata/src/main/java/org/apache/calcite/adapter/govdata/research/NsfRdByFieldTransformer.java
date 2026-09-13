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
 *
 * <p>NCSES's pre-2016 editions (e.g. the FY2004-14 and FY1993-2003 long-run historical
 * tables) encode this same hierarchy differently: instead of a real cell-style indent
 * (style {@code alignment/@indent}, read via {@link Cell#getCellStyle()}), the field-name
 * string itself carries literal leading non-breaking-space (U+00A0) characters — 4 per
 * level. Confirmed live 2026-09-13 against NCSES publication nsf14316's table 134 (FYs
 * 2004-14): every style's {@code indent} attribute is absent/zero, but "All fields" has
 * zero leading NBSPs, a level-1 field like "Computer sciences and mathematics" has
 * exactly 4, and its level-2 children ("Computer sciences", "Mathematics", ...) have
 * exactly 8 — otherwise byte-identical row/column layout (same header row, same blank
 * spacer rows between data rows, same "NA"/"*" markers) to the current-edition files.
 * {@link #fieldLevel} tries the style indent first and falls back to counting leading
 * NBSPs (divided by 4) only when the style indent is zero and the raw string actually
 * starts with one — current-edition files are unaffected since their indent always comes
 * through the style.
 */
public class NsfRdByFieldTransformer extends EiaBulkXlsxTransformer {

  /**
   * Literal header-row marker in column 0 ("Field", followed by fiscal-year column
   * headers). The header row's own position is NOT fixed — confirmed live 2026-09-13:
   * the current-edition file and NCSES publication nsf14316's table 134 both have it at
   * row index 3 (title spans 3 rows), but that same publication's table 133 has it at
   * row index 2 (title spans only 2 rows) — a one-row difference within the SAME
   * publication. A hardcoded row index silently parsed zero records for table 133 (the
   * "header" row it read was actually the blank spacer row below the real header, whose
   * getLastCellNum() left the data-column loop with nothing to iterate). Locating the
   * header by content instead of position is immune to this.
   */
  private static final String HEADER_MARKER = "Field";

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

    int headerRowIdx = findHeaderRow(sheet);
    if (headerRowIdx < 0) {
      LOGGER.error("NSF R&D by field: no row with column 0 == \"{}\" found", HEADER_MARKER);
      return "[]";
    }
    Row headerRow = sheet.getRow(headerRowIdx);

    ArrayNode result = MAPPER.createArrayNode();
    int lastCol = headerRow.getLastCellNum();

    for (int r = headerRowIdx + 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      Cell fieldCell = row.getCell(0);
      String rawField = cellString(fieldCell);
      if (rawField == null || stripNbsp(rawField).trim().isEmpty()) {
        continue;
      }
      int fieldLevel = fieldLevel(fieldCell, rawField);
      // .trim() only strips <= U+0020 and leaves a pre-2016 file's leading NBSP
      // (U+00A0) indent markers in place — strip those explicitly first.
      String field = stripNbsp(rawField).trim();

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
   * Returns the field-hierarchy level (0/1/2) for a field-name cell, trying the
   * current-edition encoding (real cell-style indent) first and falling back to the
   * pre-2016 encoding (leading NBSP count / 4) when the style carries no indent but the
   * raw string starts with one. See the class Javadoc for how this was confirmed.
   */
  private int fieldLevel(Cell fieldCell, String rawField) {
    int styleIndent = fieldCell.getCellStyle().getIndention();
    if (styleIndent != 0) {
      return styleIndent;
    }
    int nbsp = 0;
    while (nbsp < rawField.length() && rawField.charAt(nbsp) == ' ') {
      nbsp++;
    }
    return nbsp / 4;
  }

  /** Strips only leading U+00A0 (non-breaking space) characters, not regular whitespace. */
  private String stripNbsp(String s) {
    int i = 0;
    while (i < s.length() && s.charAt(i) == '\u00A0') {
      i++;
    }
    return s.substring(i);
  }

  /**
   * Scans from the top of the sheet for the row whose column-0 cell is exactly
   * {@link #HEADER_MARKER} ("Field"), returning its 0-based row index or -1 if not found.
   * The title block above it spans a different number of rows across NCSES table
   * vintages (see the class Javadoc), so its position cannot be assumed.
   */
  private int findHeaderRow(Sheet sheet) {
    for (int r = 0; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      if (HEADER_MARKER.equals(cellString(row.getCell(0)))) {
        return r;
      }
    }
    return -1;
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
