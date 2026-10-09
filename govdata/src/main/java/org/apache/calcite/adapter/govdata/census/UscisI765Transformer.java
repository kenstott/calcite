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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;

/**
 * Parses the USCIS quarterly Form I-765 (Application for Employment Authorization) workbooks
 * into one row per (fiscal_year, fiscal_quarter, EAD eligibility category, filing type).
 *
 * <p>USCIS publishes one workbook per fiscal quarter with an irregular file suffix
 * ({@code ...fy2025_q1.xlsx}, {@code ...fy2025_q4_v1.xlsx}), and only the current quarter is
 * linked from its index page, so each quarter's file is located by probing the known suffixes.
 * A quarter for which no file exists is not yet published and contributes no rows; a fiscal year
 * with no published quarter at all yields an empty result. The layout (header row starting
 * "EAD Eligibility Category", four filing-type groups of Receipt/Approval/Denial/Pending, then a
 * TOTAL group) is verified against the header and a mismatch fails the run.
 */
public class UscisI765Transformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UscisI765Transformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String[] SUFFIXES = {"_v1", "", "_v2", "_v3"};
  private static final String USER_AGENT =
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
          + "Chrome/124.0 Safari/537.36";

  private static final String[] FILING_TYPES = {"Initial", "Renewal", "Replacement",
      "Not Requested"};
  private static final int FIRST_COUNT_COL = 2;
  private static final int COLS_PER_FILING_TYPE = 4;

  @Override public String transform(String response, RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String yearStr = dims != null ? dims.get("year") : null;
    if (yearStr == null || yearStr.isEmpty()) {
      throw new IllegalStateException("uscis_i765: year dimension missing");
    }
    int fiscalYear = Integer.parseInt(yearStr);

    ArrayNode result = MAPPER.createArrayNode();
    for (int quarter = 1; quarter <= 4; quarter++) {
      InputStream xlsx = null;
      for (String suffix : SUFFIXES) {
        xlsx = open(String.format(
            "https://www.uscis.gov/sites/default/files/document/data/"
                + "i765_application_for_employment_fy%d_q%d%s.xlsx",
            fiscalYear, quarter, suffix));
        if (xlsx != null) {
          break;
        }
      }
      if (xlsx == null) {
        LOGGER.info("uscis_i765: FY{} Q{} workbook not published", fiscalYear, quarter);
        continue;
      }
      try (InputStream in = xlsx; XSSFWorkbook wb = new XSSFWorkbook(in)) {
        parseSheet(wb.getSheetAt(0), fiscalYear, quarter, result);
      } catch (IOException e) {
        throw new RuntimeException("uscis_i765: cannot read FY" + fiscalYear + " Q" + quarter
            + " workbook", e);
      }
    }
    return result.toString();
  }

  private void parseSheet(Sheet sheet, int fiscalYear, int quarter, ArrayNode out) {
    DataFormatter fmt = new DataFormatter();
    String where = "FY" + fiscalYear + " Q" + quarter;
    int headerRow = -1;
    for (int r = 0; r <= Math.min(sheet.getLastRowNum(), 10); r++) {
      Row row = sheet.getRow(r);
      String first = row == null ? null : fmt.formatCellValue(row.getCell(0)).trim();
      if (first != null && first.toLowerCase().startsWith("ead eligibility category")) {
        headerRow = r;
        break;
      }
    }
    if (headerRow < 0) {
      throw new IllegalStateException("uscis_i765: header row not found for " + where);
    }
    Row groups = sheet.getRow(headerRow - 1);
    for (int i = 0; i < FILING_TYPES.length; i++) {
      int col = FIRST_COUNT_COL + i * COLS_PER_FILING_TYPE;
      String label = groups == null ? "" : fmt.formatCellValue(groups.getCell(col)).trim();
      if (!label.toLowerCase().endsWith(FILING_TYPES[i].toLowerCase())) {
        throw new IllegalStateException("uscis_i765: filing-type group " + i + " header is '"
            + label + "', expected '" + FILING_TYPES[i] + "' for " + where
            + " — workbook layout changed");
      }
    }
    String[] counts = {"receipt", "approval", "denial", "pending"};
    Row header = sheet.getRow(headerRow);
    for (int c = 0; c < FILING_TYPES.length * COLS_PER_FILING_TYPE; c++) {
      String h = fmt.formatCellValue(header.getCell(FIRST_COUNT_COL + c)).trim().toLowerCase();
      if (!h.startsWith(counts[c % COLS_PER_FILING_TYPE])) {
        throw new IllegalStateException("uscis_i765: column " + (FIRST_COUNT_COL + c)
            + " header is '" + h + "', expected '" + counts[c % COLS_PER_FILING_TYPE]
            + "...' for " + where + " — workbook layout changed");
      }
    }

    int rows = 0;
    for (int r = headerRow + 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String code = fmt.formatCellValue(row.getCell(0)).trim();
      if (code.isEmpty() || code.equalsIgnoreCase("Total")) {
        continue;
      }
      if (row.getCell(FIRST_COUNT_COL) == null
          || row.getCell(FIRST_COUNT_COL).getCellType() != CellType.NUMERIC) {
        // footnotes and source lines below the table carry no counts
        continue;
      }
      String description = fmt.formatCellValue(row.getCell(1)).trim();
      for (int i = 0; i < FILING_TYPES.length; i++) {
        int base = FIRST_COUNT_COL + i * COLS_PER_FILING_TYPE;
        ObjectNode node = MAPPER.createObjectNode();
        node.put("fiscal_year", fiscalYear);
        node.put("fiscal_quarter", quarter);
        node.put("ead_category", code);
        if (description.isEmpty()) {
          node.putNull("ead_category_description");
        } else {
          node.put("ead_category_description", description);
        }
        node.put("filing_type", FILING_TYPES[i]);
        putCount(node, "receipts", row.getCell(base));
        putCount(node, "approvals", row.getCell(base + 1));
        putCount(node, "denials", row.getCell(base + 2));
        putCount(node, "pending", row.getCell(base + 3));
        out.add(node);
      }
      rows++;
    }
    if (rows == 0) {
      throw new IllegalStateException("uscis_i765: no category rows parsed for " + where);
    }
  }

  private static void putCount(ObjectNode node, String name, Cell cell) {
    if (cell != null && cell.getCellType() == CellType.NUMERIC) {
      node.put(name, (long) cell.getNumericCellValue());
    } else {
      // USCIS suppresses small cells with a non-numeric marker
      node.putNull(name);
    }
  }

  /** Opens the file as a stream, or returns null when the URL does not exist (HTTP 404). */
  private static InputStream open(String url) {
    try {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(120000);
      conn.setRequestProperty("User-Agent", USER_AGENT);
      int status = conn.getResponseCode();
      if (status == 404) {
        return null;
      }
      if (status != 200) {
        throw new IOException("HTTP " + status + " from " + url);
      }
      return conn.getInputStream();
    } catch (IOException e) {
      throw new RuntimeException("uscis_i765: download failed for " + url, e);
    }
  }
}
