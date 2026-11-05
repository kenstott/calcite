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

import org.apache.calcite.adapter.file.etl.ModelOperand;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses the USCIS quarterly Form I-765 (Application for Employment Authorization) workbooks
 * into one row per (fiscal_year, fiscal_quarter, EAD eligibility category, filing type).
 *
 * <p>USCIS publishes one workbook per fiscal quarter with an irregular file suffix
 * ({@code ...fy2025_q1.xlsx}, {@code ...fy2025_q4_v1.xlsx}), and only the current quarter is
 * linked from its index page, so each quarter's file is located by probing the known folders
 * ({@code data/}, {@code reports/}) and suffixes. A local directory ({@code census.uscisLocalDir})
 * is checked first; otherwise the file is fetched through a headed browser session, because
 * uscis.gov refuses every non-browser client.
 * A quarter for which no file exists is not yet published and contributes no rows; a fiscal year
 * with no published quarter at all yields an empty result. The layout (header row starting
 * "EAD Eligibility Category", four filing-type groups of Receipt/Approval/Denial/Pending, then a
 * TOTAL group) is verified against the header and a mismatch fails the run.
 */
public class UscisI765Transformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UscisI765Transformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String BASE_URL = "https://www.uscis.gov/sites/default/files/document/";
  private static final String[] FOLDERS = {"data", "reports"};
  private static final String[] SUFFIXES = {"_v1", "", "_v2", "_v3"};

  private static final String[] FILING_TYPES = {"Initial", "Renewal", "Replacement",
      "Not Requested"};
  private static final int FIRST_COUNT_COL = 2;
  private static final int COLS_PER_FILING_TYPE = 4;

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "uscis_i765 is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String yearStr = dims != null ? dims.get("year") : null;
    if (yearStr == null || yearStr.isEmpty()) {
      throw new IllegalStateException("uscis_i765: year dimension missing");
    }
    int fiscalYear = Integer.parseInt(yearStr);

    String localDir = ModelOperand.getString("census.uscisLocalDir");
    ArrayNode result = MAPPER.createArrayNode();
    for (int quarter = 1; quarter <= 4; quarter++) {
      InputStream xlsx = openLocal(localDir, fiscalYear, quarter);
      for (String folder : FOLDERS) {
        if (xlsx != null) {
          break;
        }
        for (String suffix : SUFFIXES) {
          String url = workbookUrl(folder, fiscalYear, quarter, suffix);
          xlsx = openBrowser(url);
          if (xlsx != null) {
            LOGGER.info("uscis_i765: FY{} Q{} workbook served from {}", fiscalYear, quarter, url);
            break;
          }
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
    List<Map<String, Object>> rows = new ArrayList<>(result.size());
    for (JsonNode node : result) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("fiscal_year", node.get("fiscal_year").intValue());
      row.put("fiscal_quarter", node.get("fiscal_quarter").intValue());
      row.put("ead_category", node.get("ead_category").textValue());
      row.put("ead_category_description", textOrNull(node.get("ead_category_description")));
      row.put("filing_type", node.get("filing_type").textValue());
      for (String count : new String[] {"receipts", "approvals", "denials", "pending"}) {
        JsonNode v = node.get(count);
        row.put(count, v == null || v.isNull() ? null : (Object) v.longValue());
      }
      rows.add(row);
    }
    return rows.iterator();
  }

  private static String textOrNull(JsonNode n) {
    return n == null || n.isNull() ? null : n.textValue();
  }

  static String workbookFileName(int fiscalYear, int quarter, String suffix) {
    return String.format("i765_application_for_employment_fy%d_q%d%s.xlsx", fiscalYear, quarter,
        suffix);
  }

  static String workbookUrl(String folder, int fiscalYear, int quarter, String suffix) {
    return BASE_URL + folder + "/" + workbookFileName(fiscalYear, quarter, suffix);
  }

  static void parseSheet(Sheet sheet, int fiscalYear, int quarter, ArrayNode out) {
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

  /**
   * The workbook for the quarter from the local directory, or null when none is configured or no
   * file there matches. The directory is the first source: a workbook placed there is used
   * without contacting uscis.gov.
   */
  private static InputStream openLocal(String dir, int fiscalYear, int quarter) {
    if (dir == null || dir.trim().isEmpty()) {
      return null;
    }
    for (String suffix : SUFFIXES) {
      File f = new File(dir.trim(), workbookFileName(fiscalYear, quarter, suffix));
      if (f.isFile()) {
        LOGGER.info("uscis_i765: FY{} Q{} workbook served from local file {}", fiscalYear,
            quarter, f);
        try {
          return new FileInputStream(f);
        } catch (IOException e) {
          throw new RuntimeException("uscis_i765: cannot read local workbook " + f, e);
        }
      }
    }
    return null;
  }

  /** Fetches the URL through the headed browser; null when uscis.gov answers 404. */
  private static InputStream openBrowser(String url) {
    UscisBrowserSession.Config config = new UscisBrowserSession.Config(
        ModelOperand.getString("census.uscisNodeCommand"),
        ModelOperand.getString("census.uscisBrowserScript"),
        ModelOperand.getString("census.uscisDataPageUrl"),
        ModelOperand.getLong("census.uscisRequestDelayMs", -1L),
        ModelOperand.getString("census.uscisBrowserCacheDir"),
        ModelOperand.getString("census.uscisChromeExecutable"));
    try {
      return UscisBrowserSession.get(config).fetch(url);
    } catch (IOException e) {
      throw new RuntimeException("uscis_i765: browser fetch failed for " + url, e);
    }
  }
}
