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
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms the NSF NCSES Federal Funds for R&amp;D Survey, Table 7 XLSX (agency &times;
 * performing-sector obligations for a single fiscal year) into tall JSON rows.
 *
 * <p>Table 7 has merged, multi-row headers spanning rows 0-5, so this transformer does
 * not match on header text — it reads fixed 0-based column indices verified against the
 * published file. The fiscal year covered by the file is not present in the data rows;
 * it is embedded in the title text (rows 0-2), so it is recovered by taking the maximum
 * plausible 4-digit year found across those rows' concatenated text.
 */
public class NsfFederalRdTransformer extends EiaBulkXlsxTransformer {

  private static final int MIN_YEAR = 1950;
  private static final int MAX_YEAR = 2100;
  private static final int TITLE_ROW_COUNT = 3;
  private static final int DATA_START_ROW = 6;

  private static final Pattern YEAR_PATTERN = Pattern.compile("(\\d{4})");

  private static final String[] SECTOR_LABELS = {
      "Intramural — Federal agencies",
      "Intramural — FFRDCs",
      "Businesses",
      "Higher education",
      "Nonprofit organizations",
      "State and local government",
      "Non-U.S. performers"
  };

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
      return parseFederalTable(workbook);
    } catch (Exception e) {
      LOGGER.error("Failed to parse NSF Federal R&D XLSX from {}: {}", url, e.getMessage());
      return "[]";
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

  private String parseFederalTable(XSSFWorkbook workbook) {
    Sheet sheet = workbook.getSheetAt(0);
    if (sheet == null) {
      LOGGER.error("NSF Federal R&D: workbook has no sheets");
      return "[]";
    }

    Integer year = findReferenceYear(sheet);
    if (year == null) {
      LOGGER.error("NSF Federal R&D: could not determine reference fiscal year from title rows");
      return "[]";
    }

    ArrayNode result = MAPPER.createArrayNode();

    for (int r = DATA_START_ROW; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }

      String agency = cellString(row.getCell(0));
      if (agency == null || agency.trim().isEmpty()) {
        continue;
      }
      agency = agency.trim();

      // Section-header rows (e.g. "Departments") have an agency label but no total.
      Double total = readValue(row, 1);
      boolean hasTotal = total != null || isAsterisk(row, 1);
      if (!hasTotal) {
        continue;
      }

      for (int i = 0; i < SECTOR_LABELS.length; i++) {
        int col = 2 + i;
        Double value = readValue(row, col);
        if (value == null) {
          continue;
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("year", year.intValue());
        out.put("funding_agency", agency);
        out.put("performing_sector", SECTOR_LABELS[i]);
        out.put("rd_field", "All fields");
        out.put("rd_type", "Total R&D");
        out.put("obligations_usd_million", value);
        result.add(out);
      }
    }

    LOGGER.debug("NSF Federal R&D: parsed {} rows for FY{}", result.size(), year);
    return result.toString();
  }

  /**
   * Reads a performer/total cell as a value, handling the survey's special markers:
   * "*" means a nonzero value that rounds to 0.0; "NA" or blank/null means no value.
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

  private boolean isAsterisk(Row row, int col) {
    String s = cellString(row.getCell(col));
    return s != null && "*".equals(s.trim());
  }

  /**
   * Determines the survey's reference fiscal year as the maximum plausible 4-digit
   * calendar year (1950-2100) found across the concatenated title text of rows 0-2.
   */
  private Integer findReferenceYear(Sheet sheet) {
    StringBuilder titleText = new StringBuilder();
    for (int r = 0; r < TITLE_ROW_COUNT; r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      for (int c = 0; c <= row.getLastCellNum(); c++) {
        String val = cellString(row.getCell(c));
        if (val != null) {
          titleText.append(val).append(' ');
        }
      }
    }

    Integer maxYear = null;
    Matcher matcher = YEAR_PATTERN.matcher(titleText.toString());
    while (matcher.find()) {
      int candidate;
      try {
        candidate = Integer.parseInt(matcher.group(1));
      } catch (NumberFormatException e) {
        continue;
      }
      if (candidate < MIN_YEAR || candidate > MAX_YEAR) {
        continue;
      }
      if (maxYear == null || candidate > maxYear) {
        maxYear = candidate;
      }
    }
    return maxYear;
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception { // NOSONAR - required by base class
    // Not used — transform() is overridden to handle the single-fiscal-year layout.
    return "[]";
  }
}
