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

/**
 * Transforms the NSF NCSES National Patterns of R&amp;D Resources, Table 1 XLSX
 * (a single multi-year file: U.S. GDP, total R&amp;D expenditure, and R&amp;D as a
 * share of GDP by performing sector and funding source) into tall JSON rows.
 *
 * <p>Table 1 has merged, multi-row headers spanning rows 0-5, so this transformer
 * does not match on header text at all — it locates data rows purely by content
 * (column 0 parses as a plausible 4-digit calendar year) and reads fixed 0-based
 * column indices verified against the published file. Only the total row carries
 * dollar amounts (current + constant USD); the per-sector and per-source columns
 * are R&amp;D-as-%-of-GDP shares, emitted as {@code pct_of_gdp} (never as dollars).
 */
public class NsfNationalRdTransformer extends EiaBulkXlsxTransformer {

  private static final int MIN_YEAR = 1950;
  private static final int MAX_YEAR = 2100;

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
      return parseNationalTable(workbook);
    } catch (Exception e) {
      LOGGER.error("Failed to parse NSF National R&D XLSX from {}: {}", url, e.getMessage());
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

  private String parseNationalTable(XSSFWorkbook workbook) {
    Sheet sheet = workbook.getSheetAt(0);
    if (sheet == null) {
      LOGGER.error("NSF National R&D: workbook has no sheets");
      return "[]";
    }

    ArrayNode result = MAPPER.createArrayNode();

    for (int r = 0; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }

      Integer year = parseYear(cellString(row.getCell(0)));
      if (year == null) {
        continue;
      }

      Double totalCurrent = cellDouble(row.getCell(4));
      Double totalConstant = cellDouble(row.getCell(5));
      Double pctOfGdp = cellDouble(row.getCell(6));

      // 1. Grand total row.
      if (totalCurrent != null) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("year", year.intValue());
        out.put("performing_sector", "All performers");
        out.put("funding_source", "All sources");
        out.put("rd_type", "Total");
        out.put("rd_expenditure_usd_million", totalCurrent * 1000.0);
        if (totalConstant != null) {
          out.put("rd_expenditure_constant_usd_million", totalConstant * 1000.0);
        } else {
          out.putNull("rd_expenditure_constant_usd_million");
        }
        if (pctOfGdp != null) {
          out.put("pct_of_gdp", pctOfGdp);
        } else {
          out.putNull("pct_of_gdp");
        }
        result.add(out);
      }

      // 2. Performer breakdown rows. Table 1 columns 7-13 are R&D-as-%-of-GDP (the
      //    performer/source shares sum to the total R&D/GDP %), NOT dollar amounts — so
      //    these rows carry pct_of_gdp only; per-sector dollars are not published here.
      addPerformerRow(result, year, "Business", cellDouble(row.getCell(7)));
      addPerformerRow(result, year, "Federal government", cellDouble(row.getCell(8)));
      addPerformerRow(result, year, "Higher education", cellDouble(row.getCell(9)));
      addPerformerRow(result, year, "Other", cellDouble(row.getCell(10)));

      // 3. Source breakdown rows (also R&D-as-%-of-GDP).
      addSourceRow(result, year, "Business", cellDouble(row.getCell(11)));
      addSourceRow(result, year, "Federal", cellDouble(row.getCell(12)));
      addSourceRow(result, year, "Other", cellDouble(row.getCell(13)));
    }

    LOGGER.debug("NSF National R&D: parsed {} rows", result.size());
    return result.toString();
  }

  private void addPerformerRow(ArrayNode result, Integer year, String sector, Double pctOfGdp) {
    if (pctOfGdp == null) {
      return;
    }
    ObjectNode out = MAPPER.createObjectNode();
    out.put("year", year.intValue());
    out.put("performing_sector", sector);
    out.put("funding_source", "All sources");
    out.put("rd_type", "Total");
    out.putNull("rd_expenditure_usd_million");
    out.putNull("rd_expenditure_constant_usd_million");
    out.put("pct_of_gdp", pctOfGdp);
    result.add(out);
  }

  private void addSourceRow(ArrayNode result, Integer year, String source, Double pctOfGdp) {
    if (pctOfGdp == null) {
      return;
    }
    ObjectNode out = MAPPER.createObjectNode();
    out.put("year", year.intValue());
    out.put("performing_sector", "All performers");
    out.put("funding_source", source);
    out.put("rd_type", "Total");
    out.putNull("rd_expenditure_usd_million");
    out.putNull("rd_expenditure_constant_usd_million");
    out.put("pct_of_gdp", pctOfGdp);
    result.add(out);
  }

  /**
   * Parses a leading 4-digit integer year out of a cell string like "1953" or
   * "1953.0". Returns null if the cell is not a plausible calendar year.
   */
  private Integer parseYear(String s) {
    if (s == null) {
      return null;
    }
    String trimmed = s.trim();
    if (trimmed.length() < 4) {
      return null;
    }
    String prefix = trimmed.substring(0, 4);
    for (int i = 0; i < 4; i++) {
      if (!Character.isDigit(prefix.charAt(i))) {
        return null;
      }
    }
    int year;
    try {
      year = Integer.parseInt(prefix);
    } catch (NumberFormatException e) {
      return null;
    }
    if (year < MIN_YEAR || year > MAX_YEAR) {
      return null;
    }
    return year;
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception { // NOSONAR - required by base class
    // Not used — transform() is overridden to handle the single-sheet, multi-year layout.
    return "[]";
  }
}
