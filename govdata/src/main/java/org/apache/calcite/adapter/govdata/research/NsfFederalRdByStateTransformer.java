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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms the NSF NCSES Federal Funds for R&amp;D Survey, Table 60 XLSX (state or
 * location &times; funding-agency obligations for a single fiscal year) into tall JSON
 * rows.
 *
 * <p>Table 60 has merged, multi-row headers spanning rows 0-3, so this transformer does
 * not match on header text — it reads fixed 0-based column indices verified against the
 * published file, same convention as the sibling {@link NsfFederalRdTransformer} (Table
 * 7). The fiscal year covered by the file is not present in the data rows; it is embedded
 * in the title text (rows 0-2), so it is recovered the same way — the maximum plausible
 * 4-digit year found across those rows' concatenated text. The per-state "Total" column
 * (col 1) is a rollup across the twelve agency columns and is not emitted as its own row,
 * matching how the sibling transformer omits Table 7's per-agency total column: summing
 * the twelve agency rows recovers it without risking a stored value drifting from its
 * own components.
 */
public class NsfFederalRdByStateTransformer extends EiaBulkXlsxTransformer {

  private static final int MIN_YEAR = 1950;
  private static final int MAX_YEAR = 2100;
  private static final int TITLE_ROW_COUNT = 3;
  private static final int DATA_START_ROW = 4;

  private static final Pattern YEAR_PATTERN = Pattern.compile("(\\d{4})");

  // Column order verified against the published file; the header row's own abbreviations
  // (DHS, DOC, DOD, ...) are mapped to the full department/agency names Table 7
  // (nsf_federal_rd_obligations.funding_agency) already uses, so the two tables' agency
  // names line up for a join or comparison.
  private static final String[] AGENCY_LABELS = {
      "Department of Homeland Security",
      "Department of Commerce",
      "Department of Defense",
      "Department of Energy",
      "Department of the Interior",
      "Department of Transportation",
      "Environmental Protection Agency",
      "Department of Health and Human Services",
      "National Aeronautics and Space Administration",
      "National Science Foundation",
      "Department of Agriculture",
      "Other agencies"
  };

  private static final Map<String, String> STATE_NAME_TO_FIPS = buildStateFipsMap();

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
      return parseStateTable(workbook);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse NSF Federal R&D by state XLSX from " + url, e);
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

  private String parseStateTable(XSSFWorkbook workbook) {
    Sheet sheet = workbook.getSheetAt(0);
    if (sheet == null) {
      LOGGER.error("NSF Federal R&D by state: workbook has no sheets");
      return "[]";
    }

    Integer year = findReferenceYear(sheet);
    if (year == null) {
      LOGGER.error("NSF Federal R&D by state: could not determine reference fiscal year from title rows");
      return "[]";
    }

    ArrayNode result = MAPPER.createArrayNode();

    for (int r = DATA_START_ROW; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }

      String stateLocation = cellString(row.getCell(0));
      if (stateLocation == null || stateLocation.trim().isEmpty()) {
        continue;
      }
      stateLocation = stateLocation.trim();
      String stateFips = STATE_NAME_TO_FIPS.get(stateLocation);

      for (int i = 0; i < AGENCY_LABELS.length; i++) {
        int col = 2 + i;
        Double value = readValue(row, col);
        if (value == null) {
          continue;
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("year", year.intValue());
        out.put("state_location", stateLocation);
        if (stateFips != null) {
          out.put("state_fips", stateFips);
        } else {
          out.putNull("state_fips");
        }
        out.put("funding_agency", AGENCY_LABELS[i]);
        out.put("obligations_usd_thousand", value);
        result.add(out);
      }
    }

    LOGGER.debug("NSF Federal R&D by state: parsed {} rows for FY{}", result.size(), year);
    return result.toString();
  }

  /**
   * Reads an agency-value cell, handling the survey's special markers: "*" means a
   * nonzero value that rounds to 0.0; "NA" or blank/null means no value.
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

  private static Map<String, String> buildStateFipsMap() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("Alabama", "01"); m.put("Alaska", "02"); m.put("Arizona", "04");
    m.put("Arkansas", "05"); m.put("California", "06"); m.put("Colorado", "08");
    m.put("Connecticut", "09"); m.put("Delaware", "10"); m.put("District of Columbia", "11");
    m.put("Florida", "12"); m.put("Georgia", "13"); m.put("Hawaii", "15");
    m.put("Idaho", "16"); m.put("Illinois", "17"); m.put("Indiana", "18");
    m.put("Iowa", "19"); m.put("Kansas", "20"); m.put("Kentucky", "21");
    m.put("Louisiana", "22"); m.put("Maine", "23"); m.put("Maryland", "24");
    m.put("Massachusetts", "25"); m.put("Michigan", "26"); m.put("Minnesota", "27");
    m.put("Mississippi", "28"); m.put("Missouri", "29"); m.put("Montana", "30");
    m.put("Nebraska", "31"); m.put("Nevada", "32"); m.put("New Hampshire", "33");
    m.put("New Jersey", "34"); m.put("New Mexico", "35"); m.put("New York", "36");
    m.put("North Carolina", "37"); m.put("North Dakota", "38"); m.put("Ohio", "39");
    m.put("Oklahoma", "40"); m.put("Oregon", "41"); m.put("Pennsylvania", "42");
    m.put("Rhode Island", "44"); m.put("South Carolina", "45"); m.put("South Dakota", "46");
    m.put("Tennessee", "47"); m.put("Texas", "48"); m.put("Utah", "49");
    m.put("Vermont", "50"); m.put("Virginia", "51"); m.put("Washington", "53");
    m.put("West Virginia", "54"); m.put("Wisconsin", "55"); m.put("Wyoming", "56");
    return m;
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception { // NOSONAR - required by base class
    // Not used — transform() is overridden to handle the single-fiscal-year layout.
    return "[]";
  }
}
