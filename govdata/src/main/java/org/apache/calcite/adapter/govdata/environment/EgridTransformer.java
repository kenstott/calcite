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
package org.apache.calcite.adapter.govdata.environment;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Discovers and parses EPA eGRID's "eGRID Subregion" (SRL) worksheet — regional emission
 * rates by fuel type (coal/oil/gas/fossil) for NOx, SO2, and CO2, in lb/MWh.
 *
 * <p>eGRID is published as a multi-tab XLSX workbook whose download URL is not predictable:
 * the file lives under an opaque {@code /system/files/documents/YYYY-MM/} (or
 * {@code /sites/default/files/YYYY-MM/}) path, and the filename itself varies year to year
 * ({@code egrid2018_data_v2.xlsx}, {@code eGRID2021_data.xlsx}, {@code egrid2023_data_rev2.xlsx},
 * ...). This transformer therefore uses the same two-step discovery pattern as
 * {@code PresidentialResultsTransformer} (officials schema): (1) the framework fetches a static
 * EPA listing page and hands its HTML to {@link #transform}; this method scans it for an
 * {@code href} whose lower-cased form contains {@code egrid<year>_data} and ends in
 * {@code .xlsx}, excluding the metric-units and summary-tables variants; (2) if found, downloads
 * and parses that XLSX directly via POI.
 *
 * <p>EPA splits the year listing across two pages: {@code historical-egrid-data} carries prior
 * vintages, while the most recently released vintage (not yet archived) appears only on
 * {@code download-data}. The primary source configured on this table is
 * {@code historical-egrid-data}; when a year is not found there this transformer makes one
 * additional GET to {@code download-data} as a fallback before giving up.
 *
 * <p>Verified live for 2018-2023 (2026-08-02): each workbook has a subregion-level worksheet
 * named {@code SRL<yy>} (e.g. {@code SRL23} for 2023) with two header rows — full descriptions,
 * then the mnemonic codes (e.g. {@code SUBRGN}, {@code SRCO2RTA}, {@code SRGCO2RT}) — followed by
 * one data row per eGRID subregion (~27 rows). Codes are located by name in the mnemonic header
 * row rather than by fixed column index, since eGRID has historically inserted new columns
 * between vintages. A year with no discoverable XLSX link on either listing page yields zero
 * rows for this table, not an error — logged at WARN with "no results link found".
 */
public class EgridTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(EgridTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String EPA_BASE = "https://www.epa.gov";
  private static final String FALLBACK_LISTING_URL = "https://www.epa.gov/egrid/download-data";

  /** Mnemonic eGRID subregion (SRL) column code -> output JSON field name. */
  private static final Map<String, String> FIELD_MAP = buildFieldMap();

  @Override public String transform(String response, RequestContext context) {
    String yearStr = context.getDimensionValues().get("effective_year");
    if (yearStr == null) {
      yearStr = context.getDimensionValues().get("year");
    }

    try {
      String xlsxUrl = findResultsUrl(response, yearStr);
      if (xlsxUrl == null) {
        String fallbackHtml = downloadString(FALLBACK_LISTING_URL);
        xlsxUrl = findResultsUrl(fallbackHtml, yearStr);
      }
      if (xlsxUrl == null) {
        LOGGER.warn("eGRID: no data workbook link found for year={} on {} or {}",
            yearStr, context.getUrl(), FALLBACK_LISTING_URL);
        return "[]";
      }

      byte[] xlsxBytes = downloadBytes(xlsxUrl);
      try (XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(xlsxBytes))) {
        return parseWorkbook(workbook, yearStr);
      }

    } catch (Exception e) {
      throw new RuntimeException("eGRID: Failed to parse response for "
          + context.getUrl() + " (year=" + yearStr + ")", e);
    }
  }

  private String findResultsUrl(String listingHtml, String yearStr) {
    if (listingHtml == null || listingHtml.isEmpty() || yearStr == null) {
      return null;
    }
    Document doc = Jsoup.parse(listingHtml);
    Elements links = doc.select("a[href]");
    String needle = "egrid" + yearStr + "_data";
    for (Element link : links) {
      String href = link.attr("href");
      String lower = href.toLowerCase(Locale.ROOT);
      if (lower.endsWith(".xlsx") && lower.contains(needle)
          && !lower.contains("metric") && !lower.contains("summary_table")) {
        return href.startsWith("http") ? href : EPA_BASE + href;
      }
    }
    return null;
  }

  private String parseWorkbook(XSSFWorkbook workbook, String yearStr) {
    int yearSuffix = Integer.parseInt(yearStr.trim()) % 100;
    String sheetName = String.format(Locale.ROOT, "SRL%02d", yearSuffix);
    Sheet sheet = workbook.getSheet(sheetName);
    if (sheet == null) {
      throw new IllegalStateException(
          "eGRID: expected worksheet '" + sheetName + "' not found in workbook for year="
              + yearStr + " (workbook sheets may have been renamed upstream)");
    }

    int codeRowIdx = findCodeRowIndex(sheet);
    if (codeRowIdx < 0) {
      throw new IllegalStateException(
          "eGRID: no mnemonic header row (SUBRGN) found in sheet " + sheetName
              + " for year=" + yearStr);
    }
    Row codeRow = sheet.getRow(codeRowIdx);
    Map<String, Integer> colIndex = new HashMap<>();
    for (int c = 0; c < codeRow.getLastCellNum(); c++) {
      String code = cellString(codeRow.getCell(c));
      if (code != null) {
        colIndex.put(code.trim().toUpperCase(Locale.ROOT), c);
      }
    }

    Integer year = Integer.parseInt(yearStr.trim());
    ArrayNode result = MAPPER.createArrayNode();

    for (int r = codeRowIdx + 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      Integer subrgnCol = colIndex.get("SUBRGN");
      String subregion = subrgnCol == null ? null : cellString(row.getCell(subrgnCol));
      if (subregion == null || subregion.trim().isEmpty()) {
        continue;
      }

      ObjectNode out = MAPPER.createObjectNode();
      out.put("year", year);
      for (Map.Entry<String, String> entry : FIELD_MAP.entrySet()) {
        String code = entry.getKey();
        String field = entry.getValue();
        Integer col = colIndex.get(code);
        Cell cell = col == null ? null : row.getCell(col);
        if ("SUBRGN".equals(code) || "SRNAME".equals(code)) {
          putStringOrNull(out, field, cellString(cell));
        } else {
          putDoubleOrNull(out, field, cellDouble(cell));
        }
      }
      result.add(out);
    }

    LOGGER.debug("eGRID: Parsed {} subregion rows for year={}", result.size(), yearStr);
    return result.toString();
  }

  private int findCodeRowIndex(Sheet sheet) {
    int maxScan = Math.min(sheet.getLastRowNum(), 5);
    for (int r = 0; r <= maxScan; r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      for (int c = 0; c < row.getLastCellNum(); c++) {
        String value = cellString(row.getCell(c));
        if ("SUBRGN".equalsIgnoreCase(value)) {
          return r;
        }
      }
    }
    return -1;
  }

  private void putStringOrNull(ObjectNode out, String field, String value) {
    if (value == null) {
      out.putNull(field);
    } else {
      out.put(field, value.trim());
    }
  }

  private void putDoubleOrNull(ObjectNode out, String field, Double value) {
    if (value == null) {
      out.putNull(field);
    } else {
      out.put(field, value);
    }
  }

  private String downloadString(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream();
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return baos.toString("UTF-8");
    }
  }

  private byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream();
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return baos.toByteArray();
    }
  }

  private String cellString(Cell cell) {
    if (cell == null) {
      return null;
    }
    if (cell.getCellType() == CellType.STRING) {
      String s = cell.getStringCellValue();
      return s == null || s.trim().isEmpty() ? null : s;
    }
    if (cell.getCellType() == CellType.NUMERIC) {
      return String.valueOf(cell.getNumericCellValue());
    }
    return null;
  }

  private Double cellDouble(Cell cell) {
    if (cell == null) {
      return null;
    }
    if (cell.getCellType() == CellType.NUMERIC) {
      return cell.getNumericCellValue();
    }
    if (cell.getCellType() == CellType.STRING) {
      String s = cell.getStringCellValue();
      if (s == null) {
        return null;
      }
      String trimmed = s.trim();
      // eGRID uses "--" / "N/A" / "NA" for not-applicable or suppressed values.
      if (trimmed.isEmpty() || "--".equals(trimmed) || "N/A".equalsIgnoreCase(trimmed)
          || "NA".equalsIgnoreCase(trimmed)) {
        return null;
      }
      try {
        return Double.parseDouble(trimmed.replace(",", ""));
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private static Map<String, String> buildFieldMap() {
    Map<String, String> m = new HashMap<>();
    m.put("SUBRGN", "subregion_acronym");
    m.put("SRNAME", "subregion_name");
    m.put("SRNAMEPCAP", "nameplate_capacity_mw");
    m.put("SRNGENAN", "annual_net_generation_mwh");
    m.put("SRNGENNB", "annual_nonbaseload_generation_mwh");
    m.put("SRNOXAN", "annual_nox_emissions_tons");
    m.put("SRSO2AN", "annual_so2_emissions_tons");
    m.put("SRCO2AN", "annual_co2_emissions_tons");
    m.put("SRCO2EQA", "annual_co2e_emissions_tons");
    m.put("SRNOXRTA", "nox_output_rate_lb_per_mwh");
    m.put("SRSO2RTA", "so2_output_rate_lb_per_mwh");
    m.put("SRCO2RTA", "co2_output_rate_lb_per_mwh");
    m.put("SRC2ERTA", "co2e_output_rate_lb_per_mwh");
    m.put("SRCNOXRT", "coal_nox_rate_lb_per_mwh");
    m.put("SRONOXRT", "oil_nox_rate_lb_per_mwh");
    m.put("SRGNOXRT", "gas_nox_rate_lb_per_mwh");
    m.put("SRFSNXRT", "fossil_nox_rate_lb_per_mwh");
    m.put("SRCSO2RT", "coal_so2_rate_lb_per_mwh");
    m.put("SROSO2RT", "oil_so2_rate_lb_per_mwh");
    m.put("SRGSO2RT", "gas_so2_rate_lb_per_mwh");
    m.put("SRFSS2RT", "fossil_so2_rate_lb_per_mwh");
    m.put("SRCCO2RT", "coal_co2_rate_lb_per_mwh");
    m.put("SROCO2RT", "oil_co2_rate_lb_per_mwh");
    m.put("SRGCO2RT", "gas_co2_rate_lb_per_mwh");
    m.put("SRFSC2RT", "fossil_co2_rate_lb_per_mwh");
    m.put("SRCLPR", "coal_generation_pct");
    m.put("SROLPR", "oil_generation_pct");
    m.put("SRGSPR", "gas_generation_pct");
    m.put("SRNCPR", "nuclear_generation_pct");
    m.put("SRHYPR", "hydro_generation_pct");
    m.put("SRTRPR", "renewables_generation_pct");
    return m;
  }
}
