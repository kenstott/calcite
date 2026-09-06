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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Parses the EIA-861 "Service_Territory" file (bundled in the same annual ZIP archive
 * that {@link Eia861UtilityTransformer} already downloads for {@code eia_utility_annual})
 * into a utility-to-county crosswalk: one row per (report_year, utility_id, state_abbr,
 * county_name). This is the piece EIA-861 carries that eia_utility_annual's Sales_Ult_Cust
 * sheet does not — a per-utility list of the counties it serves, sourced from the
 * "Counties_States" sheet (50 states + DC; the sibling "Counties_Territories" sheet is out
 * of scope, matching this schema's existing 50-states+DC convention).
 */
public class Eia861ServiceTerritoryTransformer extends EiaBulkXlsxTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    Map<String, String> dims = context.getDimensionValues();
    // Use the effective (data) year, not the publish/iteration year — see
    // Eia861UtilityTransformer for why: dataLag maps the iteration year to the latest
    // available EIA-861 archive and the URL is templated with {effective_year}.
    String yearStr = dims != null ? dims.get("effective_year") : null;
    int year = 0;
    if (yearStr != null && !yearStr.isEmpty()) {
      try {
        year = Integer.parseInt(yearStr);
      } catch (NumberFormatException e) {
        LOGGER.warn("EIA-861 Service_Territory: invalid year dimension value: {}", yearStr);
      }
    }

    // EIA moved 2024+ data out of /archive/zip/ to /zip/ (same move eia_utility_annual handles).
    if (year >= 2024) {
      url = url.replace("/archive/zip/", "/zip/");
    }
    try {
      byte[] zipBytes = downloadBytes(url);
      return parseServiceTerritoryZip(zipBytes, year);
    } catch (Exception e) {
      throw new RuntimeException("EIA-861 Service_Territory: failed to parse ZIP from " + url, e);
    }
  }

  private String parseServiceTerritoryZip(byte[] zipBytes, int year) throws Exception {
    byte[] stBytes = null;
    boolean stIsXls = false;

    ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes));
    try {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String lower = entry.getName().toLowerCase();
        boolean isXlsx = lower.endsWith(".xlsx");
        boolean isXls = !isXlsx && lower.endsWith(".xls");
        if (lower.contains("service_territory") && (isXlsx || isXls)) {
          stBytes = readZipEntry(zis);
          stIsXls = isXls;
        }
        zis.closeEntry();
      }
    } finally {
      zis.close();
    }

    if (stBytes == null) {
      LOGGER.warn("EIA-861 Service_Territory: file not found in ZIP for year {}", year);
      return "[]";
    }

    Workbook wb = stIsXls ? new HSSFWorkbook(new ByteArrayInputStream(stBytes))
        : new XSSFWorkbook(new ByteArrayInputStream(stBytes));
    try {
      return parseServiceTerritorySheet(wb, year);
    } finally {
      wb.close();
    }
  }

  private byte[] readZipEntry(ZipInputStream zis) throws Exception {
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int len;
    while ((len = zis.read(buf)) > 0) {
      baos.write(buf, 0, len);
    }
    return baos.toByteArray();
  }

  private String parseServiceTerritorySheet(Workbook wb, int year) {
    Sheet sheet = wb.getSheet("Counties_States");
    if (sheet == null && wb.getNumberOfSheets() > 0) {
      sheet = wb.getSheetAt(0);
    }
    if (sheet == null) {
      LOGGER.warn("EIA-861 Service_Territory: no Counties_States sheet found for year {}", year);
      return "[]";
    }

    // Single header row: Data Year, Utility Number, Utility Name, Short Form, State, County
    Row headerRow = sheet.getRow(0);
    if (headerRow == null) {
      LOGGER.warn("EIA-861 Service_Territory: no header row for year {}", year);
      return "[]";
    }
    Map<String, Integer> colIndex = new HashMap<>();
    for (int c = 0; c <= headerRow.getLastCellNum(); c++) {
      String hdr = cellString(headerRow.getCell(c));
      if (hdr != null) {
        colIndex.put(hdr.trim().toLowerCase(), c);
      }
    }

    Integer utilCol = colIndex.get("utility number");
    Integer nameCol = colIndex.get("utility name");
    Integer shortCol = colIndex.get("short form");
    Integer stateCol = colIndex.get("state");
    Integer countyCol = colIndex.get("county");
    if (utilCol == null || stateCol == null || countyCol == null) {
      LOGGER.warn("EIA-861 Service_Territory: sheet missing utility/state/county columns for "
          + "year {}", year);
      return "[]";
    }

    ArrayNode result = MAPPER.createArrayNode();
    for (int r = 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String utilIdRaw = cellString(row.getCell(utilCol));
      String state = cellString(row.getCell(stateCol));
      String county = cellString(row.getCell(countyCol));
      if (utilIdRaw == null || utilIdRaw.trim().isEmpty()
          || state == null || state.trim().isEmpty()
          || county == null || county.trim().isEmpty()) {
        continue;
      }

      int utilId;
      try {
        utilId = Integer.parseInt(utilIdRaw.trim());
      } catch (NumberFormatException e) {
        continue;
      }

      ObjectNode out = MAPPER.createObjectNode();
      out.put("report_year", year);
      out.put("utility_id", utilId);

      String name = nameCol != null ? cellString(row.getCell(nameCol)) : null;
      if (name != null && !name.trim().isEmpty()) {
        out.put("utility_name", name.trim());
      } else {
        out.putNull("utility_name");
      }

      String shortForm = shortCol != null ? cellString(row.getCell(shortCol)) : null;
      if (shortForm != null && !shortForm.trim().isEmpty()) {
        String sf = shortForm.trim();
        out.put("short_form_filer", "y".equalsIgnoreCase(sf) || "yes".equalsIgnoreCase(sf));
      } else {
        out.put("short_form_filer", false);
      }

      out.put("state_abbr", state.trim());
      out.put("county_name", county.trim());

      result.add(out);
    }

    LOGGER.debug("EIA-861 Service_Territory: parsed {} utility-county rows for year {}",
        result.size(), year);
    return result.toString();
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception {
    // Never invoked: transform() above is fully overridden because the source is a ZIP
    // archive, not a bare workbook. Implemented only to satisfy the abstract contract,
    // matching Eia861UtilityTransformer's convention.
    return parseServiceTerritorySheet(workbook, 0);
  }
}
