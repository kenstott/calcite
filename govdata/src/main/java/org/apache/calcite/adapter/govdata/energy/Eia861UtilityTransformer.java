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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Eia861UtilityTransformer extends EiaBulkXlsxTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    Map<String, String> dims = context.getDimensionValues();
    String yearStr = dims != null ? dims.get("year") : null;
    int year = 0;
    if (yearStr != null && !yearStr.isEmpty()) {
      try {
        year = Integer.parseInt(yearStr);
      } catch (NumberFormatException e) {
        LOGGER.warn("EIA-861: invalid year dimension value: {}", yearStr);
      }
    }

    try {
      byte[] zipBytes = downloadBytes(url);
      return parseEia861Zip(zipBytes, year);
    } catch (Exception e) {
      LOGGER.error("Failed to parse EIA-861 ZIP from {}: {}", url, e.getMessage());
      return "[]";
    }
  }

  private String parseEia861Zip(byte[] zipBytes, int year) throws Exception {
    // Collect bytes for files we want to parse
    byte[] salesBytes = null;
    byte[] utilityBytes = null;

    ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes));
    try {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        String lower = name.toLowerCase();
        if (lower.contains("sales_ult_cust") && lower.endsWith(".xlsx")) {
          salesBytes = readZipEntry(zis);
        } else if (lower.contains("utility_data") && lower.endsWith(".xlsx")) {
          utilityBytes = readZipEntry(zis);
        }
        zis.closeEntry();
      }
    } finally {
      zis.close();
    }

    if (salesBytes == null) {
      LOGGER.warn("EIA-861: Sales_Ult_Cust sheet not found in ZIP for year {}", year);
      return "[]";
    }

    // Parse utility data for lookup if available
    Map<String, Map<String, String>> utilityLookup = new HashMap<>();
    if (utilityBytes != null) {
      XSSFWorkbook utilWb = new XSSFWorkbook(new ByteArrayInputStream(utilityBytes));
      try {
        utilityLookup = parseUtilityData(utilWb);
      } finally {
        utilWb.close();
      }
    }

    XSSFWorkbook salesWb = new XSSFWorkbook(new ByteArrayInputStream(salesBytes));
    try {
      return parseSalesSheet(salesWb, utilityLookup, year);
    } finally {
      salesWb.close();
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

  private Map<String, Map<String, String>> parseUtilityData(XSSFWorkbook wb) {
    Map<String, Map<String, String>> result = new HashMap<>();
    Sheet sheet = wb.getSheet("Utility_Data");
    if (sheet == null && wb.getNumberOfSheets() > 0) {
      sheet = wb.getSheetAt(0);
    }
    if (sheet == null) {
      return result;
    }

    Row headerRow = sheet.getRow(0);
    if (headerRow == null) {
      return result;
    }

    Map<String, Integer> colIndex = new HashMap<>();
    for (int c = 0; c <= headerRow.getLastCellNum(); c++) {
      String hdr = cellString(headerRow.getCell(c));
      if (hdr != null) {
        colIndex.put(hdr.trim(), c);
      }
    }

    for (int r = 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String utilId = colIndex.containsKey("Utility Number")
          ? cellString(row.getCell(colIndex.get("Utility Number"))) : null;
      if (utilId == null || utilId.isEmpty()) {
        continue;
      }
      Map<String, String> data = new HashMap<>();
      for (Map.Entry<String, Integer> entry : colIndex.entrySet()) {
        String val = cellString(row.getCell(entry.getValue()));
        if (val != null) {
          data.put(entry.getKey(), val);
        }
      }
      result.put(utilId.trim(), data);
    }
    return result;
  }

  private String parseSalesSheet(XSSFWorkbook wb, Map<String, Map<String, String>> utilityLookup,
      int year) {
    Sheet sheet = wb.getSheet("Sales_Ult_Cust");
    if (sheet == null) {
      for (int i = 0; i < wb.getNumberOfSheets(); i++) {
        String name = wb.getSheetName(i).toLowerCase();
        if (name.contains("sales")) {
          sheet = wb.getSheetAt(i);
          break;
        }
      }
    }
    if (sheet == null) {
      LOGGER.warn("EIA-861: Could not find Sales_Ult_Cust sheet");
      return "[]";
    }

    // EIA-861 Sales_Ult_Cust has a 3-row composite header
    List<String> headers = buildCompositeHeaders(sheet, 3);
    if (headers.isEmpty()) {
      LOGGER.warn("EIA-861: No headers found in Sales_Ult_Cust sheet");
      return "[]";
    }

    ArrayNode result = MAPPER.createArrayNode();
    int dataStartRow = 3;

    for (int r = dataStartRow; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }

      ObjectNode out = MAPPER.createObjectNode();
      out.put("data_year", year);

      for (int c = 0; c < headers.size(); c++) {
        String header = headers.get(c);
        if (header == null || header.isEmpty()) {
          continue;
        }
        String val = cellString(row.getCell(c));
        out.put(normalizeColumnName(header), val != null ? val : "");
      }

      // Enrich with utility lookup data if available
      String utilId = out.has("utility_number") ? out.get("utility_number").asText() : null;
      if (utilId != null && !utilId.isEmpty() && utilityLookup.containsKey(utilId)) {
        Map<String, String> utilData = utilityLookup.get(utilId);
        for (Map.Entry<String, String> entry : utilData.entrySet()) {
          String key = normalizeColumnName(entry.getKey());
          if (!out.has(key)) {
            out.put(key, entry.getValue());
          }
        }
      }

      result.add(out);
    }

    LOGGER.debug("EIA-861: parsed {} sales records for year {}", result.size(), year);
    return result.toString();
  }

  private List<String> buildCompositeHeaders(Sheet sheet, int numHeaderRows) {
    List<String> combined = new ArrayList<>();
    if (sheet.getLastRowNum() < numHeaderRows - 1) {
      return combined;
    }

    Row firstRow = sheet.getRow(0);
    if (firstRow == null) {
      return combined;
    }

    int maxCols = firstRow.getLastCellNum();
    for (int c = 0; c < maxCols; c++) {
      StringBuilder sb = new StringBuilder();
      for (int r = 0; r < numHeaderRows; r++) {
        Row hrow = sheet.getRow(r);
        if (hrow == null) {
          continue;
        }
        String part = cellString(hrow.getCell(c));
        if (part != null && !part.trim().isEmpty()) {
          if (sb.length() > 0) {
            sb.append("_");
          }
          sb.append(part.trim());
        }
      }
      combined.add(sb.toString());
    }
    return combined;
  }

  private String normalizeColumnName(String header) {
    if (header == null) {
      return "col";
    }
    return header.toLowerCase()
        .replaceAll("[^a-z0-9]+", "_")
        .replaceAll("^_+|_+$", "");
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception {
    // This is called by the base class transform() but we override transform() entirely
    return parseSalesSheet(workbook, new HashMap<String, Map<String, String>>(), 0);
  }
}
