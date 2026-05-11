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
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Eia860PowerPlantsTransformer extends EiaBulkXlsxTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    Map<String, String> dims = context.getDimensionValues();
    String yearStr = dims != null ? dims.get("year") : null;
    String year = yearStr != null ? yearStr : "";

    try {
      byte[] zipBytes = downloadBytes(url);
      return parseEia860Zip(zipBytes, year);
    } catch (Exception e) {
      LOGGER.error("Failed to parse EIA-860 ZIP from {}: {}", url, e.getMessage());
      return "[]";
    }
  }

  private String parseEia860Zip(byte[] zipBytes, String year) throws Exception {
    byte[] plantBytes = null;
    byte[] generatorBytes = null;

    ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes));
    try {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        String lower = name.toLowerCase();
        if (lower.matches(".*2___plant_y\\d+\\.xlsx")) {
          plantBytes = readZipEntry(zis);
        } else if (lower.matches(".*3_1_generator_y\\d+\\.xlsx")) {
          generatorBytes = readZipEntry(zis);
        }
        zis.closeEntry();
      }
    } finally {
      zis.close();
    }

    if (plantBytes == null) {
      LOGGER.warn("EIA-860: Plant file not found in ZIP for year {}", year);
      return "[]";
    }
    if (generatorBytes == null) {
      LOGGER.warn("EIA-860: Generator file not found in ZIP for year {}", year);
      return "[]";
    }

    Map<String, Map<String, String>> plantData = parsePlantFile(plantBytes);
    return parseGeneratorFile(generatorBytes, plantData, year);
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

  private Map<String, Map<String, String>> parsePlantFile(byte[] xlsxBytes) throws Exception {
    Map<String, Map<String, String>> result = new HashMap<>();
    XSSFWorkbook wb = new XSSFWorkbook(new ByteArrayInputStream(xlsxBytes));
    try {
      Sheet sheet = wb.getSheetAt(0);
      if (sheet == null) {
        return result;
      }

      // Skip first row (cover page); header is on row index 1
      Row headerRow = findHeaderRow(sheet, "Plant Code");
      if (headerRow == null) {
        return result;
      }

      Map<String, Integer> colIndex = buildColumnIndex(headerRow);
      int startRow = headerRow.getRowNum() + 1;

      for (int r = startRow; r <= sheet.getLastRowNum(); r++) {
        Row row = sheet.getRow(r);
        if (row == null) {
          continue;
        }
        String plantId = colIndex.containsKey("Plant Code")
            ? cellString(row.getCell(colIndex.get("Plant Code"))) : null;
        if (plantId == null || plantId.isEmpty()) {
          continue;
        }
        Map<String, String> data = new HashMap<>();
        for (Map.Entry<String, Integer> entry : colIndex.entrySet()) {
          String val = cellString(row.getCell(entry.getValue()));
          if (val != null) {
            data.put(entry.getKey(), val);
          }
        }
        result.put(plantId.trim(), data);
      }
    } finally {
      wb.close();
    }
    return result;
  }

  private String parseGeneratorFile(byte[] xlsxBytes, Map<String, Map<String, String>> plantData,
      String year) throws Exception {
    XSSFWorkbook wb = new XSSFWorkbook(new ByteArrayInputStream(xlsxBytes));
    try {
      Sheet sheet = wb.getSheetAt(0);
      if (sheet == null) {
        return "[]";
      }

      Row headerRow = findHeaderRow(sheet, "Generator ID");
      if (headerRow == null) {
        return "[]";
      }

      Map<String, Integer> colIndex = buildColumnIndex(headerRow);
      int startRow = headerRow.getRowNum() + 1;
      ArrayNode result = MAPPER.createArrayNode();

      for (int r = startRow; r <= sheet.getLastRowNum(); r++) {
        Row row = sheet.getRow(r);
        if (row == null) {
          continue;
        }

        String plantId = colIndex.containsKey("Plant Code")
            ? cellString(row.getCell(colIndex.get("Plant Code"))) : null;
        if (plantId == null || plantId.isEmpty()) {
          continue;
        }

        ObjectNode out = MAPPER.createObjectNode();
        if (year != null && !year.isEmpty()) {
          try {
            out.put("report_year", Integer.parseInt(year));
          } catch (NumberFormatException e) {
            out.putNull("report_year");
          }
        }

        // Plant attributes (denormalized from plant lookup)
        Map<String, String> plant = plantData.get(plantId.trim());
        if (plant != null) {
          putPlantFields(out, plant);
        } else {
          out.put("plant_id", plantId.trim());
        }

        // Generator attributes
        putStringField(out, "generator_id", row, colIndex, "Generator ID");
        putStringField(out, "technology", row, colIndex, "Technology");
        putStringField(out, "prime_mover", row, colIndex, "Prime Mover");
        putStringField(out, "energy_source_1", row, colIndex, "Energy Source 1");
        putStringField(out, "energy_source_2", row, colIndex, "Energy Source 2");
        putStringField(out, "energy_source_3", row, colIndex, "Energy Source 3");
        putDoubleField(out, "nameplate_capacity_mw", row, colIndex, "Nameplate Capacity (MW)");
        putDoubleField(out, "net_summer_capacity_mw", row, colIndex, "Summer Capacity (MW)");
        putDoubleField(out, "net_winter_capacity_mw", row, colIndex, "Winter Capacity (MW)");
        putDoubleField(out, "minimum_load_mw", row, colIndex, "Minimum Load (MW)");
        putStringField(out, "ownership_code", row, colIndex, "Ownership");
        putStringField(out, "operating_status", row, colIndex, "Status");
        putIntField(out, "operating_month", row, colIndex, "Operating Month");
        putIntField(out, "operating_year", row, colIndex, "Operating Year");
        putIntField(out, "planned_retirement_month", row, colIndex, "Planned Retirement Month");
        putIntField(out, "planned_retirement_year", row, colIndex, "Planned Retirement Year");
        putStringField(out, "energy_storage_flag", row, colIndex, "Energy Storage");

        result.add(out);
      }

      LOGGER.debug("EIA-860: parsed {} generator records for year {}", result.size(), year);
      return result.toString();
    } finally {
      wb.close();
    }
  }

  private void putPlantFields(ObjectNode out, Map<String, String> plant) {
    putMapField(out, "plant_id", plant, "Plant Code");
    putMapField(out, "plant_name", plant, "Plant Name");
    putMapField(out, "utility_id", plant, "Utility ID");
    putMapField(out, "utility_name", plant, "Utility Name");
    putMapField(out, "state_abbr", plant, "State");
    putMapField(out, "county_name", plant, "County");
    putMapField(out, "latitude", plant, "Latitude");
    putMapField(out, "longitude", plant, "Longitude");
    putMapField(out, "nerc_region", plant, "NERC Region");
    putMapField(out, "balancing_authority_code", plant, "Balancing Authority Code");
    putMapField(out, "balancing_authority_name", plant, "Balancing Authority Name");
    putMapField(out, "primary_purpose_naics", plant, "Primary Purpose NAICS Code");
    putMapField(out, "regulatory_status", plant, "Regulatory Status");
    putMapField(out, "sector_code", plant, "Sector Number");
    putMapField(out, "sector", plant, "Sector Name");
  }

  private void putMapField(ObjectNode out, String targetKey, Map<String, String> map,
      String sourceKey) {
    String val = map.get(sourceKey);
    if (val != null && !val.isEmpty()) {
      out.put(targetKey, val);
    } else {
      out.putNull(targetKey);
    }
  }

  private void putStringField(ObjectNode out, String targetKey, Row row,
      Map<String, Integer> colIndex, String colName) {
    if (!colIndex.containsKey(colName)) {
      out.putNull(targetKey);
      return;
    }
    String val = cellString(row.getCell(colIndex.get(colName)));
    if (val != null && !val.isEmpty()) {
      out.put(targetKey, val);
    } else {
      out.putNull(targetKey);
    }
  }

  private void putDoubleField(ObjectNode out, String targetKey, Row row,
      Map<String, Integer> colIndex, String colName) {
    if (!colIndex.containsKey(colName)) {
      out.putNull(targetKey);
      return;
    }
    Double val = cellDouble(row.getCell(colIndex.get(colName)));
    if (val != null) {
      out.put(targetKey, val);
    } else {
      out.putNull(targetKey);
    }
  }

  private void putIntField(ObjectNode out, String targetKey, Row row,
      Map<String, Integer> colIndex, String colName) {
    if (!colIndex.containsKey(colName)) {
      out.putNull(targetKey);
      return;
    }
    Integer val = cellInt(row.getCell(colIndex.get(colName)));
    if (val != null) {
      out.put(targetKey, val);
    } else {
      out.putNull(targetKey);
    }
  }

  private Row findHeaderRow(Sheet sheet, String keyColumn) {
    for (int r = 0; r <= Math.min(sheet.getLastRowNum(), 10); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      for (int c = 0; c <= row.getLastCellNum(); c++) {
        String val = cellString(row.getCell(c));
        if (keyColumn.equals(val)) {
          return row;
        }
      }
    }
    return null;
  }

  private Map<String, Integer> buildColumnIndex(Row headerRow) {
    Map<String, Integer> index = new HashMap<>();
    for (int c = 0; c <= headerRow.getLastCellNum(); c++) {
      String hdr = cellString(headerRow.getCell(c));
      if (hdr != null && !hdr.trim().isEmpty()) {
        index.put(hdr.trim(), c);
      }
    }
    return index;
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception {
    // Not used — transform() is overridden to handle ZIPs
    return "[]";
  }
}
