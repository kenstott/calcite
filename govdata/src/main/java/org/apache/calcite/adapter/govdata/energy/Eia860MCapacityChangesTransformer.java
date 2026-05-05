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
import java.util.LinkedHashMap;
import java.util.Map;

public class Eia860MCapacityChangesTransformer extends EiaBulkXlsxTransformer {

  private static final Map<String, String> SHEET_CHANGE_TYPES = new LinkedHashMap<>();

  static {
    SHEET_CHANGE_TYPES.put("Operating", "Addition");
    SHEET_CHANGE_TYPES.put("Retired", "Retirement");
    SHEET_CHANGE_TYPES.put("Planned", "Planned Addition");
    SHEET_CHANGE_TYPES.put("Canceled or Postponed", "Cancellation");
  }

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    try {
      byte[] xlsxBytes = downloadBytes(url);
      XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(xlsxBytes));
      try {
        return parseWorkbook(workbook, context);
      } finally {
        workbook.close();
      }
    } catch (Exception e) {
      LOGGER.error("Failed to parse EIA-860M XLSX from {}: {}", url, e.getMessage());
      return "[]";
    }
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception {
    Map<String, String> dims = context.getDimensionValues();
    String yearStr = dims != null ? dims.get("year") : null;
    String monthStr = dims != null ? dims.get("month") : null;
    Integer snapshotYear = parseIntOrNull(yearStr);
    Integer snapshotMonth = parseIntOrNull(monthStr);

    ArrayNode result = MAPPER.createArrayNode();

    for (Map.Entry<String, String> entry : SHEET_CHANGE_TYPES.entrySet()) {
      String sheetName = entry.getKey();
      String changeType = entry.getValue();
      Sheet sheet = workbook.getSheet(sheetName);
      if (sheet == null) {
        LOGGER.debug("EIA-860M: sheet '{}' not found, skipping", sheetName);
        continue;
      }
      parseSheet(sheet, changeType, snapshotYear, snapshotMonth, result);
    }

    LOGGER.debug("EIA-860M: parsed {} capacity change records total", result.size());
    return result.toString();
  }

  private Integer parseIntOrNull(String s) {
    if (s == null || s.trim().isEmpty()) {
      return null;
    }
    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private void parseSheet(Sheet sheet, String changeType, Integer snapshotYear,
      Integer snapshotMonth, ArrayNode result) {
    Row headerRow = findHeaderRow(sheet);
    if (headerRow == null) {
      LOGGER.warn("EIA-860M: no header row found in sheet for change_type={}", changeType);
      return;
    }

    Map<String, Integer> colIndex = buildColumnIndex(headerRow);
    int startRow = headerRow.getRowNum() + 1;

    for (int r = startRow; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }

      // Skip empty rows
      String plantId = colIndex.containsKey("Plant ID")
          ? cellString(row.getCell(colIndex.get("Plant ID"))) : null;
      if (plantId == null || plantId.isEmpty()) {
        continue;
      }

      ObjectNode out = MAPPER.createObjectNode();
      out.put("change_type", changeType);

      // Snapshot period from dimension values (which monthly report this row came from)
      if (snapshotYear != null) {
        out.put("snapshot_year", snapshotYear);
      } else {
        out.putNull("snapshot_year");
      }
      if (snapshotMonth != null) {
        out.put("snapshot_month", snapshotMonth);
      } else {
        out.putNull("snapshot_month");
      }

      putStringField(out, "plant_id", row, colIndex, "Plant ID");
      putStringField(out, "plant_name", row, colIndex, "Plant Name");
      // EIA-860M uses "Plant State" in newer files; "State" in older files
      String stateCol = colIndex.containsKey("Plant State") ? "Plant State" : "State";
      putStringField(out, "state_abbr", row, colIndex, stateCol);
      putStringField(out, "county_name", row, colIndex, "County");
      putStringField(out, "generator_id", row, colIndex, "Generator ID");
      putStringField(out, "unit_code", row, colIndex, "Unit Code");
      putStringField(out, "ownership", row, colIndex, "Ownership");
      putStringField(out, "duct_burners", row, colIndex, "Duct Burners");
      putStringField(out, "technology", row, colIndex, "Technology");
      putStringField(out, "energy_source_1", row, colIndex, "Energy Source 1");
      putStringField(out, "prime_mover", row, colIndex, "Prime Mover");
      putDoubleField(out, "nameplate_capacity_mw", row, colIndex, "Nameplate Capacity (MW)");
      putDoubleField(out, "net_summer_capacity_mw", row, colIndex, "Summer Capacity (MW)");
      putDoubleField(out, "net_winter_capacity_mw", row, colIndex, "Winter Capacity (MW)");
      out.putNull("nameplate_energy_capacity_mwh"); // not available in EIA-860M
      // change_year/change_month: the effective date of the capacity change
      putIntField(out, "change_year", row, colIndex, "Operating Year");
      putIntField(out, "change_month", row, colIndex, "Operating Month");
      putIntField(out, "operating_month", row, colIndex, "Operating Month");
      putIntField(out, "operating_year", row, colIndex, "Operating Year");
      putIntField(out, "planned_retirement_month", row, colIndex, "Planned Retirement Month");
      putIntField(out, "planned_retirement_year", row, colIndex, "Planned Retirement Year");
      putStringField(out, "current_month", row, colIndex, "Current Month");
      putStringField(out, "current_year", row, colIndex, "Current Year");
      putStringField(out, "balancing_authority_code", row, colIndex, "Balancing Authority Code");
      putStringField(out, "nerc_region", row, colIndex, "NERC Region");
      putStringField(out, "energy_storage_flag", row, colIndex, "Energy Storage");

      result.add(out);
    }
  }

  private Row findHeaderRow(Sheet sheet) {
    for (int r = 0; r <= Math.min(sheet.getLastRowNum(), 5); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      for (int c = 0; c <= row.getLastCellNum(); c++) {
        String val = cellString(row.getCell(c));
        if ("Plant ID".equals(val) || "Generator ID".equals(val)) {
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
}
