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
import org.apache.poi.ss.util.CellRangeAddress;
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

    // EIA moved 2024+ data out of /archive/zip/ to /zip/
    if (year >= 2024) {
      url = url.replace("/archive/zip/", "/zip/");
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
    // Collect bytes and track whether each file is .xls or .xlsx
    byte[] salesBytes = null;
    boolean salesIsXls = false;
    byte[] utilityBytes = null;
    boolean utilityIsXls = false;
    byte[] operationalBytes = null;
    boolean operationalIsXls = false;

    ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes));
    try {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        String lower = name.toLowerCase();
        // Accept both .xls (2013-2014) and .xlsx (2012, 2015+); exclude CS variant
        boolean isXlsx = lower.endsWith(".xlsx");
        boolean isXls = !isXlsx && lower.endsWith(".xls");
        if (lower.contains("sales_ult_cust") && !lower.contains("_cs") && (isXlsx || isXls)) {
          salesBytes = readZipEntry(zis);
          salesIsXls = isXls;
        } else if (lower.contains("utility_data") && (isXlsx || isXls)) {
          utilityBytes = readZipEntry(zis);
          utilityIsXls = isXls;
        } else if (lower.contains("operational_data") && (isXlsx || isXls)) {
          operationalBytes = readZipEntry(zis);
          operationalIsXls = isXls;
        }
        zis.closeEntry();
      }
    } finally {
      zis.close();
    }

    if (salesBytes == null) {
      LOGGER.warn("EIA-861: Sales_Ult_Cust file not found in ZIP for year {}", year);
      return "[]";
    }

    // Parse utility data for lookup if available
    Map<String, Map<String, String>> utilityLookup = new HashMap<>();
    if (utilityBytes != null) {
      Workbook utilWb = openWorkbook(utilityBytes, utilityIsXls);
      try {
        utilityLookup = parseUtilityData(utilWb);
      } finally {
        utilWb.close();
      }
    }

    // Parse operational data (summer/winter peak demand, net generation) if available
    Map<String, Map<String, Double>> operationalLookup = new HashMap<>();
    if (operationalBytes != null) {
      Workbook opWb = openWorkbook(operationalBytes, operationalIsXls);
      try {
        operationalLookup = parseOperationalData(opWb, year);
      } finally {
        opWb.close();
      }
    }

    Workbook salesWb = openWorkbook(salesBytes, salesIsXls);
    try {
      return parseSalesSheet(salesWb, utilityLookup, operationalLookup, year);
    } finally {
      salesWb.close();
    }
  }

  private Workbook openWorkbook(byte[] bytes, boolean isXls) throws Exception {
    if (isXls) {
      return new HSSFWorkbook(new ByteArrayInputStream(bytes));
    }
    return new XSSFWorkbook(new ByteArrayInputStream(bytes));
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

  private Map<String, Map<String, String>> parseUtilityData(Workbook wb) {
    Map<String, Map<String, String>> result = new HashMap<>();
    Sheet sheet = wb.getSheet("Utility_Data");
    if (sheet == null && wb.getNumberOfSheets() > 0) {
      sheet = wb.getSheetAt(0);
    }
    if (sheet == null) {
      return result;
    }

    // Utility_Data sheet uses a 2-row header: row 0 has category labels, row 1 has column names.
    // Scan the first 5 rows to find the one containing "Utility Number".
    Row headerRow = null;
    int headerRowIdx = 0;
    for (int r = 0; r <= Math.min(4, sheet.getLastRowNum()); r++) {
      Row candidate = sheet.getRow(r);
      if (candidate == null) continue;
      for (int c = 0; c <= candidate.getLastCellNum(); c++) {
        String v = cellString(candidate.getCell(c));
        if ("Utility Number".equals(v)) {
          headerRow = candidate;
          headerRowIdx = r;
          break;
        }
      }
      if (headerRow != null) break;
    }
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

    for (int r = headerRowIdx + 1; r <= sheet.getLastRowNum(); r++) {
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

  private String parseSalesSheet(Workbook wb, Map<String, Map<String, String>> utilityLookup,
      Map<String, Map<String, Double>> operationalLookup, int year) {
    Sheet sheet = wb.getSheet("Sales_Ult_Cust");
    if (sheet == null) {
      sheet = wb.getSheet("States");
    }
    if (sheet == null) {
      for (int i = 0; i < wb.getNumberOfSheets(); i++) {
        String name = wb.getSheetName(i).toLowerCase();
        if (name.contains("sales") || name.equals("states") || name.equals("territories")) {
          sheet = wb.getSheetAt(i);
          break;
        }
      }
    }
    if (sheet == null) {
      LOGGER.warn("EIA-861: Could not find sales sheet (tried Sales_Ult_Cust, States, territories)");
      return "[]";
    }

    // Build composite headers from 3 header rows, then map each to a schema column name.
    List<String> rawHeaders = buildCompositeHeaders(sheet, 3);
    if (rawHeaders.isEmpty()) {
      LOGGER.warn("EIA-861: No headers found in sales sheet for year {}", year);
      return "[]";
    }

    // Map raw composite header index → schema column name (null = skip column).
    List<String> schemaColumns = new ArrayList<String>();
    for (String raw : rawHeaders) {
      schemaColumns.add(mapToSchemaColumn(raw));
    }
    LOGGER.debug("EIA-861 year {} sheet '{}': {} columns, mapped: {}",
        year, sheet.getSheetName(), rawHeaders.size(), schemaColumns);

    ArrayNode result = MAPPER.createArrayNode();
    int dataStartRow = 3;

    for (int r = dataStartRow; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }

      // Skip rows with no utility number (subtotals, blank rows)
      String utilNumRaw = null;
      for (int c = 0; c < schemaColumns.size(); c++) {
        if ("utility_id".equals(schemaColumns.get(c))) {
          utilNumRaw = cellString(row.getCell(c));
          break;
        }
      }
      if (utilNumRaw == null || utilNumRaw.trim().isEmpty()) {
        continue;
      }

      ObjectNode out = MAPPER.createObjectNode();
      out.put("report_year", year);

      for (int c = 0; c < schemaColumns.size(); c++) {
        String col = schemaColumns.get(c);
        if (col == null) {
          continue;
        }
        String val = cellString(row.getCell(c));
        String v = val != null ? val.trim() : "";
        putTypedValue(out, col, v);
      }

      // Enrich from utility_data lookup using the parsed utility_id
      String utilId = out.has("utility_id") ? out.get("utility_id").asText() : null;
      if (utilId != null && !utilId.isEmpty() && utilityLookup.containsKey(utilId)) {
        Map<String, String> utilData = utilityLookup.get(utilId);
        if (!out.has("nerc_region") || out.get("nerc_region").isNull()) {
          String nerc = utilData.get("NERC Region");
          if (nerc == null) nerc = utilData.get("Nerc Region");
          if (nerc == null) nerc = utilData.get("NERC");
          if (nerc == null) nerc = utilData.get("nerc_region");
          if (nerc != null && !nerc.isEmpty()) out.put("nerc_region", nerc);
        }
        if (!out.has("ba_code") || out.get("ba_code").isNull()) {
          String ba = utilData.get("Balancing Authority Code");
          if (ba == null) ba = utilData.get("BA Code");
          if (ba == null) ba = utilData.get("Balancing Authority");
          if (ba == null) ba = utilData.get("BA");
          if (ba == null) ba = utilData.get("ba_code");
          if (ba != null && !ba.isEmpty()) out.put("ba_code", ba);
        }
        // Activity flags: EIA-861 Utility_Data sheet has Y/N columns for each activity
        if (!out.has("activity_generation") || out.get("activity_generation").isNull()) {
          String gen = utilData.get("Generator");
          if (gen == null) gen = utilData.get("Generates Electricity");
          if (gen == null) gen = utilData.get("Generation");
          if (gen != null && !gen.isEmpty()) {
            out.put("activity_generation", "y".equalsIgnoreCase(gen) || "yes".equalsIgnoreCase(gen) || "1".equals(gen));
          }
        }
        if (!out.has("activity_transmission") || out.get("activity_transmission").isNull()) {
          String tran = utilData.get("Transmission");
          if (tran == null) tran = utilData.get("Transmits Electricity");
          if (tran != null && !tran.isEmpty()) {
            out.put("activity_transmission", "y".equalsIgnoreCase(tran) || "yes".equalsIgnoreCase(tran) || "1".equals(tran));
          }
        }
        if (!out.has("activity_distribution") || out.get("activity_distribution").isNull()) {
          String dist = utilData.get("Distribution");
          if (dist == null) dist = utilData.get("Distributes Electricity");
          if (dist != null && !dist.isEmpty()) {
            out.put("activity_distribution", "y".equalsIgnoreCase(dist) || "yes".equalsIgnoreCase(dist) || "1".equals(dist));
          }
        }
      }

      // Enrich from Operational_Data sheet (summer/winter peak demand, net generation)
      if (utilId != null && !utilId.isEmpty() && operationalLookup.containsKey(utilId)) {
        Map<String, Double> opData = operationalLookup.get(utilId);
        for (Map.Entry<String, Double> opEntry : opData.entrySet()) {
          String opCol = opEntry.getKey();
          if (!out.has(opCol) || out.get(opCol).isNull()) {
            out.put(opCol, opEntry.getValue());
          }
        }
      }

      result.add(out);
    }

    LOGGER.debug("EIA-861: parsed {} sales records for year {}", result.size(), year);
    return result.toString();
  }

  private Map<String, Map<String, Double>> parseOperationalData(Workbook wb, int year) {
    Map<String, Map<String, Double>> result = new HashMap<>();
    Sheet sheet = wb.getSheet("Operational_Data");
    if (sheet == null) {
      for (int i = 0; i < wb.getNumberOfSheets(); i++) {
        String name = wb.getSheetName(i).toLowerCase();
        if (name.contains("operational")) {
          sheet = wb.getSheetAt(i);
          break;
        }
      }
    }
    if (sheet == null) {
      LOGGER.warn("EIA-861: Operational_Data sheet not found for year {}", year);
      return result;
    }

    List<String> rawHeaders = buildCompositeHeaders(sheet, 3);
    List<String> schemaColumns = new ArrayList<String>();
    for (String raw : rawHeaders) {
      schemaColumns.add(mapToSchemaColumn(raw));
    }

    int utilIdCol = -1;
    for (int i = 0; i < schemaColumns.size(); i++) {
      if ("utility_id".equals(schemaColumns.get(i))) {
        utilIdCol = i;
        break;
      }
    }
    if (utilIdCol < 0) {
      LOGGER.warn("EIA-861: Operational_Data has no utility_id column for year {}", year);
      return result;
    }

    for (int r = 3; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String utilId = cellString(row.getCell(utilIdCol));
      if (utilId == null || utilId.trim().isEmpty()) {
        continue;
      }
      utilId = utilId.trim();

      Map<String, Double> data = new HashMap<>();
      for (int c = 0; c < schemaColumns.size(); c++) {
        String col = schemaColumns.get(c);
        if ("summer_peak_demand_mw".equals(col) || "winter_peak_demand_mw".equals(col)
            || "net_generation_mwh".equals(col)) {
          String val = cellString(row.getCell(c));
          if (val != null && !val.trim().isEmpty()) {
            try {
              data.put(col, Double.parseDouble(val.replace(",", "").trim()));
            } catch (NumberFormatException e) {
              // non-numeric cell — skip
            }
          }
        }
      }
      if (!data.isEmpty()) {
        result.put(utilId, data);
      }
    }
    LOGGER.debug("EIA-861: parsed {} operational records for year {}", result.size(), year);
    return result;
  }

  /**
   * Map a raw composite header (from 3-row merge) to its schema column name.
   * Returns null for columns that should be skipped.
   *
   * EIA-861 States sheet layout (varies slightly by year):
   * - Rows 0-2 merged for identifiers: Utility Number, Utility Name, State, etc.
   * - Row 0: sector label (Residential, Commercial, Industrial, Transportation, Total)
   * - Row 1: measure (Number of Customers, Revenue ($1000), Sales (MWh))
   * - Row 2: sub-label (sometimes blank)
   */
  private String mapToSchemaColumn(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      return null;
    }
    String h = raw.toLowerCase().trim();

    // Utility identity columns (appear in first few columns, single-span headers)
    if (h.contains("utility number") || h.equals("utility_number") || h.equals("utilityid")
        || h.equals("utility id")) {
      return "utility_id";
    }
    if (h.contains("utility name")) {
      return "utility_name";
    }
    if (h.equals("state") || h.endsWith("_state") || h.endsWith(" state")
        || h.contains("state abbr") || h.contains("state abbreviation")) {
      return "state_abbr";
    }
    if (h.contains("ownership") || h.contains("entity type") || h.contains("ownership type")) {
      return "entity_type";
    }
    if (h.contains("balancing authority code") || h.contains("ba code") || h.equals("ba_code")
        || (h.contains("balancing") && h.contains("code"))) {
      return "ba_code";
    }
    if (h.contains("nerc") && h.contains("region")) {
      return "nerc_region";
    }
    if (h.contains("service type")) {
      return "service_type";
    }
    if (h.contains("data type") && !h.contains("data_year")) {
      return "data_type";
    }

    // Activity booleans: EIA-861 has columns for Generation/Transmission/Distribution activity
    if (isActivityCol(h, "generation") && !h.contains("net") && !h.contains("mwh")
        && !h.contains("gwh") && !h.contains("sales") && !h.contains("revenue")) {
      return "activity_generation";
    }
    if (isActivityCol(h, "transmission")) {
      return "activity_transmission";
    }
    if (isActivityCol(h, "distribution")) {
      return "activity_distribution";
    }

    // Customers by sector
    if (h.contains("residential") && isCustomers(h)) {
      return "customers_residential";
    }
    if (h.contains("commercial") && isCustomers(h)) {
      return "customers_commercial";
    }
    if (h.contains("industrial") && isCustomers(h)) {
      return "customers_industrial";
    }
    if ((h.contains("transportation") || h.contains("transport")) && isCustomers(h)) {
      return "customers_transportation";
    }
    if (isTotal(h) && isCustomers(h)) {
      return "customers_total";
    }

    // Sales (MWh) by sector
    if (h.contains("residential") && isSalesMwh(h)) {
      return "sales_residential_mwh";
    }
    if (h.contains("commercial") && isSalesMwh(h)) {
      return "sales_commercial_mwh";
    }
    if (h.contains("industrial") && isSalesMwh(h)) {
      return "sales_industrial_mwh";
    }
    if ((h.contains("transportation") || h.contains("transport")) && isSalesMwh(h)) {
      return "sales_transportation_mwh";
    }
    if (isTotal(h) && isSalesMwh(h)) {
      return "sales_total_mwh";
    }

    // Revenue (thousands) by sector
    if (h.contains("residential") && isRevenue(h)) {
      return "revenue_residential_thousand";
    }
    if (h.contains("commercial") && isRevenue(h)) {
      return "revenue_commercial_thousand";
    }
    if (h.contains("industrial") && isRevenue(h)) {
      return "revenue_industrial_thousand";
    }
    if (isTotal(h) && isRevenue(h)) {
      return "revenue_total_thousand";
    }

    // Peak demand and generation
    if (h.contains("summer") && (h.contains("peak") || h.contains("demand"))) {
      return "summer_peak_demand_mw";
    }
    if (h.contains("winter") && (h.contains("peak") || h.contains("demand"))) {
      return "winter_peak_demand_mw";
    }
    if (h.contains("net") && (h.contains("generation") || h.contains("mwh"))) {
      return "net_generation_mwh";
    }

    return null; // skip unmapped columns
  }

  private boolean isActivityCol(String h, String activity) {
    // Activity columns have the activity name but no numeric measure keywords
    return h.contains(activity) && !h.contains("customer") && !h.contains("revenue")
        && !h.contains("sales") && !h.contains("mwh") && !h.contains("gwh")
        && !h.contains("demand") && !h.contains("1000") && !h.contains("thousand");
  }

  private boolean isCustomers(String h) {
    return h.contains("customer") || h.contains("consumers") || h.contains("accounts")
        || h.contains("number of");
  }

  private boolean isSalesMwh(String h) {
    return (h.contains("sales") || h.contains("mwh") || h.contains("gwh")
        || h.contains("megawatt")) && !h.contains("revenue") && !h.contains("1000")
        && !h.contains("thousand") && !h.contains("$");
  }

  private boolean isRevenue(String h) {
    return h.contains("revenue") || h.contains("1000") || h.contains("thousand")
        || h.contains("$") || h.contains("dollar");
  }

  private boolean isTotal(String h) {
    return h.contains("total") && !h.contains("residential") && !h.contains("commercial")
        && !h.contains("industrial") && !h.contains("transportation");
  }

  private void putTypedValue(ObjectNode out, String col, String val) {
    if (val == null || val.isEmpty()) {
      out.putNull(col);
      return;
    }
    // Boolean activity columns: Y/Yes/1 → true, else false
    if (col.startsWith("activity_")) {
      out.put(col, "y".equalsIgnoreCase(val) || "yes".equalsIgnoreCase(val) || "1".equals(val)
          || "x".equalsIgnoreCase(val));
      return;
    }
    // Integer columns
    if (col.equals("utility_id") || col.equals("report_year") || col.startsWith("customers_")) {
      try {
        out.put(col, Integer.parseInt(val.replace(",", "")));
      } catch (NumberFormatException e) {
        out.putNull(col);
      }
      return;
    }
    // Double columns
    if (col.startsWith("sales_") || col.startsWith("revenue_")
        || col.endsWith("_mwh") || col.endsWith("_mw")) {
      try {
        out.put(col, Double.parseDouble(val.replace(",", "")));
      } catch (NumberFormatException e) {
        out.putNull(col);
      }
      return;
    }
    // String columns
    out.put(col, val);
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

    // Build effective cell value map accounting for merged regions.
    // POI only stores the value in the top-left cell of a merge; all other
    // cells in the merged range are blank. We propagate the top-left value
    // so composite header building sees the full label on every column.
    Map<Long, String> effectiveValues = new HashMap<>();
    for (int r = 0; r < numHeaderRows; r++) {
      Row hrow = sheet.getRow(r);
      if (hrow == null) {
        continue;
      }
      for (int c = 0; c < maxCols; c++) {
        String val = cellString(hrow.getCell(c));
        if (val != null && !val.trim().isEmpty()) {
          effectiveValues.put(cellKey(r, c), val.trim());
        }
      }
    }

    // Propagate merged cell values across the merge range.
    for (int i = 0; i < sheet.getNumMergedRegions(); i++) {
      CellRangeAddress region = sheet.getMergedRegion(i);
      if (region.getFirstRow() >= numHeaderRows) {
        continue;
      }
      long topLeftKey = cellKey(region.getFirstRow(), region.getFirstColumn());
      String topLeftVal = effectiveValues.get(topLeftKey);
      if (topLeftVal == null) {
        continue;
      }
      for (int r = region.getFirstRow(); r <= Math.min(region.getLastRow(), numHeaderRows - 1); r++) {
        for (int c = region.getFirstColumn(); c <= region.getLastColumn() && c < maxCols; c++) {
          long key = cellKey(r, c);
          if (!effectiveValues.containsKey(key)) {
            effectiveValues.put(key, topLeftVal);
          }
        }
      }
    }

    for (int c = 0; c < maxCols; c++) {
      StringBuilder sb = new StringBuilder();
      for (int r = 0; r < numHeaderRows; r++) {
        String part = effectiveValues.get(cellKey(r, c));
        if (part != null && !part.isEmpty()) {
          if (sb.length() > 0) {
            sb.append("_");
          }
          sb.append(part);
        }
      }
      combined.add(sb.toString());
    }
    return combined;
  }

  private long cellKey(int row, int col) {
    return ((long) row << 16) | (col & 0xFFFF);
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception {
    return parseSalesSheet(workbook, new HashMap<String, Map<String, String>>(),
        new HashMap<String, Map<String, Double>>(), 0);
  }
}
