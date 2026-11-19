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
package org.apache.calcite.adapter.govdata.patents;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * USPTO Patents Data Visualization Center monthly workbook ({@code dashboardspreadsheet.xlsx},
 * sheet "Historical Data"), unpivoted to one row per month + measure + technology center.
 *
 * <p>The configured source URL is the dashboard page; its HTML links the workbook, which is
 * downloaded here (the framework's text path would corrupt a binary file). The workbook is a
 * wide grid: column B onward is one month each (October 2008 onward, with blank columns for
 * the rest of the current fiscal year), and column A names the measure. A measure with no
 * value in a month (blank, or the source's "-" for "no longer tracked") produces no row.
 * Every measure label is mapped explicitly; an unmapped label that carries numbers fails the
 * run so a renamed or added measure is noticed rather than dropped.
 */
public class UsptoPatentOperationsTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UsptoPatentOperationsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String HOST = "https://www.uspto.gov";
  private static final Pattern WORKBOOK_LINK =
      Pattern.compile("href=\"(/dashboard/data/patents/dashboardspreadsheet\\.xlsx)\"");
  private static final String SHEET = "Historical Data";
  private static final int DATE_ROW = 3;
  private static final String ALL = "all";

  private static final String COUNT = "count";
  private static final String MONTHS = "months";
  private static final String RATIO = "ratio";
  private static final String PER_DISPOSAL = "actions_per_disposal";

  /** label -> {measure, unit}. */
  private static final Map<String, String[]> MEASURES = new HashMap<>();
  /** TC block header label -> measure. */
  private static final Map<String, String> TC_BLOCKS = new HashMap<>();
  /** Labels that carry no data (section titles). */
  private static final List<String> SECTION_LABELS = new ArrayList<>();

  static {
    m("Serialized Filings", "serialized_filings_fytd", COUNT);
    m("RCE Filings", "rce_filings_fytd", COUNT);
    m("Design Filings ~", "design_filings_fytd", COUNT);
    m("UPR Allowance rate *", "upr_allowance_rate_fytd", RATIO);
    m("Allowance rate without RCE abandonments *", "allowance_rate_ex_rce_abandonments_fytd", RATIO);
    m("Actions per disposal", "actions_per_disposal", PER_DISPOSAL);
    m("Number of Allowances *", "allowances_fytd", COUNT);
    m("Number of Final Rejections *", "final_rejections_fytd", COUNT);
    m("Number of Restrictions *", "restrictions_fytd", COUNT);
    m("UPR Production Units (cumulative)*", "production_units_fytd", COUNT);
    m("UPR First Action on Merits (cumulative)*", "first_actions_on_merits_fytd", COUNT);
    m("UPR Disposals (cumulative)*", "disposals_fytd", COUNT);
    m("UPR Unexamined Inventory", "upr_unexamined_inventory", COUNT);
    m("RCE Unexamined Inventory", "rce_unexamined_inventory", COUNT);
    m("Design Unexamined Inventory~", "design_unexamined_inventory", COUNT);
    m("Forward Looking First Action Pendency", "forward_looking_first_action_pendency", MONTHS);
    m("Traditional First Action Pendency (months)", "traditional_first_action_pendency", MONTHS);
    m("Traditional Total Pendency (months)", "traditional_total_pendency", MONTHS);
    m("Total Pendency Including RCEs", "total_pendency_including_rces", MONTHS);
    m("Pendency of RCEs (months)", "rce_pendency", MONTHS);
    m("Pendency of Continuations (months)", "continuation_pendency", MONTHS);
    m("Pendency of Divisional Applications (months)", "divisional_pendency", MONTHS);
    m("Pendency from RCE Filing to Next Office Action (months)",
        "rce_filing_to_next_action_pendency", MONTHS);
    m("UPR Examiner Staff", "examiner_staff", COUNT);
    m("Examiner Hires", "examiner_hires", COUNT);
    m("Examiner Attritions", "examiner_attritions", COUNT);
    m("Current Attrition rate**", "examiner_attrition_rate_12mo", RATIO);

    TC_BLOCKS.put("UPR Traditional First Action Pendency by Technology Center",
        "tc_traditional_first_action_pendency");
    TC_BLOCKS.put("UPR Traditional Total Pendency by Technology Center",
        "tc_traditional_total_pendency");
    TC_BLOCKS.put("UPR Traditional Total Pendency With RCEs by Technology Center",
        "tc_total_pendency_including_rces");
    TC_BLOCKS.put("UPR Examiner Staff by Technology Center", "tc_examiner_staff");

    SECTION_LABELS.add("Inputs");
    SECTION_LABELS.add("Level 2");
    SECTION_LABELS.add("Pendency");
    SECTION_LABELS.add("Staffing");
  }

  private static void m(String label, String measure, String unit) {
    MEASURES.put(label, new String[] {measure, unit});
  }

  @Override public String transform(String response, RequestContext context) {
    String pageUrl = context.getUrl();
    try {
      Matcher link = WORKBOOK_LINK.matcher(response);
      if (!link.find()) {
        throw new IOException("no dashboardspreadsheet.xlsx link on " + pageUrl);
      }
      String workbookUrl = HOST + link.group(1);
      LOGGER.info("USPTO patent dashboard workbook: {}", workbookUrl);
      XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(download(workbookUrl)));
      try {
        return parse(workbook).toString();
      } finally {
        workbook.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("USPTO patent dashboard: failed to read workbook via " + pageUrl, e);
    }
  }

  /** Unpivots the "Historical Data" sheet; package-visible for tests. */
  static ArrayNode parse(XSSFWorkbook workbook) {
    Sheet sheet = workbook.getSheet(SHEET);
    if (sheet == null) {
      throw new IllegalStateException("workbook has no sheet '" + SHEET + "'");
    }
    Row dateRow = sheet.getRow(DATE_ROW);
    if (dateRow == null) {
      throw new IllegalStateException(SHEET + ": no month header row at index " + DATE_ROW);
    }
    int lastCol = dateRow.getLastCellNum();
    List<LocalDate> months = new ArrayList<>();
    months.add(null); // column A is the label
    for (int c = 1; c < lastCol; c++) {
      Cell cell = dateRow.getCell(c);
      if (cell != null && cell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
        months.add(cell.getLocalDateTimeCellValue().toLocalDate());
      } else {
        months.add(null);
      }
    }

    ArrayNode out = MAPPER.createArrayNode();
    String block = null;
    for (int r = DATE_ROW + 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String label = label(row.getCell(0));
      if (label == null) {
        continue;
      }
      if (TC_BLOCKS.containsKey(label)) {
        block = TC_BLOCKS.get(label);
        continue;
      }
      boolean hasNumbers = hasNumbers(row, months.size());
      String measure;
      String unit;
      String tc = ALL;
      if (block != null && label.matches("\\d{4}")) {
        measure = block;
        unit = block.endsWith("staff") ? COUNT : MONTHS;
        tc = label;
      } else {
        block = null;
        String[] spec = MEASURES.get(label);
        if (spec == null) {
          if (hasNumbers && !SECTION_LABELS.contains(label)) {
            throw new IllegalStateException(SHEET + " row " + (r + 1) + ": unmapped measure '"
                + label + "'");
          }
          continue;
        }
        measure = spec[0];
        unit = spec[1];
      }
      for (int c = 1; c < months.size(); c++) {
        LocalDate month = months.get(c);
        Cell cell = row.getCell(c);
        if (month == null || cell == null || cell.getCellType() != CellType.NUMERIC) {
          continue;
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("month", month.toString());
        node.put("fiscal_year", month.getMonthValue() >= 10 ? month.getYear() + 1 : month.getYear());
        node.put("measure", measure);
        node.put("tech_center", tc);
        node.put("unit", unit);
        node.put("value", cell.getNumericCellValue());
        out.add(node);
      }
    }
    if (out.size() == 0) {
      throw new IllegalStateException(SHEET + ": no measure values parsed");
    }
    return out;
  }

  private static boolean hasNumbers(Row row, int cols) {
    for (int c = 1; c < cols; c++) {
      Cell cell = row.getCell(c);
      if (cell != null && cell.getCellType() == CellType.NUMERIC) {
        return true;
      }
    }
    return false;
  }

  private static String label(Cell cell) {
    if (cell == null) {
      return null;
    }
    String s;
    if (cell.getCellType() == CellType.STRING) {
      s = cell.getStringCellValue();
    } else if (cell.getCellType() == CellType.NUMERIC) {
      s = String.valueOf((long) cell.getNumericCellValue());
    } else {
      return null;
    }
    s = s.trim();
    return s.isEmpty() ? null : s;
  }

  private static byte[] download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    InputStream in = conn.getInputStream();
    try {
      byte[] buf = new byte[65536];
      int len;
      while ((len = in.read(buf)) > 0) {
        out.write(buf, 0, len);
      }
    } finally {
      in.close();
    }
    return out.toByteArray();
  }
}
