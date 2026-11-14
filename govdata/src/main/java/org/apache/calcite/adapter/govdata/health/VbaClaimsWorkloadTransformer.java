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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the VBA Monday Morning Workload Report (MMWR) weekly workbooks for one fiscal year
 * into one row per (week_ending, line) from each workbook's "Transformation" sheet — the
 * national Compensation and Pension rating-bundle pending inventory and the portion pending
 * more than 125 days (the VA "backlog").
 *
 * <p>VBA publishes one workbook per week, named irregularly
 * ({@code MMWR-05-18-2024.xlsm}, {@code MMWR_05_07_2022.xlsm}) and filed under a folder named
 * for the fiscal year (the {@code 2026} folder holds October 2025 onward). The HTTP body handed
 * in is the report index page, whose links enumerate the year's files; each is downloaded as
 * raw bytes and read with POI. The week is taken from the sheet's own "Reporting through"
 * line, never from the file name.
 *
 * <p>Column positions differ between years (the sheet is shifted two columns in FY2026), so
 * columns are located from the "# Pending" header cell and the layout is verified; a workbook
 * that does not match fails the run rather than yielding partial rows.
 */
public class VbaClaimsWorkloadTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(VbaClaimsWorkloadTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String SITE = "https://www.benefits.va.gov";
  private static final String SHEET = "Transformation";
  private static final Pattern REPORTING_THROUGH =
      Pattern.compile("^Reporting through\\s+(.+)$", Pattern.CASE_INSENSITIVE);
  private static final DateTimeFormatter WEEK_FORMAT =
      DateTimeFormatter.ofPattern("MMMM d, uuuu", Locale.US);
  private static final Pattern EP_KEY = Pattern.compile("^EP\\s+(\\w+)$");
  /** Offset from the "# Pending" column back to the line-label column. */
  private static final int LABEL_OFFSET = 6;
  private static final int MAX_HEADER_SCAN_ROWS = 10;

  @Override public String transform(String response, RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String year = dims == null ? null : dims.get("year");
    if (year == null || year.isEmpty()) {
      throw new IllegalStateException("va_claims_workload: year dimension missing");
    }
    Set<String> files = workbookPaths(response, year);
    if (files.isEmpty()) {
      throw new IllegalStateException("va_claims_workload: no MMWR workbook links for FY" + year
          + " on the VBA report index");
    }
    ArrayNode out = MAPPER.createArrayNode();
    Map<String, String> weekContent = new HashMap<>();
    Map<String, String> weekSource = new HashMap<>();
    for (String path : files) {
      String url = SITE + path;
      ArrayNode rows = MAPPER.createArrayNode();
      try (InputStream in = open(url); Workbook wb = WorkbookFactory.create(in)) {
        Sheet sheet = wb.getSheet(SHEET);
        if (sheet == null) {
          throw new IllegalStateException("va_claims_workload: no '" + SHEET + "' sheet in " + url);
        }
        parseSheet(sheet, url, rows);
      } catch (IOException e) {
        throw new RuntimeException("va_claims_workload: cannot read " + url, e);
      }
      // VBA sometimes posts two workbooks reporting through the same week. An exact repeat is
      // kept once; a repeat with different numbers has no defensible winner, so it fails.
      String week = rows.get(0).get("week_ending").asText();
      String content = rows.toString();
      String prior = weekContent.get(week);
      if (prior == null) {
        weekContent.put(week, content);
        weekSource.put(week, url);
        out.addAll(rows);
      } else if (!prior.equals(content)) {
        throw new IllegalStateException("va_claims_workload: " + url + " and "
            + weekSource.get(week) + " both report week " + week + " with different values");
      }
    }
    LOGGER.info("va_claims_workload: FY{} {} workbooks, {} rows", year, files.size(), out.size());
    return out.toString();
  }

  /** The year's workbook paths in the index HTML, de-duplicated (the page links each twice). */
  static Set<String> workbookPaths(String html, String year) {
    Pattern p = Pattern.compile("href=\"(/REPORTS/mmwr/" + Pattern.quote(year)
        + "/[^\"#]+\\.xls[xm])", Pattern.CASE_INSENSITIVE);
    Set<String> paths = new TreeSet<>();
    Matcher m = p.matcher(html);
    while (m.find()) {
      paths.add(m.group(1));
    }
    return paths;
  }

  static void parseSheet(Sheet sheet, String where, ArrayNode out) {
    DataFormatter fmt = new DataFormatter();
    LocalDate week = null;
    int pendingCol = -1;
    int headerRow = -1;
    for (int r = 0; r <= Math.min(sheet.getLastRowNum(), MAX_HEADER_SCAN_ROWS); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      for (Cell c : row) {
        String text = text(fmt, c).trim();
        Matcher m = REPORTING_THROUGH.matcher(text);
        if (m.matches()) {
          week = LocalDate.parse(m.group(1).trim(), WEEK_FORMAT);
        } else if (text.equals("# Pending") && pendingCol < 0) {
          pendingCol = c.getColumnIndex();
          headerRow = r;
        }
      }
    }
    if (week == null || pendingCol < 0) {
      throw new IllegalStateException("va_claims_workload: 'Reporting through' line or '# Pending'"
          + " header not found in " + where + " — workbook layout changed");
    }
    verifyHeader(sheet.getRow(headerRow), pendingCol, fmt, where);

    int labelCol = pendingCol - LABEL_OFFSET;
    int rows = 0;
    for (int r = headerRow + 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String label = text(fmt, row.getCell(labelCol)).trim();
      if (label.toLowerCase(Locale.ROOT).startsWith("education")) {
        break;
      }
      String key = text(fmt, row.getCell(pendingCol - 1)).trim();
      boolean total = key.equals("Total");
      Matcher ep = EP_KEY.matcher(key);
      if (!total && !ep.matches()) {
        continue;
      }
      Long pending = number(row.getCell(pendingCol));
      Long over125 = number(row.getCell(pendingCol + 1));
      if (pending == null || over125 == null) {
        throw new IllegalStateException("va_claims_workload: non-numeric counts on row "
            + (r + 1) + " ('" + label + "') in " + where);
      }
      ObjectNode node = MAPPER.createObjectNode();
      node.put("week_ending", week.toString());
      node.put("line_type", total ? "total" : "ep");
      if (total) {
        node.putNull("ep_code");
      } else {
        node.put("ep_code", ep.group(1));
      }
      node.put("line_description", label);
      node.put("pending_claims", pending);
      node.put("pending_over_125_days", over125);
      out.add(node);
      rows++;
    }
    if (rows == 0) {
      throw new IllegalStateException("va_claims_workload: no rows parsed from " + where);
    }
  }

  private static void verifyHeader(Row header, int pendingCol, DataFormatter fmt, String where) {
    String second = text(fmt, header.getCell(pendingCol + 1)).trim();
    if (!second.equals("# Pending > 125")) {
      throw new IllegalStateException("va_claims_workload: column after '# Pending' is '" + second
          + "', expected '# Pending > 125' in " + where + " — workbook layout changed");
    }
  }

  /**
   * Cell text; a formula cell yields its cached result, not the formula (DataFormatter without
   * an evaluator returns the formula string, which is what the VBA workbooks' date and label
   * cells are).
   */
  private static String text(DataFormatter fmt, Cell cell) {
    if (cell == null) {
      return "";
    }
    if (cell.getCellType() != CellType.FORMULA) {
      return fmt.formatCellValue(cell);
    }
    switch (cell.getCachedFormulaResultType()) {
    case STRING:
      return cell.getStringCellValue();
    case NUMERIC:
      return fmt.formatRawCellContents(cell.getNumericCellValue(),
          cell.getCellStyle().getDataFormat(), cell.getCellStyle().getDataFormatString());
    default:
      return "";
    }
  }

  /** Numeric value of a cell, using the cached result for formula cells; null when not numeric. */
  private static Long number(Cell cell) {
    if (cell == null) {
      return null;
    }
    CellType type = cell.getCellType() == CellType.FORMULA
        ? cell.getCachedFormulaResultType() : cell.getCellType();
    return type == CellType.NUMERIC ? Long.valueOf((long) cell.getNumericCellValue()) : null;
  }

  private static InputStream open(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    return conn.getInputStream();
  }
}
