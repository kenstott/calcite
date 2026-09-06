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
package org.apache.calcite.adapter.govdata.transport;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Extends a PHMSA annual-mileage bulk CSV (2017-present, already extracted and
 * merged by {@code source.extractPattern} before any {@code responseTransformer}
 * runs) backward to 2010 by additionally parsing the same source ZIP's
 * whole-workbook XLSX annual reports for the 2010-2016 window, which PHMSA
 * publishes only in that per-year-workbook shape for those older years (the
 * 2017+ years are published as separate lettered-Part CSVs instead, which
 * {@code extractPattern} already selects and merges).
 *
 * <p>The {@code response} handed to {@link #transform} is the already-merged
 * 2017+ CSV text, not the ZIP itself — {@code extractPattern}'s multi-entry
 * glob-match-and-merge runs strictly before any {@code responseTransformer}
 * sees the data. Once a {@code responseTransformer} is configured,
 * {@code HttpSource.parseResponse} always treats its output as a JSON array
 * regardless of {@code source.response.format} (confirmed live: an all-string
 * CSV-shaped return threw {@code JsonParseException} on the literal header
 * token). So this class parses that CSV text itself, re-downloads the ZIP (the
 * same URL already on the table's {@code source.url}, matching the established
 * {@code EiaBulkXlsxTransformer} bypass pattern for POI-based sources), reads
 * the historical per-year workbooks, and emits ONE JSON array combining both
 * eras' rows — each object keyed by the exact column names from the 2017+
 * CSV header, so every existing column expression (`src."SOME_COLUMN"`)
 * continues to work unchanged.
 *
 * <p>A column present in the 2017+ header but absent from a given historical
 * year's workbook (e.g. gas distribution's COMMODITY field, only added
 * starting with the 2015 workbook) is emitted as a JSON null — honestly
 * reflecting that the source never captured it for that year, rather than
 * guessing a value.
 */
public abstract class PhmsaHistoricalMileageXlsxTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PhmsaHistoricalMileageXlsxTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Regex matching the per-year whole-workbook XLSX entry names in the ZIP; group 1 = year. */
  protected abstract Pattern xlsxEntryPattern();

  /** First historical year to append (inclusive) — normally 2010. */
  protected abstract int firstHistoricalYear();

  /** Last historical year to append (inclusive) — the last year PHMSA published only as XLSX. */
  protected abstract int lastHistoricalYear();

  /** Name of the sheet within a given year's workbook carrying the target fields. */
  protected abstract String sheetName(int year);

  /** 0-indexed row within the sheet holding the column headers. */
  protected abstract int headerRowIndex();

  /**
   * Source column name used to detect and drop the comma-only padding rows that
   * this ZIP's per-year files carry in every era (same defect class already
   * handled for 2017+ via {@code source.rowFilter}) — never blank for a real
   * report row.
   */
  protected String operatorIdColumn() {
    return "OPERATOR_ID";
  }

  /**
   * The raw source column names this table's schema columns actually reference
   * (every {@code src."SOME_COLUMN"} expression), always including
   * {@link #operatorIdColumn()}. Only these are carried into the emitted JSON —
   * not the full raw header, which for gas distribution's union-merged 2017+ CSV
   * era runs to 310 columns (this table's schema uses 12). Emitting all of them
   * produced an all-NULL table for every row (confirmed live); trimming to just
   * the columns this table declares avoids whatever width limit that hit and is
   * the objectively smaller, more robust payload regardless.
   */
  protected abstract String[] neededColumns();

  @Override public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    List<List<String>> csvRows = parseCsvText(response);
    if (csvRows.isEmpty()) {
      return "[]";
    }
    List<String> rawHeader = csvRows.get(0);
    List<String> needed = Arrays.asList(neededColumns());
    Map<String, Integer> rawIndex = new HashMap<>();
    for (int i = 0; i < rawHeader.size(); i++) {
      rawIndex.put(rawHeader.get(i), i);
    }
    Integer opIdIdx = rawIndex.get(operatorIdColumn());

    // The framework's own CSV-parsing path (which normally applies source.rowFilter
    // to drop this ZIP's comma-only padding rows) is entirely bypassed once a
    // responseTransformer is configured — HttpSource.parseResponse routes straight to
    // its JSON path instead. Re-apply the same OPERATOR_ID-non-empty filter here so
    // those padding rows (94-145 per year, confirmed live) don't reappear as all-NULL
    // rows now that this transformer owns the full parse.
    ArrayNode out = MAPPER.createArrayNode();
    for (int r = 1; r < csvRows.size(); r++) {
      List<String> fields = csvRows.get(r);
      if (fields.size() == 1 && fields.get(0).isEmpty()) {
        continue; // trailing blank line
      }
      if (opIdIdx != null) {
        String opId = opIdIdx < fields.size() ? fields.get(opIdIdx) : null;
        if (opId == null || opId.trim().isEmpty()) {
          continue; // comma-only padding row
        }
      }
      ObjectNode o = MAPPER.createObjectNode();
      for (String col : needed) {
        Integer idx = rawIndex.get(col);
        String val = idx != null && idx < fields.size() ? fields.get(idx) : null;
        if (val == null || val.isEmpty()) {
          o.putNull(col);
        } else {
          o.put(col, val);
        }
      }
      out.add(o);
    }
    int csvRowCount = out.size();

    try {
      byte[] zipBytes = downloadBytes(url);
      ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes));
      int appended = 0;
      try {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          Matcher m = xlsxEntryPattern().matcher(entry.getName());
          if (m.matches()) {
            int year = Integer.parseInt(m.group(1));
            if (year >= firstHistoricalYear() && year <= lastHistoricalYear()) {
              byte[] xlsxBytes = readAllBytes(zis);
              appended += appendYear(out, xlsxBytes, year, needed);
            }
          }
          zis.closeEntry();
        }
      } finally {
        zis.close();
      }
      LOGGER.info("{}: appended {} historical rows ({}-{}) to {} 2017+ rows from {}",
          getClass().getSimpleName(), appended, firstHistoricalYear(), lastHistoricalYear(),
          csvRowCount, url);
    } catch (Exception e) {
      throw new RuntimeException(getClass().getSimpleName()
          + ": failed to parse historical XLSX years from " + url, e);
    }
    return out.toString();
  }

  private int appendYear(ArrayNode out, byte[] xlsxBytes, int year, List<String> header)
      throws Exception {
    XSSFWorkbook wb = new XSSFWorkbook(new ByteArrayInputStream(xlsxBytes));
    int count = 0;
    try {
      Sheet sheet = wb.getSheet(sheetName(year));
      if (sheet == null) {
        LOGGER.warn("{}: sheet '{}' not found for year {} — skipping this year",
            getClass().getSimpleName(), sheetName(year), year);
        return 0;
      }
      Row headerRow = sheet.getRow(headerRowIndex());
      if (headerRow == null) {
        LOGGER.warn("{}: no header row at index {} for year {} — skipping this year",
            getClass().getSimpleName(), headerRowIndex(), year);
        return 0;
      }
      Map<String, Integer> colIndex = new HashMap<>();
      for (int c = 0; c < headerRow.getLastCellNum(); c++) {
        String h = cellString(headerRow.getCell(c));
        if (h != null && !h.trim().isEmpty()) {
          colIndex.put(h.trim(), c);
        }
      }
      Integer opCol = colIndex.get(operatorIdColumn());
      for (int r = headerRowIndex() + 1; r <= sheet.getLastRowNum(); r++) {
        Row row = sheet.getRow(r);
        if (row == null) {
          continue;
        }
        if (opCol != null) {
          String opId = cellString(row.getCell(opCol));
          if (opId == null || opId.trim().isEmpty()) {
            // Comma-only padding row — same defect class as the 2017+ CSV era.
            continue;
          }
        }
        ObjectNode o = MAPPER.createObjectNode();
        for (String h : header) {
          Integer srcCol = colIndex.get(h);
          String val = srcCol != null ? cellString(row.getCell(srcCol)) : null;
          if (val == null || val.isEmpty()) {
            o.putNull(h);
          } else {
            o.put(h, val);
          }
        }
        out.add(o);
        count++;
      }
    } finally {
      wb.close();
    }
    return count;
  }

  /**
   * Full RFC4180 parse of CSV text into rows of fields — handles quoted fields
   * containing commas, embedded newlines, and escaped ("") quotes.
   */
  private static List<List<String>> parseCsvText(String text) {
    List<List<String>> rows = new ArrayList<>();
    List<String> row = new ArrayList<>();
    StringBuilder cur = new StringBuilder();
    boolean inQuotes = false;
    int len = text.length();
    for (int i = 0; i < len; i++) {
      char c = text.charAt(i);
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < len && text.charAt(i + 1) == '"') {
            cur.append('"');
            i++;
          } else {
            inQuotes = false;
          }
        } else {
          cur.append(c);
        }
      } else if (c == '"') {
        inQuotes = true;
      } else if (c == ',') {
        row.add(cur.toString());
        cur.setLength(0);
      } else if (c == '\n' || c == '\r') {
        if (c == '\r' && i + 1 < len && text.charAt(i + 1) == '\n') {
          i++;
        }
        row.add(cur.toString());
        cur.setLength(0);
        rows.add(row);
        row = new ArrayList<>();
      } else {
        cur.append(c);
      }
    }
    if (cur.length() > 0 || !row.isEmpty()) {
      row.add(cur.toString());
      rows.add(row);
    }
    return rows;
  }

  private static String cellString(Cell cell) {
    if (cell == null) {
      return null;
    }
    CellType type = cell.getCellType();
    if (type == CellType.STRING) {
      String s = cell.getStringCellValue();
      return s == null || s.isEmpty() ? null : s;
    }
    if (type == CellType.NUMERIC) {
      if (DateUtil.isCellDateFormatted(cell)) {
        return cell.getLocalDateTimeCellValue().toString();
      }
      double d = cell.getNumericCellValue();
      if (d == Math.floor(d) && !Double.isInfinite(d)) {
        return String.valueOf((long) d);
      }
      return String.valueOf(d);
    }
    if (type == CellType.BOOLEAN) {
      return String.valueOf(cell.getBooleanCellValue());
    }
    if (type == CellType.FORMULA) {
      try {
        return cell.getStringCellValue();
      // fallback-guard: allow standard POI formula-cell fallback to the cached numeric value
      // when the cached string type doesn't match
      } catch (Exception e) {
        try {
          return String.valueOf(cell.getNumericCellValue());
        // fallback-guard: neither cached string nor numeric was readable for this one cell
        } catch (Exception ex) {
          return null;
        }
      }
    }
    return null;
  }

  private static byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(180000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    InputStream is = conn.getInputStream();
    try {
      return readAllBytes(is);
    } finally {
      is.close();
    }
  }

  private static byte[] readAllBytes(InputStream is) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int len;
    while ((len = is.read(buf)) > 0) {
      baos.write(buf, 0, len);
    }
    return baos.toByteArray();
  }
}
