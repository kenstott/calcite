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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.CrossProcessRateLimiter;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads one Philadelphia Fed Real-Time Data Set "first, second, third release" workbook
 * ({@code <variable>_first_second_third.xlsx}) and emits the {@code econ.gdp_release_estimates}
 * rows for that variable.
 *
 * <p>The {@code DATA} sheet has a header row {@code Date, First, Second, Third, Most_Recent}
 * followed by one row per quarter labelled {@code YYYY:Qn}; values are annualized
 * quarter-over-quarter growth rates. A release that does not exist is written {@code #N/A} and
 * becomes a null, never a zero; a quarter with no value in any column yields no row. The
 * {@code NOTES} sheet carries the workbook's "Last updated on" date.
 */
public class GdpReleaseEstimatesTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GdpReleaseEstimatesTransformer.class);

  private static final String RATE_LIMIT_KEY = "www.philadelphiafed.org";
  private static final long MIN_REQUEST_INTERVAL_MS = 2_000L;

  private static final Pattern QUARTER_LABEL = Pattern.compile("^(\\d{4}):Q([1-4])$");
  private static final Pattern UPDATED_LABEL =
      Pattern.compile("^Last updated on:\\s*([A-Za-z]+ \\d{1,2}, \\d{4})$");
  private static final DateTimeFormatter UPDATED_FORMAT =
      DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.US);
  private static final String[] HEADER = {"Date", "First", "Second", "Third", "Most_Recent"};
  private static final String[] VALUE_COLUMNS =
      {"first_release", "second_release", "third_release", "most_recent"};

  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(30))
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build();

  /** Parsed rows per workbook URL; the pipeline reads each unit twice (hash, then write). */
  private static final Map<String, List<Map<String, Object>>> PARSED =
      new HashMap<String, List<Map<String, Object>>>();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String variable = context.getDimensionValues().get("variable_id");
    if (variable == null) {
      throw new IllegalStateException(
          "gdp_release_estimates: the variable_id dimension is required");
    }
    return rowsFor(context.getUrl(), context.getHeaders(), variable).iterator();
  }

  private static synchronized List<Map<String, Object>> rowsFor(String url,
      Map<String, String> headers, String variable) throws IOException {
    List<Map<String, Object>> rows = PARSED.get(url);
    if (rows == null) {
      rows = download(url, headers, variable);
      PARSED.put(url, rows);
    }
    return rows;
  }

  private static List<Map<String, Object>> download(String url, Map<String, String> headers,
      String variable) throws IOException {
    CrossProcessRateLimiter.acquire(RATE_LIMIT_KEY, MIN_REQUEST_INTERVAL_MS);
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMinutes(2))
        .GET();
    if (headers != null) {
      for (Map.Entry<String, String> h : headers.entrySet()) {
        builder.header(h.getKey(), h.getValue());
      }
    }
    HttpResponse<InputStream> response;
    try {
      response = CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted fetching GDP release workbook: " + url, e);
    }
    try (InputStream body = response.body()) {
      if (response.statusCode() != 200) {
        throw new IOException("GDP release workbook HTTP " + response.statusCode() + ": " + url);
      }
      // The site answers an unknown file name with HTTP 200 and an HTML page.
      String contentType = response.headers().firstValue("content-type").orElse("");
      if (contentType.startsWith("text/html")) {
        throw new IOException("GDP release workbook URL returned an HTML page, not a workbook: "
            + url);
      }
      List<Map<String, Object>> rows = parse(body, variable);
      LOGGER.debug("gdp_release_estimates {}: {} rows from {}", variable, rows.size(), url);
      return rows;
    }
  }

  static List<Map<String, Object>> parse(InputStream in, String variable) throws IOException {
    try (XSSFWorkbook workbook = new XSSFWorkbook(in)) {
      String updated = lastUpdated(requireSheet(workbook, "NOTES"), variable);
      Sheet data = requireSheet(workbook, "DATA");
      String measure = text(data.getRow(0), 0);
      if (measure == null) {
        throw new IOException("gdp_release_estimates " + variable + ": DATA!A1 title is empty");
      }
      List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
      boolean headerSeen = false;
      for (Row row : data) {
        String first = text(row, 0);
        if (!headerSeen) {
          if (HEADER[0].equals(first)) {
            for (int i = 1; i < HEADER.length; i++) {
              if (!HEADER[i].equals(text(row, i))) {
                throw new IOException("gdp_release_estimates " + variable + ": header column "
                    + i + " is '" + text(row, i) + "', expected '" + HEADER[i] + "'");
              }
            }
            headerSeen = true;
          }
          continue;
        }
        if (first == null) {
          continue;
        }
        Matcher m = QUARTER_LABEL.matcher(first);
        if (!m.matches()) {
          throw new IOException("gdp_release_estimates " + variable
              + ": unrecognised quarter label '" + first + "'");
        }
        int year = Integer.parseInt(m.group(1));
        int quarter = Integer.parseInt(m.group(2));
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("measure", measure);
        out.put("date", LocalDate.of(year, quarter * 3 - 2, 1).toString());
        out.put("year", Integer.valueOf(year));
        out.put("quarter", Integer.valueOf(quarter));
        out.put("period", year + "Q" + quarter);
        boolean anyValue = false;
        for (int i = 0; i < VALUE_COLUMNS.length; i++) {
          Double value = number(row.getCell(i + 1), variable, first);
          anyValue |= value != null;
          out.put(VALUE_COLUMNS[i], value);
        }
        out.put("source_updated", updated);
        if (anyValue) {
          rows.add(out);
        }
      }
      if (rows.isEmpty()) {
        throw new IOException("gdp_release_estimates " + variable + ": no quarter rows found");
      }
      return rows;
    }
  }

  private static Sheet requireSheet(XSSFWorkbook workbook, String name) throws IOException {
    Sheet sheet = workbook.getSheet(name);
    if (sheet == null) {
      throw new IOException("gdp_release_estimates: workbook has no sheet '" + name + "'");
    }
    return sheet;
  }

  private static String lastUpdated(Sheet notes, String variable) throws IOException {
    for (Row row : notes) {
      String value = text(row, 0);
      if (value != null) {
        Matcher m = UPDATED_LABEL.matcher(value);
        if (m.matches()) {
          return LocalDate.parse(m.group(1), UPDATED_FORMAT).toString();
        }
      }
    }
    throw new IOException("gdp_release_estimates " + variable
        + ": NOTES sheet has no 'Last updated on' line");
  }

  /** Trimmed string content of a text cell, or null for a missing or blank cell. */
  private static String text(Row row, int column) {
    Cell cell = row == null ? null : row.getCell(column);
    if (cell == null || cell.getCellType() != CellType.STRING) {
      return null;
    }
    String s = cell.getStringCellValue().trim();
    return s.isEmpty() ? null : s;
  }

  /** A numeric cell's value, or null for a blank cell or {@code #N/A}. */
  private static Double number(Cell cell, String variable, String quarter) throws IOException {
    if (cell == null) {
      return null;
    }
    CellType type = cell.getCellType() == CellType.FORMULA
        ? cell.getCachedFormulaResultType() : cell.getCellType();
    if (type == CellType.NUMERIC) {
      return Double.valueOf(cell.getNumericCellValue());
    }
    if (type == CellType.BLANK || type == CellType.ERROR
        || type == CellType.STRING && "#N/A".equals(cell.getStringCellValue().trim())) {
      return null;
    }
    throw new IOException("gdp_release_estimates " + variable + ": non-numeric cell at "
        + quarter + ": " + cell);
  }
}
