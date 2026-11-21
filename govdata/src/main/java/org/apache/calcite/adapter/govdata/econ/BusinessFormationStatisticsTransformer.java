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
// storage-provider-guard:ignore-file - audited: only a local temp file holding the downloaded CSV.
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.CrossProcessRateLimiter;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the Census Bureau Business Formation Statistics monthly file
 * ({@code https://www.census.gov/econ/bfs/csv/bfs_monthly.csv}) and emits the
 * {@code econ.business_formation_statistics} rows for one calendar year.
 *
 * <p>The file is one wide table, one line per (adjustment, NAICS sector, series, geography,
 * year) with the twelve months as columns {@code jan..dec}. The whole history (2004 onward) is
 * in the one file, so it is downloaded once per JVM and the year dimension selects lines.
 * Each month cell becomes one long-format row. A blank cell (month not yet published, or a
 * series that starts later) and the Census suppression flags {@code S}, {@code D} and
 * {@code NA} yield no row, never a zero.
 */
public class BusinessFormationStatisticsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BusinessFormationStatisticsTransformer.class);

  private static final String RATE_LIMIT_KEY = "www.census.gov";
  private static final long MIN_REQUEST_INTERVAL_MS = 2_000L;

  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(30))
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build();

  private static final String[] HEADER =
      {"sa", "naics_sector", "series", "geo", "year", "jan", "feb", "mar", "apr", "may", "jun",
       "jul", "aug", "sep", "oct", "nov", "dec"};

  /** Census region codes used by the file, and the state / territory FIPS codes. */
  static final Map<String, String> REGIONS = new LinkedHashMap<String, String>();
  static final Map<String, String> FIPS = new HashMap<String, String>();

  static {
    REGIONS.put("NO", "northeast");
    REGIONS.put("MW", "midwest");
    REGIONS.put("SO", "south");
    REGIONS.put("WE", "west");
    String[] f = {"AL", "01", "AK", "02", "AZ", "04", "AR", "05", "CA", "06", "CO", "08",
        "CT", "09", "DE", "10", "DC", "11", "FL", "12", "GA", "13", "HI", "15", "ID", "16",
        "IL", "17", "IN", "18", "IA", "19", "KS", "20", "KY", "21", "LA", "22", "ME", "23",
        "MD", "24", "MA", "25", "MI", "26", "MN", "27", "MS", "28", "MO", "29", "MT", "30",
        "NE", "31", "NV", "32", "NH", "33", "NJ", "34", "NM", "35", "NY", "36", "NC", "37",
        "ND", "38", "OH", "39", "OK", "40", "OR", "41", "PA", "42", "RI", "44", "SC", "45",
        "SD", "46", "TN", "47", "TX", "48", "UT", "49", "VT", "50", "VA", "51", "WA", "53",
        "WV", "54", "WI", "55", "WY", "56", "PR", "72"};
    for (int i = 0; i < f.length; i += 2) {
      FIPS.put(f[i], f[i + 1]);
    }
  }

  /** The downloaded CSV, fetched once per JVM. */
  private static Path cachedFile;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearText = context.getDimensionValues().get("year");
    if (yearText == null) {
      throw new IllegalStateException(
          "business_formation_statistics: the year dimension is required to select rows");
    }
    String year = yearText.trim();
    Path file = ensureFile(context.getUrl(), context.getHeaders());
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line = reader.readLine();
      if (line == null || !line.trim().equals(String.join(",", HEADER))) {
        throw new IOException("business_formation_statistics: unexpected CSV header: " + line);
      }
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] c = line.split(",", -1);
        if (c.length != HEADER.length) {
          throw new IOException("business_formation_statistics: expected " + HEADER.length
              + " columns, got " + c.length + " in line: " + line);
        }
        if (c[4].equals(year)) {
          addMonths(c, Integer.parseInt(year), rows);
        }
      }
    }
    LOGGER.debug("business_formation_statistics {}: {} rows from {}", year, rows.size(), file);
    return rows.iterator();
  }

  static void addMonths(String[] c, int year, List<Map<String, Object>> rows) {
    String geo = c[3];
    String level;
    String fips;
    if (geo.equals("US")) {
      level = "national";
      fips = "00";
    } else if (REGIONS.containsKey(geo)) {
      level = "region";
      fips = null;
    } else {
      level = "state";
      fips = FIPS.get(geo);
      if (fips == null) {
        throw new IllegalStateException("business_formation_statistics: unknown geo code " + geo);
      }
    }
    for (int m = 1; m <= 12; m++) {
      String cell = c[4 + m].trim();
      if (cell.isEmpty() || cell.equals("S") || cell.equals("D") || cell.equals("NA")) {
        continue;
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("series_code", c[2]);
      row.put("seasonally_adjusted", c[0].equals("A"));
      row.put("naics_sector", c[1]);
      row.put("geo_level", level);
      row.put("geo_code", geo);
      row.put("fips", fips);
      row.put("year", year);
      row.put("month", m);
      row.put("period", String.format("%04d-%02d", year, m));
      row.put("value", Double.parseDouble(cell));
      rows.add(row);
    }
  }

  private static synchronized Path ensureFile(String url, Map<String, String> headers)
      throws IOException {
    if (cachedFile != null && Files.exists(cachedFile)) {
      return cachedFile;
    }
    CrossProcessRateLimiter.acquire(RATE_LIMIT_KEY, MIN_REQUEST_INTERVAL_MS);
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMinutes(5))
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
      throw new IOException("Interrupted fetching BFS monthly file: " + url, e);
    }
    try (InputStream body = response.body()) {
      if (response.statusCode() != 200) {
        throw new IOException("BFS monthly file download HTTP " + response.statusCode() + ": " + url);
      }
      Path target = org.apache.calcite.adapter.file.StagingDirectory
          .newTempFile("bfs-monthly-", ".csv").toPath();
      Files.copy(body, target, StandardCopyOption.REPLACE_EXISTING);
      cachedFile = target;
      return target;
    }
  }
}
