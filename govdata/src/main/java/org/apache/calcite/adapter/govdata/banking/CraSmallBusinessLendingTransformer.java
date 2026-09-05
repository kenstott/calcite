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
package org.apache.calcite.adapter.govdata.banking;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.SkippedBatchException;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Streaming transformer for FFIEC's CRA Aggregate flat file, extracting only the
 * A1-1 member (Small Business Loans by County -- Originations).
 *
 * <p>FFIEC's flat-files host sits behind a Cloudflare managed challenge that
 * fingerprints the TLS/HTTP client, not just request headers: confirmed live that
 * {@code java.net.HttpURLConnection} (the client every other govdata table's
 * {@code source: {type: http}} fetch goes through) gets a 403 challenge page on
 * this host regardless of which browser-like headers are attached, while
 * {@code java.net.http.HttpClient} over HTTP/2 gets a clean 200 with the same
 * headers. This transformer exists specifically to route this one table's fetch
 * through the modern client; every other banking table can keep using the shared
 * {@code file/etl} HTTP path unchanged. {@code file/} itself stays Java 8-only —
 * only this govdata-side (Java 11+) class touches {@code java.net.http}.
 *
 * <p>Because a {@link StreamingResponseTransformer} owns its request end to end,
 * the source's declared {@code headers:} are read from {@link RequestContext} and
 * attached here rather than by {@code HttpSource} (matching the same "opens its
 * own connection" pattern documented on {@link NcuaBranchLocationsTransformer}).
 * The 145-char fixed-width A1-1 record layout is parsed directly, replicating
 * {@code banking/cra_aggregate_a11_layout.json}'s column positions rather than
 * routing through {@code file/etl}'s FIXED_WIDTH response path (unavailable once
 * the fetch itself is transformer-owned).
 */
public class CraSmallBusinessLendingTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CraSmallBusinessLendingTransformer.class);

  /** A1-1 record columns as (name, start, length), 0-based start, matching the FFIEC spec. */
  private static final Object[][] COLUMNS = {
      {"table_id", 0, 5},
      {"activity_year", 5, 4},
      {"loan_type", 9, 1},
      {"action_taken_type", 10, 1},
      {"state_fips", 11, 2},
      {"county_code", 13, 3},
      {"msa_md", 16, 5},
      {"census_tract", 21, 7},
      {"split_county", 28, 1},
      {"population_classification", 29, 1},
      {"income_group_total", 30, 3},
      {"report_level", 33, 3},
      {"loans_lt_100k_count", 36, 10},
      {"loans_lt_100k_amount", 46, 10},
      {"loans_100k_to_250k_count", 56, 10},
      {"loans_100k_to_250k_amount", 66, 10},
      {"loans_250k_to_1m_count", 76, 10},
      {"loans_250k_to_1m_amount", 86, 10},
      {"loans_to_small_biz_lt_1m_rev_count", 96, 10},
      {"loans_to_small_biz_lt_1m_rev_amount", 106, 10},
  };

  private static final java.util.Set<String> LONG_COLUMNS = new java.util.HashSet<String>(
      java.util.Arrays.asList(
          "loans_lt_100k_count", "loans_lt_100k_amount",
          "loans_100k_to_250k_count", "loans_100k_to_250k_amount",
          "loans_250k_to_1m_count", "loans_250k_to_1m_amount",
          "loans_to_small_biz_lt_1m_rev_count", "loans_to_small_biz_lt_1m_rev_amount"));

  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_2)
      .connectTimeout(Duration.ofSeconds(30))
      .build();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String url = context.getUrl();
    final ZipInputStream zis = openZip(url, context.getHeaders());
    final ZipEntry entry = findAggregateA11Entry(zis, url);
    if (entry == null) {
      zis.close();
      LOGGER.warn("CRA: no *_Aggr_A11.dat entry found in {}", url);
      return java.util.Collections.emptyIterator();
    }
    LOGGER.debug("CRA: streaming {} from {}", entry.getName(), url);

    final java.io.BufferedReader reader = new java.io.BufferedReader(
        new java.io.InputStreamReader(zis, java.nio.charset.StandardCharsets.ISO_8859_1));

    return new Iterator<Map<String, Object>>() {
      private final ArrayDeque<Map<String, Object>> pending = new ArrayDeque<Map<String, Object>>();
      private boolean closed;

      private void fill() {
        try {
          String line;
          while (pending.isEmpty() && (line = reader.readLine()) != null) {
            if (!line.isEmpty()) {
              pending.add(parseLine(line));
            }
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed streaming CRA A1-1 file: " + url, e);
        }
        if (pending.isEmpty() && !closed) {
          closed = true;
          try {
            reader.close();
          } catch (IOException ignored) {
            // best-effort
          }
        }
      }

      @Override public boolean hasNext() {
        fill();
        return !pending.isEmpty();
      }

      @Override public Map<String, Object> next() {
        fill();
        if (pending.isEmpty()) {
          throw new NoSuchElementException();
        }
        return pending.poll();
      }
    };
  }

  private static Map<String, Object> parseLine(String line) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    String stateFips = null;
    String countyCode = null;
    for (Object[] col : COLUMNS) {
      String name = (String) col[0];
      int start = (Integer) col[1];
      int length = (Integer) col[2];
      String raw = start >= line.length() ? ""
          : line.substring(start, Math.min(start + length, line.length())).trim();
      if ("table_id".equals(name) || "loan_type".equals(name) || "action_taken_type".equals(name)) {
        continue;
      }
      if ("state_fips".equals(name)) {
        stateFips = raw;
      } else if ("county_code".equals(name)) {
        countyCode = raw;
      }
      if (LONG_COLUMNS.contains(name)) {
        row.put(name, raw.isEmpty() ? null : Long.valueOf(raw));
      } else if ("activity_year".equals(name)) {
        row.put(name, raw.isEmpty() ? null : Integer.valueOf(raw));
      } else {
        row.put(name, raw.isEmpty() ? null : raw);
      }
    }
    row.put("county_fips",
        (stateFips == null || stateFips.isEmpty() || countyCode == null || countyCode.isEmpty())
            ? null : stateFips + countyCode);
    return row;
  }

  /** Finds the ZIP member whose name ends with {@code _Aggr_A11.dat} (case-sensitive, matching
   * FFIEC's own naming, e.g. {@code cra2024_Aggr_A11.dat}). */
  private static ZipEntry findAggregateA11Entry(ZipInputStream zis, String url) throws IOException {
    ZipEntry entry;
    while ((entry = zis.getNextEntry()) != null) {
      if (entry.getName().endsWith("_Aggr_A11.dat")) {
        return entry;
      }
    }
    return null;
  }

  /** Downloads the ZIP over HTTP/2 (see the class doc for why: this host's Cloudflare gate
   * fingerprints {@code HttpURLConnection}'s TLS/HTTP-1.1 client and rejects it outright). */
  private static ZipInputStream openZip(String url, Map<String, String> headers) throws IOException {
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
      throw new IOException("Interrupted fetching CRA aggregate file: " + url, e);
    }
    int code = response.statusCode();
    if (code == 404) {
      throw new SkippedBatchException("CRA aggregate file not yet published (HTTP 404): " + url);
    }
    if (code < 200 || code >= 300) {
      throw new IOException("CRA aggregate download HTTP " + code + ": " + url);
    }
    return new ZipInputStream(response.body());
  }
}
