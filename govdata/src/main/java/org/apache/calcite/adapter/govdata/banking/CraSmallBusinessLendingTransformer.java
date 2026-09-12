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

import org.apache.calcite.adapter.file.etl.CrossProcessRateLimiter;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.SkippedBatchException;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
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

  // Confirmed against production's own pipeline_tracker: this host's Cloudflare gate 403s a
  // request some of the time (activity years 2012/2016/2017/2025 all recorded a genuine HTTP
  // 403 from this exact client/header combination), while an identical request for a
  // neighboring activity year succeeds - an intermittent gate, not a permanent block on this
  // client. Worth a few retries before giving up.
  private static final int MAX_FETCH_RETRIES = 4;
  private static final long RETRY_BACKOFF_MS = 5_000L;

  // Confirmed live (kenstott/govdata-ops#241): the gate is request-VOLUME-triggered, not
  // per-year or per-client-fingerprint - a burst of back-to-back requests to this host (even
  // across years that had each individually just succeeded moments earlier) trips it on every
  // subsequent request. FFIEC documents no rate limit anywhere checked (robots.txt, response
  // headers - no Retry-After/X-RateLimit-* on a 403) - this is Cloudflare's generic bot-
  // management challenge (`cf-mitigated: challenge`), whose own session cookie
  // (`__cf_bm`) carries Cloudflare's standard 30-minute default TTL, meaning a tripped
  // challenge state is not something a same-run retry can out-wait practically. Pacing
  // successive requests to this host - across years within one run, and across concurrent
  // worker threads on the same host - avoids tripping the volume heuristic in the first place,
  // which is more effective than reacting to it after the fact. Host-wide (not per-year) since
  // the trigger is aggregate volume, not any one URL.
  private static final String FFIEC_RATE_LIMIT_KEY = "ffiec.gov";
  private static final long FFIEC_MIN_REQUEST_INTERVAL_MS = 5_000L;

  // A CAPTCHA-classified failure (detected by isZipMagic below) gets a longer backoff than a
  // generic transient IOException: retrying within the existing few-second window is retrying
  // against a still-active challenge and provably cannot succeed, whereas a genuine one-off
  // transient error (a dropped connection, a mid-response timeout) has a real chance of
  // succeeding on the standard backoff. Still short of the __cf_bm TTL - a same-run retry is a
  // secondary safety net behind the pacing above, not the primary defense.
  private static final long CAPTCHA_RETRY_BACKOFF_MS = 30_000L;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String url = context.getUrl();
    final ZipInputStream zis = openZipWithRetry(url, context.getHeaders());
    final ZipEntry entry = findAggregateA11Entry(zis, url);
    if (entry == null) {
      zis.close();
      // Every configured activity year (1996+) publishes an A1-1 aggregate, so a ZIP that
      // downloaded successfully but doesn't contain the expected member is not "this year
      // has no data" (that's what the source's own 404 already communicates, upstream in
      // openZip) - it means this table's own name-matching assumption doesn't hold for this
      // file. Throwing surfaces it as an error the pipeline can retry/report on, rather than
      // silently completing with zero rows and no way to tell it apart from a real empty year.
      throw new IOException("CRA: no *_Aggr_A11.dat entry found in " + url);
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

  /** Retries {@link #openZip} on a transient failure. A 404 (source hasn't published this
   * year yet) is not transient and propagates on the first attempt - only an actual
   * connection/HTTP-status failure (this host's Cloudflare gate 403ing some requests and not
   * others for the same client/headers, confirmed against production's own tracker) is worth
   * retrying. */
  private static ZipInputStream openZipWithRetry(String url, Map<String, String> headers)
      throws IOException {
    IOException last = null;
    for (int attempt = 0; attempt <= MAX_FETCH_RETRIES; attempt++) {
      if (attempt > 0) {
        long backoff = backoffForRetry(last, attempt);
        LOGGER.warn("CRA: retrying {} (attempt {}/{}, backoff {}ms): {}",
            url, attempt + 1, MAX_FETCH_RETRIES + 1, backoff, last.getMessage());
        try {
          Thread.sleep(backoff);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while retrying CRA aggregate fetch", ie);
        }
      }
      try {
        return openZip(url, headers);
      } catch (SkippedBatchException e) {
        throw e;
      } catch (IOException e) {
        last = e;
      }
    }
    throw last;
  }

  /** Selects the backoff for the next attempt: a CAPTCHA-classified failure (see
   * {@link #CAPTCHA_RETRY_BACKOFF_MS}'s doc) always gets the longer, fixed backoff regardless
   * of attempt number, since retrying sooner cannot succeed against a still-active challenge;
   * any other transient {@link IOException} keeps the existing linear backoff. */
  static long backoffForRetry(IOException lastFailure, int attempt) {
    return lastFailure instanceof FfiecCaptchaException
        ? CAPTCHA_RETRY_BACKOFF_MS : RETRY_BACKOFF_MS * attempt;
  }

  /** Downloads the ZIP over HTTP/2 (see the class doc for why: this host's Cloudflare gate
   * fingerprints {@code HttpURLConnection}'s TLS/HTTP-1.1 client and rejects it outright). */
  private static ZipInputStream openZip(String url, Map<String, String> headers) throws IOException {
    CrossProcessRateLimiter.acquire(FFIEC_RATE_LIMIT_KEY, FFIEC_MIN_REQUEST_INTERVAL_MS);
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
    // FFIEC's Cloudflare gate can return 200 OK with a CAPTCHA HTML page instead of a zip
    // when the challenge fires - a status-code-only check would pass this through, and the
    // downstream ZipInputStream then finds no entries and surfaces as "no *_Aggr_A11.dat
    // entry found" outside the retry loop. Sniffing the standard zip magic (PK\x03\x04, and
    // the empty/spanned variants) here forces the throw back into the retry path so the
    // intermittent gate gets the same 4x/5s-backoff treatment as a real 403.
    BufferedInputStream sniffable = new BufferedInputStream(response.body());
    sniffable.mark(4);
    byte[] magic = new byte[4];
    int read = 0;
    while (read < 4) {
      int n = sniffable.read(magic, read, 4 - read);
      if (n < 0) {
        break;
      }
      read += n;
    }
    sniffable.reset();
    if (read < 4 || !isZipMagic(magic)) {
      String contentType = response.headers().firstValue("content-type").orElse("(none)");
      String snippet = readBodySnippet(sniffable, 512);
      throw new FfiecCaptchaException("CRA aggregate download returned non-zip body (HTTP " + code
          + ", Content-Type=" + contentType + ") for " + url
          + " - likely FFIEC CAPTCHA/challenge; snippet: " + snippet);
    }
    return new ZipInputStream(sniffable);
  }

  /** Marks a non-zip response body as CAPTCHA/challenge-shaped rather than a generic transient
   * failure, so {@link #openZipWithRetry} can apply {@link #CAPTCHA_RETRY_BACKOFF_MS} instead of
   * the shorter default backoff - see that constant's doc for why the two need to differ. */
  // Package-private for unit test access (backoffForRetry's differentiation logic).
  static final class FfiecCaptchaException extends IOException {
    FfiecCaptchaException(String message) {
      super(message);
    }
  }

  // Package-private for unit test access.
  static boolean isZipMagic(byte[] b) {
    if (b[0] != (byte) 0x50 || b[1] != (byte) 0x4B) {
      return false;
    }
    // PK\x03\x04 = local file header, PK\x05\x06 = end-of-central-directory (empty archive),
    // PK\x07\x08 = spanned archive marker. All are valid ZIP openings.
    return (b[2] == (byte) 0x03 && b[3] == (byte) 0x04)
        || (b[2] == (byte) 0x05 && b[3] == (byte) 0x06)
        || (b[2] == (byte) 0x07 && b[3] == (byte) 0x08);
  }

  private static String readBodySnippet(InputStream in, int maxBytes) {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] chunk = new byte[Math.min(512, maxBytes)];
    int total = 0;
    try {
      while (total < maxBytes) {
        int n = in.read(chunk, 0, Math.min(chunk.length, maxBytes - total));
        if (n < 0) {
          break;
        }
        buf.write(chunk, 0, n);
        total += n;
      }
    } catch (IOException ignored) {
      // best-effort — snippet is diagnostic only
    }
    return new String(buf.toByteArray(), java.nio.charset.StandardCharsets.ISO_8859_1)
        .replaceAll("\\s+", " ").trim();
  }
}
