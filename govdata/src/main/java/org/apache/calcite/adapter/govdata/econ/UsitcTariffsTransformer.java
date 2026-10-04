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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Streaming transformer for {@code econ.usitc_tariffs} (USITC DataWeb imports-for-consumption
 * with the duty breakout).
 *
 * <p>DataWeb {@code report2/runReport} is a POST whose body is a nested {@code SavedQuery} object
 * and whose result is an <em>asynchronous, per-measure grid</em> — verified live 2026-07-16 against
 * a real {@code TRADE_USITC_API_TOKEN}. Two hard properties of the API shape this design:
 * <ul>
 *   <li>A single {@code runReport} is capped at <b>20,000 result rows</b> (the API rejects more at
 *       "step 8"), and a broad all-commodities year query is also too slow to compute (the async
 *       job never finishes within a reasonable poll budget).</li>
 *   <li>Most HTS-2 chapters compute quickly and return well under the cap; the largest ones
 *       (organic chemicals, plastics, apparel, steel articles, machinery, vehicles) do neither.</li>
 * </ul>
 * So this transformer <b>chunks by HTS chapter</b>: for one data year it issues one query per
 * chapter (01..99), each requesting imports for consumption at HTS-8 broken out by country with
 * measures {@code CONS_CUSTOMS_VALUE} + {@code CONS_CALC_DUTY} (the two {@code dataToReport} codes
 * confirmed valid), then merges all chapters into the year's rows. Requests are paced to respect
 * DataWeb's burst rate limit.
 *
 * <p>A chapter that still will not compute, or that comes back truncated at the row cap, is
 * bisected into HTS-4 heading ranges and refetched — see {@link #fetchSlice}. The split is
 * reactive, so a normal chapter costs exactly one request and only the heavy ones pay for depth.
 *
 * <p>Per response: one {@code table} per measure ({@code tableInfo.dataToReportDesc}); each row is a
 * <em>positional</em> {@code rowEntries} array whose columns are identified by {@code columnInfo}
 * ({@code type} = {@code hts}/{@code country}/{@code data}) and whose values are comma-formatted
 * strings. Country is the DataWeb country <em>name</em> (the grid reports names, not codes).
 *
 * <p>Fail-loud (rule #6): a validation error ({@code dto.errors}) or a non-2xx status throws, and
 * so does a year in which ANY chapter could not be retrieved. The chapters partition the HTS
 * schedule, so a missing one is an absent slice of the year rather than a smaller sample of it, and
 * committing it would publish a partition that reads as a complete year while every tariff line
 * under that chapter is gone. A year that yields zero rows likewise throws rather than committing
 * an empty partition.
 * {@code hts_description}, dutiable value, first-unit quantity, and program breakout are additional
 * DataWeb measures not yet wired (their codes need a DataWeb-UI network capture).
 */
public class UsitcTariffsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsitcTariffsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 900_000;
  /** Retry budget for transient hard failures (I/O, 5xx). 429s do NOT draw from this. */
  private static final int HTTP_RETRIES = 4;
  /**
   * Separate retry budget for HTTP 429 (rate limit). Kept apart from {@link #HTTP_RETRIES} so a
   * burst of rate-limiting can't exhaust the hard-error budget and silently drop a chapter.
   */
  private static final int RATE_LIMIT_RETRIES = 6;
  /** Cap on any single 429 wait (also caps a server-supplied {@code Retry-After}). */
  private static final long RATE_LIMIT_MAX_WAIT_MS = 60_000L;
  /** First delay between polls of a job DataWeb still reports unfinished. */
  private static final long POLL_DELAY_MS = 5_000L;
  /**
   * Ceiling on the poll delay, which grows 1.5x from {@link #POLL_DELAY_MS}. A poll is not a cheap
   * status check — {@link #postAndPoll} re-POSTs the whole query every time, so backing off cuts
   * origin load, and the 429s, as much as it cuts the wait.
   */
  private static final long POLL_DELAY_MAX_MS = 30_000L;
  /**
   * Wall-clock budget for one query while DataWeb keeps reporting it unfinished. Reaching it means
   * the slice asks for more than the API will compute, which is recoverable by asking for less, so
   * it is the signal to split in {@link #fetchSlice} rather than a reason to drop the chapter.
   *
   * <p>This is dead time — it buys nothing but the news that a split is needed — so it sits just
   * past the point where a query that is going to answer has answered. Measured live on chapter 29:
   * the slices that did return came back in seconds once the range was narrow enough, while the two
   * that did not were still unfinished at 300s. Splitting a slice that would in fact have completed
   * costs one extra pair of requests and still yields correct rows, so short is the cheap side of
   * this trade.
   */
  private static final long QUERY_DEADLINE_MS = 120_000L;
  /** Pace between chapter requests to stay under DataWeb's burst rate limit. */
  private static final long CHAPTER_PACE_MS = 1_000L;
  /** DataWeb hard cap on a single runReport result (validated live: rejects &gt;20,000 at step 8). */
  private static final int GRID_ROW_CAP = 20_000;
  /**
   * Separator joining the two halves of a merge key. NUL occurs in neither an HTS code nor a
   * DataWeb country name, so the join cannot collide.
   *
   * <p>Spelled as the octal escape rather than as a literal NUL byte or a backslash-u escape. A
   * literal byte makes this file binary to git and to grep, which hides every diff of it; and a
   * backslash-u escape is expanded before the source is lexed, which would put that same literal
   * byte straight back. The octal form is an ordinary string escape and leaves the file text.
   */
  private static final String KEY_SEP = "\0";
  /** HTS-4 headings under one chapter: {@code NN00}..{@code NN99}. */
  private static final int HEADINGS_PER_CHAPTER = UsitcSlicePlan.HEADINGS_PER_CHAPTER;
  /**
   * Host-local record of where earlier runs had to split a chapter, so this run starts from slices
   * DataWeb is known to answer rather than paying {@link #QUERY_DEADLINE_MS} to rediscover each
   * split. Beside {@link #API_LOCK_FILE}, which is host-wide state for the same reason.
   */
  private static final File SLICE_PLAN_FILE =
      new File(System.getProperty("java.io.tmpdir"), "usitc-slice-plan.json");
  /**
   * Bisection depth cap for {@link #fetchSlice}. Halving 100 headings reaches a single one in 7
   * steps; 8 leaves a step of headroom while still bounding a pathological recursion.
   */
  private static final int MAX_SPLIT_DEPTH = 8;

  /**
   * Cross-process serialization of DataWeb calls. Concurrent econ workers (e.g. worker-econ-2022 and
   * worker-econ-2023) are separate JVMs sharing one {@code TRADE_USITC_API_TOKEN}; an in-JVM lock
   * can't coordinate them, so every API call takes an OS advisory lock on this shared file. Only one
   * DataWeb request runs at a time across all workers on the host, which keeps bursts under the rate
   * limit that was producing 429s and poll timeouts.
   */
  private static final File API_LOCK_FILE =
      new File(System.getProperty("java.io.tmpdir"), "usitc-dataweb-api.lock");
  /** In-JVM guard so two threads in one worker can't overlap-lock the same channel. */
  private static final Object API_MONITOR = new Object();
  /** Minimum global spacing between consecutive DataWeb calls (held while the file lock is owned). */
  private static final long API_MIN_GAP_MS = 1_000L;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("UsitcTariffsTransformer: no URL in context");
    }
    String year = context.getDimensionValues().get("effective_year");
    if (year == null || year.isEmpty()) {
      year = context.getDimensionValues().get("year");
    }
    if (year == null || year.isEmpty()) {
      throw new IllegalStateException("UsitcTariffsTransformer: no year in context for " + url);
    }
    String token = context.getHeaders().get("Authorization");
    if (token == null || token.trim().isEmpty() || token.trim().equalsIgnoreCase("Bearer")) {
      throw new IllegalStateException("UsitcTariffsTransformer: missing Authorization bearer token "
          + "(set TRADE_USITC_API_TOKEN) for " + url);
    }

    // key "hts8\0country" -> [hts8, country, customsValue, calcDuty]
    Map<String, Object[]> merged = new LinkedHashMap<String, Object[]>();
    int chaptersOk = 0;
    List<String> failedChapters = new ArrayList<String>();
    List<String> chapters = chapters();
    File planFile = slicePlanFile();
    UsitcSlicePlan planned = UsitcSlicePlan.load(planFile);
    UsitcSlicePlan learned = UsitcSlicePlan.empty();
    LOGGER.info("usitc_tariffs[{}]: chunking {} HTS chapters ({} start pre-split from {})",
        year, chapters.size(), planned.splitChapters(), planFile);
    for (String chapter : chapters) {
      try {
        fetchChapter(url, context.getHeaders(), year, chapter, planned, learned, merged);
        chaptersOk++;
      } catch (IOException e) {
        // Collected, not rethrown here: one pass over every chapter names all the gaps in a single
        // run, which is what makes the failure below actionable. The batch still fails.
        failedChapters.add(chapter);
        LOGGER.warn("usitc_tariffs[{}]: chapter {} failed: {}", year, chapter, e.getMessage());
      }
      sleep(CHAPTER_PACE_MS);
    }

    // Saved before the failure checks below: a cut records a slice that timed out, which stays true
    // whether or not the rest of the year succeeded, and a retry should not pay for it again.
    if (learned.hasCutsNotIn(planned)) {
      UsitcSlicePlan saved = UsitcSlicePlan.saveMerged(planFile, learned);
      LOGGER.info("usitc_tariffs[{}]: slice plan now pre-splits {} chapters ({})",
          year, saved.splitChapters(), planFile);
    }

    List<Map<String, Object>> rows = buildRows(merged);
    LOGGER.info("usitc_tariffs[{}]: {} rows from {} chapters ({} failed)",
        year, rows.size(), chaptersOk, failedChapters.size());
    // Any missing chapter fails the batch. The chapters partition the HTS schedule, so a missing
    // one is an absent slice of the year rather than a smaller sample of it: every tariff line
    // under it disappears while the partition still reads as a complete year, and the pipeline
    // marks the period done so nothing re-fetches it. Failing here leaves the last good snapshot
    // in place and re-runs the whole year next pass, which is the only way the gap closes.
    if (!failedChapters.isEmpty()) {
      throw new IOException("usitc_tariffs[" + year + "]: " + failedChapters.size() + " of "
          + chapters.size() + " HTS chapters failed " + failedChapters
          + " — not committing a partial year");
    }
    if (rows.isEmpty()) {
      throw new IOException("usitc_tariffs[" + year + "]: no rows from any HTS chapter "
          + "— not committing an empty partition");
    }
    return rows.iterator();
  }

  /** HTS-2 chapters 01..99, excluding 77 (reserved — no commodities, would error). */
  private static List<String> chapters() {
    List<String> out = new ArrayList<String>(99);
    for (int i = 1; i <= 99; i++) {
      if (i == 77) {
        continue;
      }
      out.add(String.format("%02d", i));
    }
    return out;
  }

  /**
   * The nested DataWeb {@code SavedQuery} body for one data year and one commodity slice — imports
   * for consumption, HTS-8 broken out by country, programs aggregated, zero rows suppressed, customs
   * value + calculated duties. Kept as an explicit template so it reads as the exact verified spec;
   * {@code __YEAR__} and {@code __CODES__} are the only substitutions.
   *
   * <p>{@code codes} is the already-quoted, comma-separated body of the {@code commodities} array:
   * a lone chapter ({@code "84"}) or a run of HTS-4 headings ({@code "8400","8401",...}). The API
   * accepts either at {@code granularity 8}, and ignores codes that do not exist — verified live,
   * which is what lets {@link #fetchSlice} bisect a heading range without a catalog of valid codes.
   */
  private static String buildQueryBody(String year, String codes) {
    String tpl = "{"
        + "\"reportOptions\":{\"tradeType\":\"Import\",\"classificationSystem\":\"HTS\"},"
        + "\"searchOptions\":{"
        + "\"componentSettings\":{"
        + "\"dataToReport\":[\"CONS_CUSTOMS_VALUE\",\"CONS_CALC_DUTY\"],"
        + "\"scale\":\"1\",\"timeframeSelectType\":\"fullYears\",\"years\":[\"__YEAR__\"],"
        + "\"startDate\":null,\"endDate\":null,\"startMonth\":null,\"endMonth\":null,"
        + "\"yearsTimeline\":\"Annual\"},"
        + "\"commodities\":{\"commodities\":[__CODES__],\"commoditiesExpanded\":[],"
        + "\"commoditiesManual\":null,\"commodityGroups\":{\"systemGroups\":[],\"userGroups\":[]},"
        + "\"granularity\":\"8\",\"searchGranularity\":\"8\",\"groupGranularity\":\"8\","
        + "\"aggregation\":\"Break Out Commodities\",\"codeDisplayFormat\":\"NO\","
        + "\"commoditySelectType\":\"list\",\"showHTSValidDetails\":true},"
        + "\"countries\":{\"countries\":[],\"countriesExpanded\":[],"
        + "\"countryGroups\":{\"systemGroups\":[],\"userGroups\":[]},"
        + "\"aggregation\":\"Break Out Countries\",\"countriesSelectType\":\"all\"},"
        + "\"MiscGroup\":{"
        + "\"importPrograms\":{\"importPrograms\":[],\"aggregation\":\"Aggregate CSC\"},"
        + "\"extImportPrograms\":{\"programsSelectType\":\"all\",\"extImportPrograms\":[],"
        + "\"extImportProgramsExpanded\":[],\"aggregation\":\"Aggregate CSC\"},"
        + "\"provisionCodes\":{\"rateProvisionCodes\":[],\"rateProvisionCodesExpanded\":[],"
        + "\"aggregation\":\"Aggregate RPCODE\",\"provisionCodesSelectType\":\"all\","
        + "\"rateProvisionGroups\":{\"systemGroups\":[]}},"
        + "\"districts\":{\"districts\":[],\"districtsExpanded\":[],"
        + "\"districtGroups\":{\"userGroups\":[]},\"aggregation\":\"Aggregate District\","
        + "\"districtsSelectType\":\"all\"}}},"
        + "\"sortingAndDataFormat\":{"
        + "\"DataSort\":{\"sortOrder\":[{\"sortData\":\"COUNTRY\",\"orderBy\":\"asc\","
        + "\"year\":\"__YEAR__\"}],\"columnOrder\":[\"null\"],\"sortYear\":null},"
        + "\"reportCustomizations\":{\"totalRecords\":\"20000\",\"exportCombineTables\":false,"
        + "\"reportsGrid\":true,\"removeDuplicateValues\":true,\"suppressZeroValues\":true,"
        + "\"displayCommodityList\":false,\"reportsFontSize\":\"m\",\"exportRawData\":false}}"
        + "}";
    return tpl.replace("__YEAR__", year).replace("__CODES__", codes);
  }

  /**
   * Fetches one commodity slice into {@code merged}, splitting it when DataWeb cannot deliver it
   * whole. Two outcomes mean the slice asks for more than the API will compute in one query, and
   * both are recoverable by asking for less:
   *
   * <ul>
   *   <li>the query outruns {@link #QUERY_DEADLINE_MS} still reporting itself unfinished;</li>
   *   <li>it returns exactly {@link #GRID_ROW_CAP} rows, which is DataWeb truncating the grid
   *       rather than reporting an error — the rows are real but the slice is a top slice.</li>
   * </ul>
   *
   * <p>Both split the same way: bisect the HTS-4 heading range under the chapter and fetch the
   * halves. Only a branch that actually fails splits, so the ~88 chapters that answer in seconds
   * still cost one request each and the heavy ones pay for depth only where they need it. Merging
   * is idempotent — the halves cover a superset of a truncated parent, keyed identically — so rows
   * already accumulated from a capped attempt are simply overwritten with themselves.
   *
   * @param lo    first HTS-4 heading offset in this slice, 0..99
   * @param hi    last HTS-4 heading offset in this slice, inclusive
   * @param depth 0 asks for the bare chapter code; deeper levels enumerate {@code [lo, hi]}
   * @return grid rows returned by this slice and its children; the merged row count is
   *         {@code merged.size()}, since slices share (hts8, country) keys
   */
  int fetchSlice(String url, Map<String, String> headers, String year, String chapter,
      int lo, int hi, int depth, UsitcSlicePlan learned, Map<String, Object[]> merged)
      throws IOException {
    String label;
    String codes;
    if (depth == 0) {
      label = "chapter " + chapter;
      codes = "\"" + chapter + "\"";
    } else {
      label = "chapter " + chapter + " headings " + heading(chapter, lo)
          + "-" + heading(chapter, hi);
      StringBuilder sb = new StringBuilder();
      for (int i = lo; i <= hi; i++) {
        if (sb.length() > 0) {
          sb.append(',');
        }
        sb.append('"').append(heading(chapter, i)).append('"');
      }
      codes = sb.toString();
    }

    String reason;
    try {
      int widestTable = fetchOnce(url, headers, year, label, codes, merged);
      if (widestTable < GRID_ROW_CAP) {
        LOGGER.debug("usitc_tariffs[{}]: {} -> {} grid rows ({} merged)",
            year, label, widestTable, merged.size());
        return widestTable;
      }
      reason = "returned the " + GRID_ROW_CAP + "-row grid cap, so it is a top slice";
    } catch (QueryTooSlowException e) {
      reason = e.getMessage();
    }

    if (depth >= MAX_SPLIT_DEPTH || lo >= hi) {
      throw new IOException(label + " " + reason + ", and is already the narrowest slice this "
          + "transformer can request — it cannot be split further");
    }
    int mid = lo + (hi - lo) / 2;
    LOGGER.warn("usitc_tariffs[{}]: {} {} — splitting into {}-{} and {}-{}",
        year, label, reason, heading(chapter, lo), heading(chapter, mid),
        heading(chapter, mid + 1), heading(chapter, hi));
    learned.addCut(chapter, mid + 1);
    int rows = fetchSlice(url, headers, year, chapter, lo, mid, depth + 1, learned, merged);
    rows += fetchSlice(url, headers, year, chapter, mid + 1, hi, depth + 1, learned, merged);
    return rows;
  }

  /**
   * Fetches every slice of {@code chapter} that {@code planned} says to start from — the bare
   * chapter query when no earlier run had to split it. A slice that has since become too heavy
   * still splits reactively, and the new cut goes into {@code learned}.
   */
  int fetchChapter(String url, Map<String, String> headers, String year, String chapter,
      UsitcSlicePlan planned, UsitcSlicePlan learned, Map<String, Object[]> merged)
      throws IOException {
    int rows = 0;
    for (int[] slice : planned.slices(chapter)) {
      boolean whole = slice[0] == 0 && slice[1] == HEADINGS_PER_CHAPTER - 1;
      // Depth 0 asks for the bare chapter code; any narrower slice enumerates its headings.
      rows += fetchSlice(url, headers, year, chapter, slice[0], slice[1], whole ? 0 : 1,
          learned, merged);
    }
    return rows;
  }

  /**
   * One query for one slice, accumulated into {@code merged}.
   *
   * @return the widest single measure table, which is what the grid cap applies to
   * @throws QueryTooSlowException if DataWeb never finished computing the slice
   */
  int fetchOnce(String url, Map<String, String> headers, String year, String label, String codes,
      Map<String, Object[]> merged) throws IOException {
    JsonNode dto = postAndPoll(url, headers, buildQueryBody(year, codes), year, label);
    return accumulate(merged, dto);
  }

  /** Where this run reads and writes the slice plan; overridable so tests do not touch the host's. */
  File slicePlanFile() {
    return SLICE_PLAN_FILE;
  }

  /** The HTS-4 heading at {@code offset} within {@code chapter}: ("84", 7) -> "8407". */
  private static String heading(String chapter, int offset) {
    return chapter + String.format("%02d", offset);
  }

  /** Signals a slice DataWeb never finished computing — recoverable by splitting, so it is
   *  distinct from a rejection, which splitting would not fix. */
  static final class QueryTooSlowException extends IOException {
    QueryTooSlowException(String message) {
      super(message);
    }
  }

  /**
   * POSTs one slice query and polls until the async job's {@code dto} is populated. Throws (fail
   * loud) on {@code dto.errors} or on a non-2xx HTTP status after retries, and
   * {@link QueryTooSlowException} once {@link #QUERY_DEADLINE_MS} is spent with the job still
   * unfinished.
   *
   * <p>Each "poll" re-POSTs the whole query — DataWeb hands back no job handle — so a poll costs
   * exactly what the submission cost, takes the host-wide API lock, and draws on the same rate
   * limit. The delay therefore backs off rather than staying at a fixed tick: the budget buys more
   * wall time and fewer resubmissions at once.
   */
  private JsonNode postAndPoll(String url, Map<String, String> headers, String body,
      String year, String label) throws IOException {
    long deadline = System.currentTimeMillis() + QUERY_DEADLINE_MS;
    long delay = POLL_DELAY_MS;
    int polls = 0;
    while (true) {
      String responseBody = postSerialized(url, headers, body, year, label);
      polls++;
      JsonNode root = MAPPER.readTree(responseBody);
      JsonNode dto = root.get("dto");
      if (dto != null && !dto.isNull()) {
        JsonNode errors = dto.get("errors");
        if (errors != null && errors.isArray() && errors.size() > 0) {
          throw new IOException("DataWeb rejected " + label + ": " + errors);
        }
        return dto;
      }
      if (System.currentTimeMillis() >= deadline) {
        throw new QueryTooSlowException("was still unfinished after " + polls + " polls over "
            + (QUERY_DEADLINE_MS / 1000L) + "s");
      }
      sleep(delay);
      delay = Math.min(delay * 3L / 2L, POLL_DELAY_MAX_MS);
    }
  }

  /**
   * Runs one {@link #post} under a host-wide OS file lock so concurrent worker JVMs sharing the
   * DataWeb token issue their API calls one at a time. The lock is held only for the single request
   * (not across the multi-poll wait), so workers still interleave between polls; a short spacing gap
   * is held before release to enforce a minimum global inter-call interval.
   */
  private static String postSerialized(String url, Map<String, String> headers, String body,
      String year, String label) throws IOException {
    synchronized (API_MONITOR) {
      try (RandomAccessFile raf = new RandomAccessFile(API_LOCK_FILE, "rw");
           FileChannel channel = raf.getChannel()) {
        FileLock lock = channel.lock();
        try {
          return post(url, headers, body, year, label);
        } finally {
          sleep(API_MIN_GAP_MS);
          lock.release();
        }
      }
    }
  }

  private static String post(String url, Map<String, String> headers, String body,
      String year, String label) throws IOException {
    byte[] payload = body.getBytes(StandardCharsets.UTF_8);
    IOException last = null;
    int attempt = 0;      // hard-error / 5xx attempts (draws from HTTP_RETRIES)
    int rateLimited = 0;  // 429 attempts (draws from RATE_LIMIT_RETRIES)
    while (attempt < HTTP_RETRIES && rateLimited < RATE_LIMIT_RETRIES) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setDoOutput(true);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      conn.setRequestProperty("Accept", "application/json");
      conn.setRequestProperty("Content-Type", "application/json");
      if (headers != null) {
        for (Map.Entry<String, String> h : headers.entrySet()) {
          conn.setRequestProperty(h.getKey(), h.getValue());
        }
      }
      try {
        conn.setRequestMethod("POST");
        try (OutputStream os = conn.getOutputStream()) {
          os.write(payload);
        }
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK) {
          try (InputStream in = conn.getInputStream()) {
            return readAll(in);
          }
        }
        String detail = readError(conn);
        if (status == 429) {
          rateLimited++;
          long wait = retryAfterMs(conn, rateLimited);
          LOGGER.warn("usitc_tariffs[{}]: {} HTTP 429 (rate-limit retry {}/{}, waiting {} ms)",
              year, label, rateLimited, RATE_LIMIT_RETRIES, wait);
          sleep(wait);
          continue;
        }
        if (status == 500 || status == 503) {
          attempt++;
          LOGGER.warn("usitc_tariffs[{}]: {} HTTP {} (attempt {}/{})",
              year, label, status, attempt, HTTP_RETRIES);
          sleep(1500L * attempt);
          continue;
        }
        throw new IOException("HTTP " + status + " from " + url
            + (detail != null ? " — " + detail : ""));
      } catch (IOException e) {
        last = e;
        attempt++;
        sleep(1500L * attempt);
      } finally {
        conn.disconnect();
      }
    }
    throw last != null ? last
        : new IOException("rate limit / retries exhausted for " + label);
  }

  /**
   * Wait before a 429 retry: honor a numeric {@code Retry-After} (seconds) if the server sent one,
   * otherwise exponential backoff (2s, 4s, 8s, 16s, 32s), both capped at
   * {@link #RATE_LIMIT_MAX_WAIT_MS}.
   */
  private static long retryAfterMs(HttpURLConnection conn, int rateLimited) {
    String ra = conn.getHeaderField("Retry-After");
    if (ra != null) {
      try {
        long secs = Long.parseLong(ra.trim());
        if (secs > 0) {
          return Math.min(secs * 1000L, RATE_LIMIT_MAX_WAIT_MS);
        }
      } catch (NumberFormatException e) {
        // Retry-After may be an HTTP-date form; fall through to backoff.
      }
    }
    long backoff = 2000L * (1L << Math.min(rateLimited - 1, 4));
    return Math.min(backoff, RATE_LIMIT_MAX_WAIT_MS);
  }

  /**
   * Merge one response's per-measure tables into {@code merged} keyed by (hts8, country). Returns
   * the max rows seen in any single table (to detect a slice that hit the grid cap).
   */
  private static int accumulate(Map<String, Object[]> merged, JsonNode dto) {
    JsonNode tables = dto.get("tables");
    int maxRows = 0;
    if (tables == null || !tables.isArray()) {
      return 0;
    }
    for (JsonNode table : tables) {
      String measure = text(table.path("tableInfo").path("dataToReportDesc"));
      boolean isCustoms = measure != null && measure.toLowerCase().contains("customs");
      boolean isDuty = measure != null && measure.toLowerCase().contains("dut");
      JsonNode rowGroups = table.get("row_groups");
      if (rowGroups == null || !rowGroups.isArray()) {
        continue;
      }
      for (JsonNode rg : rowGroups) {
        int htsPos = -1;
        int countryPos = -1;
        int dataPos = -1;
        JsonNode columnInfo = rg.get("columnInfo");
        if (columnInfo != null && columnInfo.isArray()) {
          for (int i = 0; i < columnInfo.size(); i++) {
            String type = text(columnInfo.get(i).path("type"));
            if ("hts".equals(type) && htsPos < 0) {
              htsPos = i;
            } else if ("country".equals(type) && countryPos < 0) {
              countryPos = i;
            } else if ("data".equals(type) && dataPos < 0) {
              dataPos = i;
            }
          }
        }
        if (htsPos < 0 || countryPos < 0 || dataPos < 0) {
          continue;
        }
        JsonNode rows = rg.get("rowsNew");
        if (rows == null || !rows.isArray()) {
          continue;
        }
        maxRows = Math.max(maxRows, rows.size());
        for (JsonNode row : rows) {
          JsonNode entries = row.get("rowEntries");
          if (entries == null || !entries.isArray() || entries.size() <= dataPos) {
            continue;
          }
          String hts = text(entries.get(htsPos).path("value"));
          String country = text(entries.get(countryPos).path("value"));
          Double val = num(text(entries.get(dataPos).path("value")));
          if (hts == null || country == null) {
            continue;
          }
          String key = hts + KEY_SEP + country;
          Object[] slot = merged.get(key);
          if (slot == null) {
            slot = new Object[]{hts, country, null, null};
            merged.put(key, slot);
          }
          if (isCustoms) {
            slot[2] = val;
          } else if (isDuty) {
            slot[3] = val;
          }
        }
      }
    }
    return maxRows;
  }

  private static List<Map<String, Object>> buildRows(Map<String, Object[]> merged) {
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>(merged.size());
    for (Object[] slot : merged.values()) {
      String hts = (String) slot[0];
      Double customs = (Double) slot[2];
      Double duty = (Double) slot[3];
      Double ave = (customs != null && customs != 0.0 && duty != null) ? duty / customs : null;
      Map<String, Object> r = new LinkedHashMap<String, Object>();
      r.put("hts8", hts);
      r.put("hs6", hts != null && hts.length() >= 6 ? hts.substring(0, 6) : hts);
      r.put("country_name", slot[1]);
      r.put("customs_value_usd", customs);
      r.put("calculated_duties_usd", duty);
      r.put("ave_duty_rate", ave);
      out.add(r);
    }
    return out;
  }

  private static String text(JsonNode n) {
    if (n == null || n.isNull() || n.isMissingNode()) {
      return null;
    }
    String s = n.asText().trim();
    return s.isEmpty() ? null : s;
  }

  /** Parse a DataWeb formatted numeric string ("2,719,750", "0") to a double, or null. */
  private static Double num(String v) {
    if (v == null) {
      return null;
    }
    String s = v.replace(",", "").trim();
    if (s.isEmpty()) {
      return null;
    }
    try {
      return Double.parseDouble(s);
    // fallback-guard: allow per-field DataWeb numeric-string parser; null on unparseable value is documented
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int n;
    while ((n = in.read(buf)) != -1) {
      bos.write(buf, 0, n);
    }
    return new String(bos.toByteArray(), StandardCharsets.UTF_8);
  }

  private static String readError(HttpURLConnection conn) {
    InputStream es = conn.getErrorStream();
    if (es == null) {
      return null;
    }
    try {
      String s = readAll(es).trim();
      return s.isEmpty() ? null : (s.length() > 500 ? s.substring(0, 500) : s);
    // fallback-guard: allow cosmetic diagnostic helper; null just means error text is unavailable for logging
    } catch (IOException e) {
      return null;
    }
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
