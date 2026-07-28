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
package org.apache.calcite.adapter.govdata.cyber.threat;

import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.ModelOperand;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Transforms AlienVault OTX subscribed-pulse responses into flat {@code threat_pulses} rows.
 *
 * <p>Requires {@code CYBER_OTX_API_KEY}, sent as the {@code X-OTX-API-KEY} header.
 *
 * <h3>Why this does not follow the {@code next} cursor</h3>
 *
 * <p>OTX's {@code next} link is offset pagination ({@code ?page=N}), and the server pays a cost
 * proportional to the offset. Measured against the live API, latency degrades linearly with how
 * many rows are being skipped — independent of page size:
 *
 * <pre>
 *   limit=5    page=1 → 0.50s    page=200 → 6.26s    page=500 → 11.94s
 *   limit=50   page=1 → 13.2s    page=100 → 24.6s    page=177 → 40.1s
 * </pre>
 *
 * <p>The feed's default page size is 5, so a full load is ~1,770 pages whose tail requests take
 * 20-40s each. Following {@code next} therefore cannot complete a full load: the deep half of the
 * crawl exceeds any sane per-request timeout, and the summed latency runs to hours.
 *
 * <p>So the crawl is driven here as a <b>keyset</b> scan instead. {@code sort=modified} orders the
 * feed ascending, and {@code modified_since} is a strict {@code >} filter, so every request is
 * page 1 of the remaining population:
 *
 * <pre>
 *   GET /pulses/subscribed
 *       ?limit=50&amp;sort=modified&amp;modified_since=&lt;max modified seen so far&gt;
 * </pre>
 *
 * <p>Offset depth never grows, so per-request latency stays flat (~0.6-1.3s measured) for the whole
 * crawl. A measured full load is 182 requests / 8,847 rows in 6.4 min, against ~1,770
 * ever-slower requests before.
 *
 * <p>Three details make this safe rather than merely fast:
 * <ul>
 *   <li>{@code limit=50} is the server-side maximum — {@code limit=100} and {@code limit=200} are
 *       silently clamped to 50, so asking for more only hides the real page size.</li>
 *   <li>{@code modified} is <b>not unique</b>. Bulk imports leave groups of pulses sharing an
 *       identical microsecond stamp — 8 at {@code 2020-06-15T18:33:01.745000}, with pairs
 *       throughout the feed. Since {@code modified_since} is strictly exclusive, advancing the
 *       cursor onto a page's maximum would step over any tied rows the server truncated off the
 *       end of that page. So a full page holds its trailing tie group back and advances only to
 *       the highest fully-consumed timestamp; the next request re-fetches that group whole.</li>
 *   <li>The envelope's {@code count} audits exactly that: it must fall by the number of rows
 *       consumed, no more. A larger drop means rows were stepped over anyway, and fails the crawl
 *       rather than yielding a quietly short snapshot. A cursor that cannot advance — a full page
 *       of one single timestamp — fails too, instead of spinning.</li>
 * </ul>
 *
 * <p>{@code first_seen} (partition column) is the date portion of {@code created}. Array fields
 * ({@code tags}, {@code targeted_countries}, ATT&amp;CK IDs, malware names) are pipe-delimited.
 */
public class OtxResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OtxResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** OTX's server-side maximum page size — {@code limit=100}/{@code 200} are clamped to this. */
  private static final int PAGE_LIMIT = 50;

  /** Ascending sort on {@code modified}; the ordering that makes the feed keyset-pageable. */
  private static final String KEYSET_SORT = "modified";

  /** Full-load cursor floor — older than the oldest pulse in the feed (2015-01-14). */
  private static final String EPOCH_FLOOR = "1970-01-01T00:00:00";

  // Per-attempt connect/read timeout. Keyset pages are flat-latency (~1-3s, worst observed 13s on a
  // loaded backend), so 45s is headroom rather than a budget: anything slower is a stuck
  // connection, not a big page, and failing it lets the retry loop cycle rather than park the
  // pull thread.
  private static final int TIMEOUT_MS = 45_000;

  // Fallbacks used only when the source declares no rateLimit: block.
  private static final int DEFAULT_MAX_RETRIES = 5;
  private static final long DEFAULT_RATE_DELAY_MS = 500L;

  // Whole-crawl wall-clock budget. A healthy full load measures ~6.5 min, so this bounds a
  // flapping run: on exceed the accumulated pages are checkpointed and the pull fails loudly (never
  // truncates) — the next run resumes from the cursor rather than the start. 0 disables.
  private static final long OVERALL_DEADLINE_MS = 45L * 60_000L;

  // Persist crawl progress ({populationKey, cursor, pages, rows}) every N pages so a killed /
  // timed-out / deadline-failed run resumes at the last checkpointed cursor.
  private static final int CHECKPOINT_EVERY_PAGES = 25;

  private static final String USER_AGENT = "GovData/1.0";

  /** Fetch variable carrying the recovered watermark (freshness {@code watermark_var}). */
  private static final String OTX_WATERMARK_VAR = "otxModifiedSince";

  /**
   * {@inheritDoc}
   *
   * <p>The {@code response} the framework already fetched is deliberately discarded. It was made
   * against the bare source URL, so it carries the default page size and descending order — the
   * wrong shape, and the wrong end of the feed, to seed a keyset scan from. Re-fetching page one on
   * our own terms costs a single small request and keeps the pagination contract in one place here,
   * rather than split between this class and a query string in the schema YAML.
   */
  @Override public String transform(String response, RequestContext context) {
    // Read the key from the request context's headers — the framework resolves the source's
    // ${CYBER_OTX_API_KEY:} header at load and passes it here. (Reading it back out of the model
    // via ModelOperand's partitionedTables path returned empty in practice, so use the resolved
    // header the transformer is already handed.)
    String apiKey = context.getHeaders().get("X-OTX-API-KEY");
    if (apiKey == null || apiKey.trim().isEmpty()) {
      // A required credential being absent is a hard failure, never a silent skip.
      throw new IllegalStateException("OTX: CYBER_OTX_API_KEY is required but missing "
          + "(threat_pulses source X-OTX-API-KEY resolved empty).");
    }

    // GOVDATA_DQ is an allowed global run-flag exception — set cross-schema by the run scripts.
    boolean dqMode = "true".equalsIgnoreCase(System.getenv("GOVDATA_DQ"));
    String cacheMode = dqMode ? "dq" : "full";

    // Optional pull-cache: OFF by default. Standard idempotence is the table's freshness gate
    // (skips the Iceberg write when the max `modified` is unchanged) plus the modified_since cursor
    // below (bounds the fetch). The cache is an opt-in escape hatch for the rare case where even
    // the bounded crawl is too costly (e.g. heavy local testing): set CYBER_OTX_CACHE_TTL_DAYS>0
    // to reuse the assembled population for that many days. 0 (default) disables it entirely.
    long cacheTtlMs = ModelOperand.getLong("cyber_threat.otxCacheTtlDays", 0L) * 86_400_000L;
    StorageProvider sp = StorageProviderFactory.createForGovDataCache();
    String cachePath = sp.resolvePath(
        sp.resolvePath(StorageProviderFactory.getGovDataCacheDir(), "cyber_threat"),
        "otx_pulses_" + cacheMode + ".json");

    if (cacheTtlMs > 0) {
      try {
        if (sp.exists(cachePath)) {
          long age = System.currentTimeMillis() - sp.getMetadata(cachePath).getLastModified();
          if (age < cacheTtlMs) {
            try (InputStream cacheIn = sp.openInputStream(cachePath)) {
              byte[] cached = readFully(cacheIn);
              LOGGER.info("OTX: reusing cached pulse population within {}-day opt-in TTL "
                  + "({} bytes): {}", cacheTtlMs / 86_400_000L, cached.length, cachePath);
              return new String(cached, StandardCharsets.UTF_8);
            }
          }
        }
      } catch (IOException e) {
        LOGGER.debug("OTX: cache read failed ({}), falling through to live pull", e.getMessage());
      }
    }

    // Make the source's declared rateLimit: block actually apply. This transformer opens its own
    // connections (it needs gzip and its own timeouts), so without this the YAML block would be
    // dead config that looks tunable but changes nothing.
    HttpSourceConfig.RateLimitConfig rateLimit = context.getRateLimit();
    int maxRetries = rateLimit != null && rateLimit.getMaxRetries() > 0
        ? rateLimit.getMaxRetries() : DEFAULT_MAX_RETRIES;
    long rateDelayMs = rateLimit != null && rateLimit.getRequestsPerSecond() > 0
        ? 1000L / rateLimit.getRequestsPerSecond() : DEFAULT_RATE_DELAY_MS;

    try {
      // Cursor start, keyed on cyber_threat.otxWriteMode (set by the launch script; mirrors the
      // Iceberg write so fetch and write never disagree):
      //   - append (production daily, warm): start from the prior run's committed watermark. The
      //     engine recovers it from the freshness token (type: version = max pulse `modified`) and
      //     injects it as otxModifiedSince. The Iceberg write appends, accumulating version
      //     history.
      //   - append (cold, no watermark): full load from the epoch floor — seeds the watermark.
      //   - replace (historical full snapshot, and the DQ sample, which needs full row-count and
      //     variety): full load, paired with replace-partitions so the snapshot stays canonical.
      String baseUrl = stripQuery(context.getUrl());
      String writeMode = ModelOperand.getString("cyber_threat.otxWriteMode", "replace");
      String watermark = context.getVariables().get(OTX_WATERMARK_VAR);

      String startCursor;
      boolean warmDelta = "append".equalsIgnoreCase(writeMode)
          && watermark != null && !watermark.trim().isEmpty();
      if (warmDelta) {
        startCursor = watermark.trim();
        LOGGER.info("OTX: daily delta — keyset crawl from modified_since={} (recovered watermark)",
            startCursor);
      } else {
        startCursor = EPOCH_FLOOR;
        LOGGER.info("OTX: {} mode — full keyset crawl from {}", writeMode, EPOCH_FLOOR);
      }

      // A checkpoint is only resumable onto the same population; the start cursor and page size are
      // part of that identity, since resuming a delta crawl onto a full load (or vice versa) would
      // silently produce a partial snapshot.
      String populationKey = baseUrl + "|since=" + startCursor + "|limit=" + PAGE_LIMIT;
      String checkpointPath = sp.resolvePath(
          sp.resolvePath(StorageProviderFactory.getGovDataCacheDir(), "cyber_threat"),
          "otx_pulses_" + cacheMode + ".checkpoint.json");

      ArrayNode rows;
      String cursor;
      int pages;
      ObjectNode resumed = readCheckpoint(sp, checkpointPath, populationKey);
      if (resumed != null) {
        rows = (ArrayNode) resumed.get("rows");
        cursor = textOrNull(resumed, "cursor");
        pages = resumed.path("pages").asInt(0);
        LOGGER.info("OTX: resuming keyset crawl from checkpoint — {} rows, {} pages already "
            + "fetched, cursor={}", rows.size(), pages, cursor);
      } else {
        rows = MAPPER.createArrayNode();
        cursor = startCursor;
        pages = 0;
      }

      // `count` is the number of pulses still matching the cursor filter. Because the filter is
      // strictly exclusive, it must fall by exactly the number of rows just consumed; a bigger drop
      // means the crawl stepped over rows. -1 = no prior page to compare against (the first page,
      // or the first page after a resume).
      long prevCount = -1L;
      int prevRows = 0;

      long startMs = System.currentTimeMillis();
      int loggedAt = rows.size();

      while (true) {
        if (OVERALL_DEADLINE_MS > 0 && System.currentTimeMillis() - startMs > OVERALL_DEADLINE_MS) {
          // Fail loudly rather than truncate, but checkpoint first so the next run resumes here.
          writeCheckpoint(sp, checkpointPath, populationKey, cursor, pages, rows);
          throw new IOException("OTX: keyset crawl exceeded " + (OVERALL_DEADLINE_MS / 60_000L)
              + "min wall-clock deadline after " + pages + " pages (" + rows.size()
              + " rows, cursor=" + cursor + ") — checkpointed; the next run resumes from this "
              + "cursor rather than the start.");
        }
        if (pages > 0) {
          sleepQuietly(rateDelayMs);
        }

        // fetchPage retries transient failures internally and THROWS if it exhausts them — a
        // partial "canonical snapshot" must fail the pull, never be written as if complete.
        JsonNode root = MAPPER.readTree(fetchPage(keysetUrl(baseUrl, cursor), apiKey, maxRetries));
        JsonNode results = root.path("results");
        if (!results.isArray()) {
          throw new IOException("OTX: 'results' missing or not an array at cursor " + cursor);
        }
        long count = root.path("count").asLong(-1L);

        if (prevCount >= 0 && count >= 0 && count < prevCount - prevRows) {
          throw new IOException("OTX: keyset crawl stepped over "
              + (prevCount - prevRows - count) + " pulse(s) at cursor " + cursor
              + " — remaining count fell to " + count + " where " + (prevCount - prevRows)
              + " was expected. Pulses sharing an identical `modified` across the page boundary "
              + "are excluded by the strict modified_since filter; the snapshot would be short.");
        }

        int pageRows = results.size();
        if (pageRows == 0) {
          LOGGER.info("OTX: keyset crawl drained after {} pages at cursor {}", pages, cursor);
          break;
        }

        // On a full page the highest `modified` may be mid-tie — the server had more rows sharing
        // it that did not fit. Advancing onto it would exclude them, so hold that group back and
        // advance only to the highest fully-consumed timestamp. A short page is the tail of the
        // population, so nothing can be pending and the page maximum is safe.
        boolean fullPage = pageRows >= PAGE_LIMIT;
        String nextCursor = nextCursor(results, fullPage);
        if (nextCursor == null) {
          throw new IOException("OTX: cannot advance the keyset cursor past " + cursor + " — all "
              + pageRows + " pulses on this page share one `modified`, so a page of " + PAGE_LIMIT
              + " cannot step over the tie group.");
        }
        if (nextCursor.compareTo(cursor) <= 0) {
          throw new IOException("OTX: keyset cursor went backwards, " + cursor + " -> "
              + nextCursor + " — the feed is not ordered by `modified` ascending as sort="
              + KEYSET_SORT + " requires.");
        }

        int kept = appendRows(results, rows, nextCursor);
        pages++;
        cursor = nextCursor;
        prevCount = count;
        // Only the rows at or below the new cursor are consumed; a held-back tie group is still
        // pending and must stay in the expected remaining count.
        prevRows = kept;

        if (pages % CHECKPOINT_EVERY_PAGES == 0) {
          writeCheckpoint(sp, checkpointPath, populationKey, cursor, pages, rows);
        }
        // Log each time we cross another 1000 rows (robust to non-exact multiples), with the page
        // count, so a slow-but-progressing crawl is distinguishable from a stall.
        if (rows.size() - loggedAt >= 1000) {
          loggedAt = rows.size();
          LOGGER.info("OTX: accumulated {} pulse rows across {} pages (cursor {})",
              rows.size(), pages, cursor);
        }
      }

      LOGGER.info("OTX: returning {} threat_pulses rows from {} pages", rows.size(), pages);
      String assembled = MAPPER.writeValueAsString(rows);
      deleteCheckpointQuietly(sp, checkpointPath);
      if (cacheTtlMs > 0) {
        writeCacheQuietly(sp, cachePath, assembled);
      }
      return assembled;

    } catch (Exception e) {
      LOGGER.error("OTX: failed: {}", e.getMessage());
      throw new RuntimeException("Failed to process OTX pulses: " + e.getMessage(), e);
    }
  }

  /** Builds the keyset request for the remaining population after {@code cursor}. */
  private static String keysetUrl(String baseUrl, String cursor) {
    return baseUrl + "?limit=" + PAGE_LIMIT + "&sort=" + KEYSET_SORT
        + "&modified_since=" + urlEncode(cursor);
  }

  /** Drops any query string from the source URL — this class owns the whole keyset query. */
  private static String stripQuery(String url) {
    int q = url.indexOf('?');
    return q < 0 ? url : url.substring(0, q);
  }

  private static String urlEncode(String value) {
    try {
      return URLEncoder.encode(value, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // UTF-8 is required of every JVM; reaching here means the platform is broken.
      throw new IllegalStateException("UTF-8 unavailable", e);
    }
  }

  /** Writes the assembled result to the cache, logging debug on any failure (never throws). */
  private static void writeCacheQuietly(StorageProvider sp, String cachePath, String assembled) {
    try {
      sp.writeFile(cachePath, assembled.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      LOGGER.debug("OTX: cache write failed ({}), continuing", e.getMessage());
    }
  }

  /**
   * Picks the timestamp to advance the cursor to.
   *
   * <p>For a short page — the tail of the population — every row is present, so the page maximum is
   * safe. For a full page the maximum may be only part of a tie group the server truncated, so this
   * returns the second-highest <em>distinct</em> {@code modified} instead: the highest value whose
   * rows are certainly all in hand. Returns null when a full page carries a single distinct
   * timestamp, which no {@code limit=}-sized window can step over.
   *
   * <p>Ties are not hypothetical in this feed: bulk imports leave groups sharing an identical
   * microsecond stamp (8 pulses at {@code 2020-06-15T18:33:01.745000}, with pairs throughout).
   */
  private static String nextCursor(JsonNode results, boolean fullPage) {
    String max = null;
    String secondMax = null;
    for (JsonNode pulse : results) {
      String modified = textOrNull(pulse, "modified");
      if (modified == null) {
        continue;
      }
      if (max == null || modified.compareTo(max) > 0) {
        if (max != null && (secondMax == null || max.compareTo(secondMax) > 0)) {
          secondMax = max;
        }
        max = modified;
      } else if (!modified.equals(max)
          && (secondMax == null || modified.compareTo(secondMax) > 0)) {
        secondMax = modified;
      }
    }
    return fullPage ? secondMax : max;
  }

  /**
   * Flattens the rows at or below {@code ceiling} onto {@code out}, returning how many were kept.
   * Rows above the ceiling are a partially-served tie group; they are left for the next request,
   * which re-fetches them whole. Rows carrying no {@code modified} cannot be positioned against the
   * ceiling, so they are kept where they arrived rather than dropped.
   */
  private int appendRows(JsonNode results, ArrayNode out, String ceiling) {
    int kept = 0;
    for (JsonNode pulse : results) {
      String modified = textOrNull(pulse, "modified");
      if (modified != null && modified.compareTo(ceiling) > 0) {
        continue;
      }
      kept++;

      String pulseId = textOrNull(pulse, "id");
      if (pulseId == null) {
        continue;
      }

      String created = textOrNull(pulse, "created");

      ObjectNode row = MAPPER.createObjectNode();
      row.put("pulse_id", pulseId);
      row.put("name", textOrNull(pulse, "name"));
      row.put("author", textOrNull(pulse, "author_name"));
      row.put("tags", joinStringArray(pulse.path("tags")));
      row.put("targeted_countries", joinStringArray(pulse.path("targeted_countries")));
      // OTX v1 returns malware_families/attack_ids as plain string arrays (not {id,display_name}
      // objects) and no indicator_count — the inline indicators array carries the count.
      row.put("malware_families", joinStringArray(pulse.path("malware_families")));
      row.put("attack_ids", joinStringArray(pulse.path("attack_ids")));
      JsonNode indicatorsNode = pulse.path("indicators");
      if (indicatorsNode.isArray()) {
        row.put("ioc_count", indicatorsNode.size());
      } else {
        row.putNull("ioc_count");
      }
      row.put("created", created);
      row.put("modified", modified);
      row.put("tlp", textOrNull(pulse, "tlp"));
      row.put("source", "otx");
      row.put("first_seen", extractDate(created));

      out.add(row);
    }
    return kept;
  }

  /**
   * Reads a resumable crawl checkpoint if one exists and is for the current population. Returns the
   * checkpoint object ({@code rows}, {@code cursor}, {@code pages}) or null to start fresh. A
   * checkpoint whose {@code populationKey} differs (mode/watermark/page size changed) or that is
   * malformed is ignored — resuming onto a different population would corrupt the snapshot.
   */
  private ObjectNode readCheckpoint(StorageProvider sp, String path, String populationKey) {
    try {
      if (!sp.exists(path)) {
        return null;
      }
      byte[] bytes;
      try (InputStream in = sp.openInputStream(path)) {
        bytes = readFully(in);
      }
      JsonNode node = MAPPER.readTree(bytes);
      if (!node.isObject() || !node.path("rows").isArray()
          || textOrNull(node, "cursor") == null) {
        LOGGER.warn("OTX: ignoring malformed checkpoint {} — starting fresh", path);
        return null;
      }
      String savedKey = textOrNull(node, "populationKey");
      if (savedKey == null || !savedKey.equals(populationKey)) {
        LOGGER.info("OTX: checkpoint is for a different population ({} != {}) — starting fresh",
            savedKey, populationKey);
        return null;
      }
      return (ObjectNode) node;
    } catch (Exception e) {
      LOGGER.warn("OTX: checkpoint read failed ({}) — starting fresh", e.getMessage());
      return null;
    }
  }

  /**
   * Persists crawl progress. Best-effort: a checkpoint write failure must never fail the pull (it
   * only costs resume granularity), so it logs and continues rather than throwing.
   */
  private void writeCheckpoint(StorageProvider sp, String path, String populationKey, String cursor,
      int pages, ArrayNode rows) {
    try {
      ObjectNode cp = MAPPER.createObjectNode();
      cp.put("populationKey", populationKey);
      cp.put("cursor", cursor);
      cp.put("pages", pages);
      cp.set("rows", rows);
      sp.writeFile(path, MAPPER.writeValueAsBytes(cp));
    } catch (Exception e) {
      LOGGER.debug("OTX: checkpoint write failed ({}), continuing", e.getMessage());
    }
  }

  /** Removes the checkpoint after a successful, complete crawl. Best-effort. */
  private void deleteCheckpointQuietly(StorageProvider sp, String path) {
    try {
      if (sp.exists(path)) {
        sp.delete(path);
      }
    } catch (Exception e) {
      LOGGER.debug("OTX: checkpoint delete failed ({}), continuing", e.getMessage());
    }
  }

  /** Joins a JSON string array into a pipe-delimited string. */
  private static String joinStringArray(JsonNode arr) {
    if (!arr.isArray() || arr.size() == 0) {
      return null;
    }
    List<String> items = new ArrayList<String>();
    for (JsonNode item : arr) {
      String val = item.asText(null);
      if (val != null && !val.isEmpty()) {
        items.add(val);
      }
    }
    return joinList(items);
  }

  private static String joinList(List<String> items) {
    if (items.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append("|");
      }
      sb.append(items.get(i));
    }
    return sb.toString();
  }

  private static String extractDate(String datetime) {
    if (datetime == null || datetime.length() < 10) {
      return datetime;
    }
    return datetime.substring(0, 10);
  }

  /**
   * Fetches one page, retrying transient failures (429, 5xx, network timeouts/resets) with bounded
   * exponential backoff (honoring {@code Retry-After} for 429). Permanent failures (auth,
   * unexpected 4xx) and exhausted retries THROW — the caller must fail the crawl rather than
   * silently truncate the snapshot.
   *
   * <p>Requests gzip: a {@code limit=50} page is ~342KB raw but ~68KB compressed, a 5x reduction in
   * bytes and roughly half the wall-clock per page across the whole feed.
   *
   * <p>On success the connection is NOT disconnected. The body is fully read and closed, which lets
   * the JVM return the keep-alive socket to its pool for the next page; {@code disconnect()}
   * would instead force a fresh TLS handshake on every one of the ~182 requests.
   */
  private String fetchPage(String url, String apiKey, int maxRetries) throws IOException {
    String lastErr = "unknown";
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      HttpURLConnection conn = null;
      boolean poolable = false;
      try {
        conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(TIMEOUT_MS);
        conn.setReadTimeout(TIMEOUT_MS);
        conn.setRequestProperty("X-OTX-API-KEY", apiKey);
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Accept-Encoding", "gzip");
        conn.setRequestProperty("User-Agent", USER_AGENT);

        int status = conn.getResponseCode();
        if (status == 200) {
          String body = readBody(conn);
          poolable = true;
          return body;
        }
        // Drain before deciding: an undrained error stream cannot be released cleanly.
        drainErrorStream(conn);
        if (status == 401 || status == 403) {
          // Permanent: bad/missing key. Fail loudly — never truncate the snapshot.
          throw new IllegalStateException("OTX auth failure HTTP " + status
              + " — check CYBER_OTX_API_KEY");
        }
        if (status != 429 && status < 500) {
          throw new IllegalStateException("OTX unexpected HTTP " + status + " fetching " + url);
        }
        // 429 / 5xx — retryable.
        lastErr = "HTTP " + status;
        long backoff = retryDelayMs(conn, attempt);
        LOGGER.warn("OTX: {} on {} (attempt {}/{}) — backing off {}ms",
            lastErr, url, attempt, maxRetries, backoff);
        sleepQuietly(backoff);
      } catch (IOException e) {
        // Network-level failure (timeout, connection reset, DNS) — retryable.
        lastErr = e.toString();
        long backoff = retryDelayMs(null, attempt);
        LOGGER.warn("OTX: network error fetching {} (attempt {}/{}) — backing off {}ms: {}",
            url, attempt, maxRetries, backoff, e.getMessage());
        sleepQuietly(backoff);
      } finally {
        if (conn != null && !poolable) {
          conn.disconnect();
        }
      }
    }
    throw new IOException("OTX: giving up on " + url + " after " + maxRetries
        + " attempts (last error: " + lastErr + ")");
  }

  /** Reads the response body, transparently decoding a gzip-encoded one. */
  private static String readBody(HttpURLConnection conn) throws IOException {
    InputStream in = conn.getInputStream();
    if ("gzip".equalsIgnoreCase(conn.getContentEncoding())) {
      in = new GZIPInputStream(in);
    }
    try {
      return new String(readFully(in), StandardCharsets.UTF_8);
    } finally {
      in.close();
    }
  }

  /** Consumes and closes the error stream so the connection can be released cleanly. */
  private static void drainErrorStream(HttpURLConnection conn) {
    InputStream err = conn.getErrorStream();
    if (err == null) {
      return;
    }
    try {
      byte[] buf = new byte[4096];
      while (err.read(buf) != -1) {
        continue;
      }
    } catch (IOException e) {
      LOGGER.debug("OTX: error-stream drain failed ({}), continuing", e.getMessage());
    } finally {
      try {
        err.close();
      } catch (IOException e) {
        LOGGER.debug("OTX: error-stream close failed ({}), continuing", e.getMessage());
      }
    }
  }

  private static byte[] readFully(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  /** Retry delay: honor the 429 {@code Retry-After} seconds header if present, else exponential
   *  backoff (1,2,4,…,60s cap). */
  private static long retryDelayMs(HttpURLConnection conn, int attempt) {
    if (conn != null) {
      String ra = conn.getHeaderField("Retry-After");
      if (ra != null) {
        try {
          long secs = Long.parseLong(ra.trim());
          if (secs > 0) {
            return Math.min(120_000L, secs * 1000L);
          }
        } catch (NumberFormatException ignored) {
          // Retry-After may be an HTTP-date; fall through to exponential.
        }
      }
    }
    int shift = Math.min(attempt - 1, 6);
    return Math.min(60_000L, 1_000L * (1L << shift));
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    String t = v.asText();
    return t.isEmpty() ? null : t;
  }

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
