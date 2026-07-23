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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Transforms AlienVault OTX subscribed-pulse responses into flat
 * {@code threat_pulses} rows, handling cursor-based pagination.
 *
 * <p>Requires {@code CYBER_OTX_API_KEY} environment variable. The key is sent
 * as the {@code X-OTX-API-KEY} header on all requests.
 *
 * <p>OTX pagination uses a {@code "next"} URL in the response envelope:
 * <pre>
 * {
 *   "count": 1234,
 *   "next": "https://otx.alienvault.com/api/v1/pulses/subscribed?page=2",
 *   "previous": null,
 *   "results": [
 *     {
 *       "id": "abc123",
 *       "name": "Emotet campaign",
 *       "author_name": "researcher",
 *       "tags": ["emotet", "malware"],
 *       "targeted_countries": ["US", "UK"],
 *       "malware_families": ["Emotet", "TrickBot"],
 *       "attack_ids": ["T1566", "T1059"],
 *       "indicators": [ ... ],
 *       "created": "2024-01-15T08:30:00Z",
 *       "modified": "2024-01-20T12:00:00Z",
 *       "tlp": "white"
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>{@code first_seen} (partition column) is the date portion of {@code created}.
 * Array fields ({@code tags}, {@code targeted_countries}, ATT&CK IDs, malware names)
 * are pipe-delimited strings.
 */
public class OtxResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OtxResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Per-attempt connect/read timeout. Deliberately short (was 60s): under a flapping OTX
  // (502/503/reset) a long per-attempt timeout parks the single pull thread for up to a minute per
  // failed attempt with almost no log output — looking hung. 20s fails a bad attempt fast so the
  // retry/backoff loop cycles quickly.
  private static final int TIMEOUT_MS = 20_000;
  private static final long RATE_DELAY_MS = 500L;
  private static final int MAX_RETRIES = 5;

  // Whole-pull wall-clock budget. The rate-limited full pull is inherently long (~1300 pages x
  // 500ms), so this bounds a BAD (flapping) run, not a healthy one: on exceed the accumulated pages
  // are checkpointed and the pull fails loudly (never truncates) — the next run resumes from the
  // checkpoint rather than page 1. 0 disables.
  private static final long OVERALL_DEADLINE_MS = 45L * 60_000L;

  // Persist pagination progress ({baseUrl, nextUrl, pages, rows}) to the cache store every N pages,
  // so a killed / timed-out / deadline-failed run resumes near the last good page instead of
  // refetching the whole subscribed list from page 1.
  private static final int CHECKPOINT_EVERY_PAGES = 25;

  /** Fetch variable carrying the recovered incremental watermark (freshness {@code watermark_var}). */
  private static final String OTX_WATERMARK_VAR = "otxModifiedSince";

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

    // Optional pull-cache: OFF by default. Standard idempotence is the table's freshness:hash
    // gate (skips the Iceberg write when the assembled population is unchanged) plus the
    // modified_since delta below (bounds the fetch). The cache is an opt-in escape hatch for
    // the rare case where even the bounded re-pagination is too costly (e.g. heavy local
    // testing): set CYBER_OTX_CACHE_TTL_DAYS>0 to reuse the assembled population for that many
    // days. 0 (default) disables it entirely — no blind always-on TTL.
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
              ByteArrayOutputStream cacheBaos = new ByteArrayOutputStream();
              byte[] buf = new byte[8192];
              int n;
              while ((n = cacheIn.read(buf)) != -1) {
                cacheBaos.write(buf, 0, n);
              }
              byte[] cached = cacheBaos.toByteArray();
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

    try {
      // Incremental bound, keyed on cyber_threat.otxWriteMode (set by the launch script; mirrors
      // the Iceberg write so fetch and write never disagree):
      //   - append (production daily, warm): fetch only pulses modified since the prior run's
      //     committed watermark. The engine recovers that watermark from the freshness token
      //     (type: version = max pulse `modified`) and injects it as otxModifiedSince; here it
      //     becomes modified_since. The Iceberg write appends, accumulating version history.
      //   - append (cold, no watermark): full load — the first pull seeds the watermark.
      //   - replace (historical full snapshot, and DQ sample which needs full row-count/variety):
      //     full load, paired with replace-partitions so the snapshot stays canonical. Default.
      String baseUrl = context.getUrl();
      String writeMode = ModelOperand.getString("cyber_threat.otxWriteMode", "replace");
      String watermark = context.getVariables().get(OTX_WATERMARK_VAR);
      if (!"append".equalsIgnoreCase(writeMode)) {
        LOGGER.info("OTX: {} mode — full load (canonical snapshot)", writeMode);
      } else if (watermark != null && !watermark.trim().isEmpty()) {
        baseUrl = baseUrl + (baseUrl.contains("?") ? "&" : "?")
            + "modified_since=" + watermark.trim();
        LOGGER.info("OTX: daily delta — modified_since={} (recovered watermark)", watermark.trim());
      } else {
        LOGGER.info("OTX: daily append cold start (no watermark) — full load");
      }
      boolean deltaActive = baseUrl.contains("modified_since=");

      // Resumable pagination: the rate-limited full pull is ~1300 pages, so a kill / pool timeout /
      // deadline must not force restarting from page 1. Progress is checkpointed to the cache store
      // and resumed here when the checkpoint is for the same population (same baseUrl => same
      // mode/filter). Rows accumulate in an in-memory ArrayNode (same memory profile as the prior
      // ByteArrayOutputStream assembly) so the partial array can be serialized into the checkpoint.
      String checkpointPath = sp.resolvePath(
          sp.resolvePath(StorageProviderFactory.getGovDataCacheDir(), "cyber_threat"),
          "otx_pulses_" + cacheMode + ".checkpoint.json");

      ArrayNode rows;
      String nextUrl;
      int pages;
      ObjectNode resumed = readCheckpoint(sp, checkpointPath, baseUrl);
      if (resumed != null) {
        rows = (ArrayNode) resumed.get("rows");
        nextUrl = textOrNull(resumed, "nextUrl");
        pages = resumed.path("pages").asInt(0);
        LOGGER.info("OTX: resuming pull from checkpoint — {} rows, {} pages already fetched, "
            + "next={}", rows.size(), pages, nextUrl);
      } else {
        rows = MAPPER.createArrayNode();
        // First page: reuse the source-provided response only in full-load mode. In delta mode the
        // source already fetched the UNFILTERED first page (the source URL has no modified_since),
        // so its `next` cursor walks the entire subscribed list. Re-fetch the modified_since
        // baseUrl instead so pagination follows the bounded, filtered chain.
        String firstPage = (!deltaActive && response != null && !response.trim().isEmpty())
            ? response : fetchPage(baseUrl, apiKey);
        if (firstPage == null) {
          String emptyResult = "[]";
          if (cacheTtlMs > 0) {
            writeCacheQuietly(sp, cachePath, emptyResult);
          }
          return emptyResult;
        }
        nextUrl = processPage(firstPage, rows);
        pages = 1;
      }

      // fetchPage retries transient failures internally and THROWS if it exhausts them — a
      // partial "canonical snapshot" must fail the pull, never be written as if complete.
      long startMs = System.currentTimeMillis();
      int loggedAt = 0;
      while (nextUrl != null) {
        if (OVERALL_DEADLINE_MS > 0 && System.currentTimeMillis() - startMs > OVERALL_DEADLINE_MS) {
          // Fail loudly rather than truncate, but checkpoint first so the next run resumes here.
          writeCheckpoint(sp, checkpointPath, baseUrl, nextUrl, pages, rows);
          throw new IOException("OTX: pull exceeded " + (OVERALL_DEADLINE_MS / 60_000L)
              + "min wall-clock deadline at page " + pages + " (" + rows.size()
              + " rows) — checkpointed; the next run resumes from here rather than page 1.");
        }
        sleepQuietly(RATE_DELAY_MS);
        String page = fetchPage(nextUrl, apiKey);
        nextUrl = processPage(page, rows);
        pages++;
        if (pages % CHECKPOINT_EVERY_PAGES == 0) {
          writeCheckpoint(sp, checkpointPath, baseUrl, nextUrl, pages, rows);
        }
        // Log each time we cross another 1000 rows (robust to non-exact multiples), with the
        // page count, so a slow-but-progressing pull is distinguishable from a stall.
        if (rows.size() - loggedAt >= 1000) {
          loggedAt = rows.size();
          LOGGER.info("OTX: accumulated {} pulse rows across {} pages", rows.size(), pages);
        }
      }

      LOGGER.info("OTX: returning {} threat_pulses rows", rows.size());
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

  /** Writes the assembled result to the cache, logging debug on any failure (never throws). */
  private static void writeCacheQuietly(StorageProvider sp, String cachePath, String assembled) {
    try {
      sp.writeFile(cachePath, assembled.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      LOGGER.debug("OTX: cache write failed ({}), continuing", e.getMessage());
    }
  }

  /**
   * Processes one page of results, appending flattened rows to {@code out}. Returns the
   * {@code next} URL, or null if no more pages.
   */
  private String processPage(String pageJson, ArrayNode out) throws Exception {
    JsonNode root = MAPPER.readTree(pageJson);

    JsonNode results = root.path("results");
    if (!results.isArray()) {
      LOGGER.warn("OTX: results not an array in response");
      return null;
    }

    for (JsonNode pulse : results) {
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
      row.put("modified", textOrNull(pulse, "modified"));
      row.put("tlp", textOrNull(pulse, "tlp"));
      row.put("source", "otx");
      row.put("first_seen", extractDate(created));

      out.add(row);
    }

    String next = textOrNull(root, "next");
    return (next != null && next.startsWith("http")) ? next : null;
  }

  /**
   * Reads a resumable pagination checkpoint if one exists and is for the current pull. Returns the
   * checkpoint object ({@code rows}, {@code nextUrl}, {@code pages}) or null to start fresh. A
   * checkpoint whose {@code baseUrl} differs from the current pull (mode/watermark changed) or that
   * is malformed is ignored — resuming onto a different population would corrupt the snapshot.
   */
  private ObjectNode readCheckpoint(StorageProvider sp, String path, String baseUrl) {
    try {
      if (!sp.exists(path)) {
        return null;
      }
      byte[] bytes;
      try (InputStream in = sp.openInputStream(path)) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) != -1) {
          b.write(buf, 0, n);
        }
        bytes = b.toByteArray();
      }
      JsonNode node = MAPPER.readTree(bytes);
      if (!node.isObject() || !node.path("rows").isArray()) {
        LOGGER.warn("OTX: ignoring malformed checkpoint {} — starting fresh", path);
        return null;
      }
      String savedBase = textOrNull(node, "baseUrl");
      if (savedBase == null || !savedBase.equals(baseUrl)) {
        LOGGER.info("OTX: checkpoint is for a different pull ({} != {}) — starting fresh",
            savedBase, baseUrl);
        return null;
      }
      return (ObjectNode) node;
    } catch (Exception e) {
      LOGGER.warn("OTX: checkpoint read failed ({}) — starting fresh", e.getMessage());
      return null;
    }
  }

  /**
   * Persists pagination progress. Best-effort: a checkpoint write failure must never fail the pull
   * (it only costs resume granularity), so it logs and continues rather than throwing.
   */
  private void writeCheckpoint(StorageProvider sp, String path, String baseUrl, String nextUrl,
      int pages, ArrayNode rows) {
    try {
      ObjectNode cp = MAPPER.createObjectNode();
      cp.put("baseUrl", baseUrl);
      cp.put("nextUrl", nextUrl);
      cp.put("pages", pages);
      cp.set("rows", rows);
      sp.writeFile(path, MAPPER.writeValueAsBytes(cp));
    } catch (Exception e) {
      LOGGER.debug("OTX: checkpoint write failed ({}), continuing", e.getMessage());
    }
  }

  /** Removes the checkpoint after a successful, complete pull. Best-effort. */
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
   * Fetches one page, retrying transient failures (429, 5xx, network timeouts/resets) with
   * bounded exponential backoff (honoring {@code Retry-After} for 429). Permanent failures
   * (auth, unexpected 4xx) and exhausted retries THROW — the caller must fail the pull rather
   * than silently truncate the snapshot.
   */
  private String fetchPage(String url, String apiKey) throws IOException {
    String lastErr = "unknown";
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = null;
      try {
        conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(TIMEOUT_MS);
        conn.setReadTimeout(TIMEOUT_MS);
        conn.setRequestProperty("X-OTX-API-KEY", apiKey);
        conn.setRequestProperty("Accept", "application/json");

        int status = conn.getResponseCode();
        if (status == 200) {
          try (BufferedReader reader = new BufferedReader(
              new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
              sb.append(line);
            }
            return sb.toString();
          }
        }
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
            lastErr, url, attempt, MAX_RETRIES, backoff);
        sleepQuietly(backoff);
      } catch (IOException e) {
        // Network-level failure (timeout, connection reset, DNS) — retryable.
        lastErr = e.toString();
        long backoff = retryDelayMs(null, attempt);
        LOGGER.warn("OTX: network error fetching {} (attempt {}/{}) — backing off {}ms: {}",
            url, attempt, MAX_RETRIES, backoff, e.getMessage());
        sleepQuietly(backoff);
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
    }
    throw new IOException("OTX: giving up on " + url + " after " + MAX_RETRIES
        + " attempts (last error: " + lastErr + ")");
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
