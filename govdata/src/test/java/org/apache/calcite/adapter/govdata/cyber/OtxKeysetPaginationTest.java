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
package org.apache.calcite.adapter.govdata.cyber;

import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;
import org.apache.calcite.adapter.govdata.cyber.threat.OtxResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Live integration test for {@link OtxResponseTransformer}'s keyset crawl.
 *
 * <p>This exercises the real AlienVault OTX API rather than a fixture, because the bug it guards
 * against was a property of the live endpoint (offset pagination whose per-request cost grows with
 * depth), not of our parsing. A recorded fixture would have passed against the broken code.
 *
 * <p>Requires {@code CYBER_OTX_API_KEY}; skipped when absent.
 */
@Tag("integration")
public class OtxKeysetPaginationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(OtxKeysetPaginationTest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String OTX_URL = "https://otx.alienvault.com/api/v1/pulses/subscribed";

  private static String apiKey;

  @BeforeAll static void loadEnv() {
    TestEnvironmentLoader.ensureLoaded();
    apiKey = TestEnvironmentLoader.getEnv("CYBER_OTX_API_KEY");
  }

  /** Builds the context the ETL framework would hand the transformer. */
  private static RequestContext context(Map<String, String> variables) {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-OTX-API-KEY", apiKey);

    Map<String, Object> rateLimitMap = new HashMap<String, Object>();
    rateLimitMap.put("requestsPerSecond", 2.0);
    rateLimitMap.put("maxRetries", 5);

    return RequestContext.builder()
        .url(OTX_URL)
        .headers(headers)
        .variables(variables != null ? variables : Collections.<String, String>emptyMap())
        .rateLimit(HttpSourceConfig.RateLimitConfig.fromMap(rateLimitMap))
        .build();
  }

  /**
   * Full load: the crawl must drain the entire subscribed feed. This is the case that could not
   * complete at all under offset pagination — the deep half of the walk exceeded the per-request
   * timeout, so the pull failed every time at roughly the same place.
   */
  @Test void fullCrawlDrainsEntireFeedWithoutDuplicates() throws Exception {
    assumeTrue(apiKey != null && !apiKey.isEmpty(), "CYBER_OTX_API_KEY not set");

    long startMs = System.currentTimeMillis();
    String json = new OtxResponseTransformer().transform(null, context(null));
    long elapsedMs = System.currentTimeMillis() - startMs;

    JsonNode rows = MAPPER.readTree(json);
    assertTrue(rows.isArray(), "transformer must return a JSON array");
    LOGGER.info("OTX full crawl: {} rows in {}ms", rows.size(), elapsedMs);

    // The feed carried 8,847 pulses when this was written and grows slowly. A floor well under
    // that catches a truncated crawl without making the test brittle as the feed changes.
    assertTrue(rows.size() > 8000,
        "expected the full subscribed feed (~8.8k pulses), got " + rows.size());

    // Keyset scanning is only correct if the strictly-exclusive cursor neither repeats nor skips.
    // Duplicates are directly checkable here; skips are caught in-crawl by the count arithmetic,
    // which would have thrown before returning.
    Set<String> ids = new HashSet<String>();
    String minModified = null;
    String maxModified = null;
    for (JsonNode row : rows) {
      String pulseId = row.path("pulse_id").asText(null);
      assertNotNull(pulseId, "pulse_id must never be null");
      assertTrue(ids.add(pulseId), "duplicate pulse_id from the keyset crawl: " + pulseId);

      String modified = row.path("modified").asText(null);
      assertNotNull(modified, "modified drives the cursor and must never be null");
      if (minModified == null || modified.compareTo(minModified) < 0) {
        minModified = modified;
      }
      if (maxModified == null || modified.compareTo(maxModified) > 0) {
        maxModified = modified;
      }
    }
    assertEquals(rows.size(), ids.size(), "pulse_ids must be unique across the crawl");

    // The crawl starts at the epoch floor, so a complete drain must reach back to the oldest
    // pulses in the feed (2015) — proving it did not silently start partway in.
    assertTrue(minModified.startsWith("2015"),
        "full crawl should reach the oldest pulses (2015), oldest seen: " + minModified);
    LOGGER.info("OTX modified range: {} .. {}", minModified, maxModified);

    // Column contract the Iceberg write depends on.
    JsonNode first = rows.get(0);
    for (String column : new String[]{"pulse_id", "name", "author", "tags", "targeted_countries",
        "malware_families", "attack_ids", "ioc_count", "created", "modified", "tlp", "source",
        "first_seen"}) {
      assertTrue(first.has(column), "missing column in emitted row: " + column);
    }
    assertEquals("otx", first.path("source").asText());
    assertEquals(first.path("created").asText().substring(0, 10),
        first.path("first_seen").asText(), "first_seen must be the date portion of created");

    // Flat-latency is the whole point: ~178 requests at ~1s plus 500ms spacing. The old offset
    // walk needed hours for the same population. Generous ceiling — this asserts the shape of the
    // fix, not a benchmark.
    assertTrue(elapsedMs < 20 * 60_000L,
        "full crawl should finish well inside the 45-min deadline, took " + elapsedMs + "ms");
  }

  /**
   * Delta mode: an {@code append} run with a recovered watermark must crawl only the tail of the
   * feed. This is the production-daily path, and it shares the cursor mechanism with the full load
   * — the watermark simply becomes the starting cursor.
   */
  @Test void watermarkDeltaCrawlsOnlyPulsesAfterTheWatermark() throws Exception {
    assumeTrue(apiKey != null && !apiKey.isEmpty(), "CYBER_OTX_API_KEY not set");

    // otxWriteMode is read from the model operand and defaults to "replace" (full load) when no
    // schema has been captured, so drive delta mode through the operand the launch script sets.
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("otxWriteMode", "append");
    org.apache.calcite.adapter.file.etl.ModelOperand.capture("cyber_threat", operand);
    try {
      String watermark = "2026-01-01T00:00:00";
      Map<String, String> variables = new HashMap<String, String>();
      variables.put("otxModifiedSince", watermark);

      String json = new OtxResponseTransformer().transform(null, context(variables));
      JsonNode rows = MAPPER.readTree(json);
      LOGGER.info("OTX delta crawl since {}: {} rows", watermark, rows.size());

      assertTrue(rows.size() > 0, "delta crawl returned nothing since " + watermark);
      assertTrue(rows.size() < 8000,
          "delta crawl should be bounded by the watermark, got " + rows.size() + " rows");

      Set<String> ids = new HashSet<String>();
      for (JsonNode row : rows) {
        assertTrue(ids.add(row.path("pulse_id").asText()), "duplicate pulse_id in delta crawl");
        // The cursor is a strict lower bound, so nothing at or before the watermark may appear.
        assertTrue(row.path("modified").asText().compareTo(watermark) > 0,
            "delta crawl returned a pulse modified at/before the watermark: "
                + row.path("modified").asText());
      }
      assertFalse(ids.isEmpty());
    } finally {
      org.apache.calcite.adapter.file.etl.ModelOperand.capture("cyber_threat",
          new HashMap<String, Object>());
    }
  }

  /**
   * A crawl killed mid-flight must resume from its checkpointed cursor, not restart. Seeds a
   * checkpoint for the full-load population but parked late in the feed, then asserts the crawl
   * picks up there — it returns only the tail rather than all 8.8k pulses.
   */
  @Test void crawlResumesFromCheckpointCursorRatherThanRestarting() throws Exception {
    assumeTrue(apiKey != null && !apiKey.isEmpty(), "CYBER_OTX_API_KEY not set");

    String cacheDir = System.getenv("GOVDATA_CACHE_DIR");
    assumeTrue(cacheDir != null && !cacheDir.isEmpty(), "GOVDATA_CACHE_DIR not set");

    // populationKey must match what a full load computes, or the checkpoint is correctly rejected.
    ObjectNode checkpoint = MAPPER.createObjectNode();
    checkpoint.put("populationKey",
        OTX_URL + "|since=1970-01-01T00:00:00|limit=50");
    checkpoint.put("cursor", "2026-01-01T00:00:00");
    checkpoint.put("pages", 160);
    checkpoint.set("rows", MAPPER.createArrayNode());

    Path dir = Paths.get(cacheDir, "cyber_threat");
    Files.createDirectories(dir);
    Path checkpointPath = dir.resolve("otx_pulses_full.checkpoint.json");
    Files.write(checkpointPath, MAPPER.writeValueAsBytes(checkpoint));

    try {
      JsonNode rows = MAPPER.readTree(new OtxResponseTransformer().transform(null, context(null)));
      LOGGER.info("OTX resumed crawl: {} rows", rows.size());

      // Resuming from 2026-01-01 must yield only the tail, not the whole feed.
      assertTrue(rows.size() > 0, "resumed crawl returned nothing");
      assertTrue(rows.size() < 3000,
          "crawl restarted from the beginning instead of resuming: " + rows.size() + " rows");
      for (JsonNode row : rows) {
        assertTrue(row.path("modified").asText().compareTo("2026-01-01T00:00:00") > 0,
            "resumed crawl re-fetched rows from before the checkpoint cursor");
      }
      // A completed crawl clears its checkpoint, so the next run starts clean.
      assertFalse(Files.exists(checkpointPath),
          "checkpoint should be deleted after a successful crawl");
    } finally {
      Files.deleteIfExists(checkpointPath);
    }
  }
}
