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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
 *       "malware_families": [{"id": "...", "display_name": "Emotet"}],
 *       "attack_ids": [{"id": "T1566", "display_name": "Phishing"}],
 *       "indicator_count": 42,
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

  private static final int TIMEOUT_MS = 60_000;
  private static final long RATE_DELAY_MS = 500L;

  @Override public String transform(String response, RequestContext context) {
    String apiKey = System.getenv("CYBER_OTX_API_KEY");
    if (apiKey == null || apiKey.trim().isEmpty()) {
      LOGGER.warn("OTX: CYBER_OTX_API_KEY not set; skipping OTX pulse fetch");
      return "[]";
    }

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(4 * 1024 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();

      int[] count = {0};

      // Apply delta filter if CYBER_OTX_DELTA_DAYS is set (e.g., "1" for daily incremental)
      String baseUrl = context.getUrl();
      String deltaDaysEnv = System.getenv("CYBER_OTX_DELTA_DAYS");
      if (deltaDaysEnv != null && !deltaDaysEnv.trim().isEmpty()) {
        try {
          int deltaDays = Integer.parseInt(deltaDaysEnv.trim());
          if (deltaDays > 0) {
            String since = LocalDateTime.now(java.time.ZoneOffset.UTC).minusDays(deltaDays)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
            baseUrl = baseUrl + (baseUrl.contains("?") ? "&" : "?") + "modified_since=" + since;
            LOGGER.info("OTX: delta mode — modified_since={} ({} days)", since, deltaDays);
          }
        } catch (NumberFormatException e) {
          LOGGER.warn("OTX: invalid CYBER_OTX_DELTA_DAYS '{}', using full load", deltaDaysEnv);
        }
      } else {
        LOGGER.info("OTX: full load mode (CYBER_OTX_DELTA_DAYS not set)");
      }

      // First page: use the response if provided; otherwise fetch the (possibly modified) base URL
      String firstPage = (response != null && !response.trim().isEmpty())
          ? response : fetchPage(baseUrl, apiKey);

      if (firstPage == null) {
        gen.writeEndArray();
        gen.close();
        return baos.toString("UTF-8");
      }

      String nextUrl = processPage(firstPage, gen, count);

      while (nextUrl != null) {
        sleepQuietly(RATE_DELAY_MS);
        String page = fetchPage(nextUrl, apiKey);
        if (page == null) {
          LOGGER.warn("OTX: pagination stopped at {}", nextUrl);
          break;
        }
        nextUrl = processPage(page, gen, count);

        if (count[0] % 1000 == 0 && count[0] > 0) {
          LOGGER.info("OTX: accumulated {} pulse rows so far", count[0]);
        }
      }

      gen.writeEndArray();
      gen.close();

      LOGGER.info("OTX: returning {} threat_pulses rows", count[0]);
      return baos.toString("UTF-8");

    } catch (Exception e) {
      LOGGER.error("OTX: failed: {}", e.getMessage());
      throw new RuntimeException("Failed to process OTX pulses: " + e.getMessage(), e);
    }
  }

  /**
   * Processes one page of results. Returns the {@code next} URL, or null if no more pages.
   */
  private String processPage(String pageJson, JsonGenerator gen, int[] count) throws Exception {
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
      row.put("malware_families", joinDisplayNames(pulse.path("malware_families")));
      row.put("attack_ids", joinAttackIds(pulse.path("attack_ids")));
      row.put("ioc_count", intOrNull(pulse, "indicator_count"));
      row.put("created", created);
      row.put("modified", textOrNull(pulse, "modified"));
      row.put("tlp", textOrNull(pulse, "tlp"));
      row.put("source", "otx");
      row.put("first_seen", extractDate(created));

      gen.writeTree(row);
      count[0]++;
    }

    String next = textOrNull(root, "next");
    return (next != null && next.startsWith("http")) ? next : null;
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

  /** Joins {@code display_name} fields from an array of objects. */
  private static String joinDisplayNames(JsonNode arr) {
    if (!arr.isArray() || arr.size() == 0) {
      return null;
    }
    List<String> names = new ArrayList<String>();
    for (JsonNode item : arr) {
      String name = textOrNull(item, "display_name");
      if (name != null) {
        names.add(name);
      }
    }
    return joinList(names);
  }

  /** Joins ATT&CK technique IDs (the {@code id} field, e.g., "T1566") from array. */
  private static String joinAttackIds(JsonNode arr) {
    if (!arr.isArray() || arr.size() == 0) {
      return null;
    }
    List<String> ids = new ArrayList<String>();
    for (JsonNode item : arr) {
      String id = textOrNull(item, "id");
      if (id != null) {
        ids.add(id);
      }
    }
    return joinList(ids);
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

  private String fetchPage(String url, String apiKey) {
    try {
      HttpURLConnection conn =
          (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("X-OTX-API-KEY", apiKey);
      conn.setRequestProperty("Accept", "application/json");

      int status = conn.getResponseCode();
      if (status == 401 || status == 403) {
        LOGGER.error("OTX: auth failure HTTP {} — check CYBER_OTX_API_KEY", status);
        return null;
      }
      if (status == 429) {
        LOGGER.warn("OTX: rate limited, sleeping 60s");
        sleepQuietly(60_000L);
        conn.disconnect();
        return fetchPage(url, apiKey);
      }
      if (status != 200) {
        LOGGER.warn("OTX: HTTP {} fetching {}", status, url);
        return null;
      }

      BufferedReader reader = new BufferedReader(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      reader.close();
      conn.disconnect();
      return sb.toString();

    } catch (Exception e) {
      LOGGER.warn("OTX: error fetching {}: {}", url, e.getMessage());
      return null;
    }
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    String t = v.asText();
    return t.isEmpty() ? null : t;
  }

  private static Integer intOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    return v.asInt();
  }

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
