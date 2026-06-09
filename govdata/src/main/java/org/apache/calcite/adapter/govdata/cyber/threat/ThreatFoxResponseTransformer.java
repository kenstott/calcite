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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Transforms ThreatFox IOC API POST responses into flat {@code ioc_mixed} rows.
 *
 * <p>ThreatFox requires an {@code Auth-Key} header even for free-tier access.
 * The key is set via {@code CYBER_THREATFOX_API_KEY} environment variable.
 * Without it the table is disabled via the schema {@code enabled} field.
 *
 * <p>Input: JSON object with {@code data} array:
 * <pre>
 * {
 *   "query_status": "ok",
 *   "data": [
 *     {
 *       "id": "123456",
 *       "ioc": "1.2.3.4:8080",
 *       "ioc_type": "ip:port",
 *       "threat_type": "botnet_cc",
 *       "fk_malware": "win.dridex",
 *       "malware_printable": "Dridex",
 *       "malware_alias": "Bugat,Cridex",
 *       "first_seen": "2024-01-15 08:30:00 UTC",
 *       "last_seen": "2024-01-20 12:00:00 UTC",
 *       "confidence_level": 75,
 *       "is_compromised": false,
 *       "reference": "https://example.com",
 *       "tags": ["emotet", "botnet"],
 *       "reporter": "user123",
 *       "anonymous": false
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>{@code malware_alias} is a comma-separated string → pipe-delimited in output.
 * {@code tags} is a JSON array → pipe-delimited in output.
 * {@code first_seen} datetime → date portion for the partition column.
 */
public class ThreatFoxResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThreatFoxResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.trim().isEmpty()) {
      LOGGER.warn("ThreatFox: empty response from {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      String queryStatus = root.path("query_status").asText("");
      if ("no_results".equals(queryStatus)) {
        LOGGER.info("ThreatFox: no results for query");
        return "[]";
      }
      if (root.has("error")) {
        LOGGER.error("ThreatFox: API error: {}", root.path("error").asText());
        return "[]";
      }

      JsonNode data = root.path("data");
      if (!data.isArray()) {
        LOGGER.warn("ThreatFox: expected data array, got {}", data.getNodeType());
        return "[]";
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream(512 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();

      int count = 0;
      for (JsonNode entry : data) {
        String iocValue = textOrNull(entry, "ioc");
        if (iocValue == null) {
          continue;
        }

        String firstSeenRaw = textOrNull(entry, "first_seen");
        ObjectNode row = MAPPER.createObjectNode();
        row.put("ioc_id", textOrNull(entry, "id"));
        row.put("ioc_value", iocValue);
        row.put("ioc_type", textOrNull(entry, "ioc_type"));
        row.put("threat_type", textOrNull(entry, "threat_type"));
        row.put("malware_key", textOrNull(entry, "malware"));
        row.put("malware_printable", textOrNull(entry, "malware_printable"));
        row.put("malware_aliases", commaToPipe(textOrNull(entry, "malware_alias")));
        row.put("first_seen_utc", firstSeenRaw);
        row.put("last_seen_utc", textOrNull(entry, "last_seen"));
        row.put("confidence_level", intOrNull(entry, "confidence_level"));
        row.put("is_compromised", boolOrNull(entry, "is_compromised"));
        row.put("reference", textOrNull(entry, "reference"));
        row.put("tags", joinArrayNode(entry.path("tags")));
        row.put("reporter", textOrNull(entry, "reporter"));
        row.put("anonymous", boolOrNull(entry, "anonymous"));
        row.put("source", "threatfox");
        row.put("first_seen", extractDate(firstSeenRaw));

        gen.writeTree(row);
        count++;
      }

      gen.writeEndArray();
      gen.close();

      LOGGER.info("ThreatFox: returning {} ioc_mixed rows", count);
      return baos.toString("UTF-8");

    } catch (Exception e) {
      LOGGER.error("ThreatFox: failed to parse response: {}", e.getMessage());
      throw new RuntimeException("Failed to parse ThreatFox response: " + e.getMessage(), e);
    }
  }

  /** Converts comma-separated aliases string to pipe-delimited, normalizing whitespace. */
  private static String commaToPipe(String csv) {
    if (csv == null || csv.trim().isEmpty()) {
      return null;
    }
    String[] parts = csv.split(",");
    List<String> items = new ArrayList<String>();
    for (String part : parts) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        items.add(trimmed);
      }
    }
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

  private static String joinArrayNode(JsonNode arr) {
    if (!arr.isArray() || arr.size() == 0) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (JsonNode item : arr) {
      String val = item.asText(null);
      if (val != null && !val.isEmpty()) {
        if (sb.length() > 0) {
          sb.append("|");
        }
        sb.append(val);
      }
    }
    return sb.length() > 0 ? sb.toString() : null;
  }

  /** Extracts the date portion from a datetime string like "2024-01-15 08:30:00 UTC". */
  private static String extractDate(String datetime) {
    if (datetime == null || datetime.length() < 10) {
      return datetime;
    }
    return datetime.substring(0, 10);
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

  private static Boolean boolOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    return v.asBoolean();
  }
}
