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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JsonGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

/**
 * Transforms Feodo Tracker IP blocklist JSON into flat {@code ioc_ips} rows.
 *
 * <p>Input: JSON array from
 * {@code https://feodotracker.abuse.ch/downloads/ipblocklist.json}
 * <pre>
 * [
 *   {
 *     "ip_address": "1.2.3.4",
 *     "port": 8080,
 *     "status": "online",
 *     "hostname": null,
 *     "as_number": 12345,
 *     "as_name": "SOME-AS-NAME",
 *     "country": "US",
 *     "first_seen_online": "2024-01-15",
 *     "last_online": "2024-01-20",
 *     "malware": "Dridex"
 *   },
 *   ...
 * ]
 * </pre>
 *
 * <p>The {@code first_seen} partition column is taken from {@code first_seen_online},
 * which is already a date string. {@code source} is set to {@code "feodo"}.
 */
public class FeodoResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeodoResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.trim().isEmpty()) {
      LOGGER.warn("Feodo: empty response from {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      if (!root.isArray()) {
        LOGGER.warn("Feodo: expected JSON array at root, got {}", root.getNodeType());
        return "[]";
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream(64 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();

      int count = 0;
      for (JsonNode entry : root) {
        String ip = textOrNull(entry, "ip_address");
        if (ip == null) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("ip_address", ip);
        row.put("port", intOrNull(entry, "port"));
        row.put("status", textOrNull(entry, "status"));
        row.put("hostname", textOrNull(entry, "hostname"));
        row.put("as_number", intOrNull(entry, "as_number"));
        row.put("as_name", textOrNull(entry, "as_name"));
        row.put("country", textOrNull(entry, "country"));
        row.put("last_online", textOrNull(entry, "last_online"));
        row.put("malware_family", textOrNull(entry, "malware"));
        row.put("source", "feodo");

        String firstSeen = textOrNull(entry, "first_seen_online");
        row.put("first_seen", extractDate(firstSeen));

        gen.writeTree(row);
        count++;
      }

      gen.writeEndArray();
      gen.close();

      LOGGER.info("Feodo: returning {} ioc_ips rows", count);
      return baos.toString("UTF-8");

    } catch (Exception e) {
      LOGGER.error("Feodo: failed to parse response: {}", e.getMessage());
      throw new RuntimeException("Failed to parse Feodo response: " + e.getMessage(), e);
    }
  }

  /** Extracts the date portion (first 10 chars) from a date or datetime string. */
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
    if (v.isInt() || v.isLong()) {
      return v.asInt();
    }
    String t = v.asText().trim();
    if (t.isEmpty()) {
      return null;
    }
    try {
      return Integer.parseInt(t);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
