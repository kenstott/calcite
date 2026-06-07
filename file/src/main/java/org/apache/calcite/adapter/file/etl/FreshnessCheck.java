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
package org.apache.calcite.adapter.file.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;

/**
 * Pure freshness-token logic: extracts the comparable token for a
 * {@link FreshnessConfig} from a probe (HTTP headers, a small probe body, or the
 * downloaded content), and decides whether the source changed versus the
 * tracker's last-seen token. The actual HTTP probing and the pipeline skip-gate
 * are wired separately; this class holds the testable decision logic.
 */
final class FreshnessCheck {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private FreshnessCheck() {
  }

  /**
   * Extracts the freshness token for {@code config} from the available inputs.
   *
   * @param config     the freshness configuration (type + options)
   * @param headers    response/HEAD headers (case-insensitive lookup), or null
   * @param probeBody  small metadata/probe-response body for VERSION/COUNT/CHECKSUM-sidecar, or null
   * @param content    the downloaded body for HASH, or null
   * @return the token string, or null if it could not be determined
   */
  static String token(FreshnessConfig config, Map<String, String> headers,
      String probeBody, byte[] content) {
    if (config == null || config.getType() == null) {
      return null;
    }
    switch (config.getType()) {
    case ETAG:
      return header(headers, "ETag");
    case LAST_MODIFIED:
      return header(headers, "Last-Modified");
    case SIZE:
      return header(headers, "Content-Length");
    case VERSION:
      return jsonValue(probeBody, config.getVersionField());
    case CHECKSUM:
      if (config.isObjectMetadata()) {
        String md5 = header(headers, "Content-MD5");
        if (md5 == null) {
          md5 = header(headers, "x-goog-hash");
        }
        return md5 != null ? md5 : header(headers, "ETag");
      }
      return probeBody == null ? null : firstTokenOf(probeBody);
    case COUNT:
      return jsonValue(probeBody, config.getCountPath());
    case GRAPHQL:
      return jsonValue(probeBody, config.getQueryPath());
    case HASH:
      return content == null ? null : sha256Hex(content);
    default:
      return null;
    }
  }

  /**
   * True if the source should be (re)processed: when either token is null (can't
   * prove unchanged, or first run) or the tokens differ.
   */
  static boolean changed(String previous, String current) {
    if (previous == null || current == null) {
      return true;
    }
    return !previous.equals(current);
  }

  private static String header(Map<String, String> headers, String name) {
    if (headers == null) {
      return null;
    }
    for (Map.Entry<String, String> e : headers.entrySet()) {
      if (e.getKey() != null && e.getKey().equalsIgnoreCase(name)) {
        return e.getValue();
      }
    }
    return null;
  }

  /** First whitespace-delimited token of a sidecar digest line (e.g. {@code "<sha>  file"}). */
  private static String firstTokenOf(String body) {
    String trimmed = body.trim();
    int sp = indexOfWhitespace(trimmed);
    return sp < 0 ? trimmed : trimmed.substring(0, sp);
  }

  private static int indexOfWhitespace(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (Character.isWhitespace(s.charAt(i))) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Reads a value from a JSON body by a simple dot path. Accepts a leading
   * {@code $.} or {@code $}; e.g. {@code $.totalResults}, {@code meta.version}.
   * Array indices are supported as a {@code [n]} suffix on a segment, e.g.
   * {@code data.securityAdvisories.nodes[0].updatedAt}.
   */
  private static String jsonValue(String body, String path) {
    if (body == null || path == null || path.isEmpty()) {
      return null;
    }
    String p = path.trim();
    if (p.startsWith("$.")) {
      p = p.substring(2);
    } else if (p.startsWith("$")) {
      p = p.substring(1);
    }
    try {
      JsonNode node = MAPPER.readTree(body);
      for (String segment : p.split("\\.")) {
        if (segment.isEmpty() || node == null) {
          continue;
        }
        node = step(node, segment);
      }
      return node == null || node.isNull() ? null : node.asText();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Resolves one path segment, which may carry trailing array indices, e.g.
   * {@code nodes[0]} or {@code matrix[1][2]}.
   */
  private static JsonNode step(JsonNode node, String segment) {
    int bracket = segment.indexOf('[');
    String field = bracket < 0 ? segment : segment.substring(0, bracket);
    JsonNode current = field.isEmpty() ? node : node.get(field);
    if (bracket < 0 || current == null) {
      return current;
    }
    String rest = segment.substring(bracket);
    while (!rest.isEmpty() && current != null) {
      int close = rest.indexOf(']');
      if (rest.charAt(0) != '[' || close < 0) {
        return null;
      }
      try {
        current = current.get(Integer.parseInt(rest.substring(1, close).trim()));
      } catch (NumberFormatException e) {
        return null;
      }
      rest = rest.substring(close + 1);
    }
    return current;
  }

  private static String sha256Hex(byte[] content) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(content);
      StringBuilder sb = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        sb.append(Character.forDigit((b >> 4) & 0xf, 16));
        sb.append(Character.forDigit(b & 0xf, 16));
      }
      return sb.toString();
    } catch (Exception e) {
      return null;
    }
  }

  /** Convenience used by tests / callers that have a string body to hash. */
  static String sha256Hex(String content) {
    return content == null ? null : sha256Hex(content.getBytes(StandardCharsets.UTF_8));
  }
}
