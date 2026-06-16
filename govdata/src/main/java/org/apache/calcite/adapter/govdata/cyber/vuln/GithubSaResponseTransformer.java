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
package org.apache.calcite.adapter.govdata.cyber.vuln;

import org.apache.calcite.adapter.file.etl.ModelOperand;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Transforms GitHub Security Advisories (GHSA) GraphQL cursor-paginated responses.
 *
 * <p>Requires a GitHub token with {@code public_repo} scope set via the
 * {@code CYBER_GITHUB_TOKEN} environment variable. Without a token, the GitHub
 * GraphQL API will reject all requests.
 *
 * <p>Produces rows for the {@code vuln_cross_refs} table: one row per GHSA-CVE pair.
 * Only advisories that have at least one associated CVE ID are emitted.
 *
 * <p>The first-page GraphQL response is passed as {@code response}; subsequent pages
 * are fetched directly via HTTP (cursor-based pagination).
 *
 * <p>GraphQL query targets {@code securityAdvisories} with fields:
 * {@code ghsaId}, {@code summary}, {@code publishedAt}, {@code severity},
 * {@code identifiers} (type+value), {@code references} (url).
 */
public class GithubSaResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GithubSaResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String GRAPHQL_URL = "https://api.github.com/graphql";
  private static final int PAGE_SIZE = 100;
  private static final int TIMEOUT_MS = 60_000;
  private static final long RATE_DELAY_MS = 1000L;

  @SuppressWarnings("InlineFormatString")
  private static final String QUERY_TEMPLATE =
      "{ \"query\": \"{ securityAdvisories(first: %d%s) { pageInfo { hasNextPage endCursor } "
          + "nodes { ghsaId publishedAt severity summary "
          + "identifiers { type value } "
          + "references { url } } } }\" }";

  @Override public String transform(String response, RequestContext context) {
    // Read the token from where it lives in the model: the vuln_cross_refs source's Authorization
    // header ("bearer ${CYBER_GITHUB_TOKEN:}", resolved at YAML load) — not System.getenv.
    String auth = ModelOperand.getString(
        "cyber_vuln.partitionedTables.vuln_cross_refs.source.headers.Authorization");
    String token = auth != null && auth.regionMatches(true, 0, "bearer ", 0, 7)
        ? auth.substring(7).trim() : (auth != null ? auth.trim() : null);
    if (token == null || token.isEmpty()) {
      // A required credential being absent is a hard failure, never a silent skip.
      throw new IllegalStateException("GithubSA: GitHub token is required but missing — set "
          + "CYBER_GITHUB_TOKEN (vuln_cross_refs source Authorization resolved empty).");
    }

    ArrayNode allRows = MAPPER.createArrayNode();

    try {
      // The HTTP source sends a POST with no body; GitHub rejects it. Ignore the initial
      // response entirely and self-fetch all pages using the GraphQL cursor API.
      String cursor = null;
      do {
        if (cursor != null) {
          sleepQuietly(RATE_DELAY_MS);
        }
        String pageResponse = fetchPage(token, cursor);
        if (pageResponse == null) {
          break;
        }
        cursor = processPage(pageResponse, allRows);
        if (allRows.size() % 1000 == 0 && allRows.size() > 0) {
          LOGGER.info("GithubSA: accumulated {} cross-refs", allRows.size());
        }
      } while (cursor != null);
    } catch (Exception e) {
      LOGGER.error("GithubSA: failed: {}", e.getMessage());
      throw new RuntimeException("Failed to process GitHub Security Advisories: " + e.getMessage(),
          e);
    }

    LOGGER.info("GithubSA: returning {} vuln_cross_refs rows", allRows.size());
    try {
      return MAPPER.writeValueAsString(allRows);
    } catch (Exception e) {
      LOGGER.error("GithubSA: failed to serialize rows: {}", e.getMessage());
      throw new RuntimeException("Failed to serialize GitHub SA rows", e);
    }
  }

  /**
   * Processes one page of GraphQL response and returns next cursor, or null if no more pages.
   */
  @SuppressWarnings("UnusedVariable")
  private String processPage(String pageResponse, ArrayNode accumulator) throws IOException {
    JsonNode root = MAPPER.readTree(pageResponse);

    // Check for GraphQL errors
    JsonNode errors = root.path("errors");
    if (errors.isArray() && errors.size() > 0) {
      String msg = errors.get(0).path("message").asText("unknown error");
      LOGGER.error("GithubSA: GraphQL error: {}", msg);
      throw new RuntimeException("GitHub GraphQL error: " + msg);
    }

    JsonNode advisories = root.path("data").path("securityAdvisories");
    JsonNode nodes = advisories.path("nodes");

    if (nodes.isArray()) {
      for (JsonNode node : nodes) {
        String ghsaId = textOrNull(node, "ghsaId");
        if (ghsaId == null) {
          continue;
        }

        String publishedAt = textOrNull(node, "publishedAt");
        String severity = textOrNull(node, "severity");
        String referenceUrl = extractFirstUrl(node.path("references"));

        // Emit one row per CVE identifier
        for (JsonNode ident : node.path("identifiers")) {
          String type = textOrNull(ident, "type");
          String value = textOrNull(ident, "value");
          if ("CVE".equals(type) && value != null && value.startsWith("CVE-")) {
            ObjectNode row = MAPPER.createObjectNode();
            row.put("cve_id", value);
            row.put("external_id", ghsaId);
            row.put("external_source", "ghsa");
            row.put("url", referenceUrl);
            accumulator.add(row);
          }
        }
      }
    }

    // Check for next page
    JsonNode pageInfo = advisories.path("pageInfo");
    boolean hasNextPage = pageInfo.path("hasNextPage").asBoolean(false);
    if (hasNextPage) {
      return textOrNull(pageInfo, "endCursor");
    }
    return null;
  }

  private String extractFirstUrl(JsonNode references) {
    if (!references.isArray() || references.size() == 0) {
      return null;
    }
    return textOrNull(references.get(0), "url");
  }

  private String fetchPage(String token, String cursor) {
    try {
      String afterClause = cursor != null ? (", after: \\\"" + cursor + "\\\"") : "";
      String body = String.format(QUERY_TEMPLATE, PAGE_SIZE, afterClause);

      HttpURLConnection conn =
          (HttpURLConnection) URI.create(GRAPHQL_URL).toURL().openConnection();
      conn.setRequestMethod("POST");
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setRequestProperty("Authorization", "bearer " + token);
      conn.setRequestProperty("Accept", "application/json");

      byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
      conn.setRequestProperty("Content-Length", String.valueOf(bodyBytes.length));
      try (OutputStream os = conn.getOutputStream()) {
        os.write(bodyBytes);
      }

      int status = conn.getResponseCode();
      if (status == 401 || status == 403) {
        LOGGER.error("GithubSA: auth failure HTTP {} — check CYBER_GITHUB_TOKEN", status);
        return null;
      }
      if (status != 200) {
        LOGGER.warn("GithubSA: HTTP {} fetching cursor={}", status, cursor);
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
      LOGGER.warn("GithubSA: error fetching cursor={}: {}", cursor, e.getMessage());
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

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
