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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Transforms one page of a GitHub Security Advisories (GHSA) GraphQL response into flat
 * {@code vuln_cross_refs} rows — one row per GHSA-CVE pair. Only advisories carrying at least
 * one CVE identifier produce rows.
 *
 * <p>Pagination is driven entirely by the file adapter's built-in CURSOR pagination configured
 * in cyber-vuln-schema.yaml ({@code type: CURSOR, cursorIn: body, cursorParam: after,
 * cursorPath: data.securityAdvisories.pageInfo.endCursor,
 * hasNextPath: data.securityAdvisories.pageInfo.hasNextPage}). The framework templates the
 * {@code after} cursor (and the {@code updatedSince} delta bound) into the GraphQL POST body,
 * supplies the {@code Authorization} header, and reads the next cursor / has-next flag from the
 * raw envelope. This transformer is called once per page and only reshapes that page's rows.
 *
 * <p>The crawl is incremental: the schema injects the last committed {@code max(updatedAt)} as
 * {@code updatedSince}, so each run fetches only advisories changed since the prior snapshot.
 */
public class GithubSaResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GithubSaResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("GithubSA: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      JsonNode errors = root.path("errors");
      if (errors.isArray() && errors.size() > 0) {
        String msg = errors.get(0).path("message").asText("unknown error");
        // Fail loudly — a GraphQL error mid-crawl must not be cached or treated as end-of-data.
        throw new RuntimeException("GitHub GraphQL error: " + msg);
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream(256 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();
      int rows = writePage(root, gen);
      gen.writeEndArray();
      gen.close();

      LOGGER.debug("GithubSA: {} cross-ref rows from page", rows);
      return baos.toString(StandardCharsets.UTF_8.name());

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("GithubSA: failed to parse response: {}", e.getMessage());
      throw new RuntimeException("Failed to parse GitHub SA response: " + e.getMessage(), e);
    }
  }

  private int writePage(JsonNode root, JsonGenerator gen) throws Exception {
    JsonNode nodes = root.path("data").path("securityAdvisories").path("nodes");
    if (!nodes.isArray()) {
      return 0;
    }
    int count = 0;
    for (JsonNode node : nodes) {
      String ghsaId = textOrNull(node, "ghsaId");
      if (ghsaId == null) {
        continue;
      }
      String referenceUrl = extractFirstUrl(node.path("references"));

      // One row per CVE identifier on this advisory.
      for (JsonNode ident : node.path("identifiers")) {
        String type = textOrNull(ident, "type");
        String value = textOrNull(ident, "value");
        if ("CVE".equals(type) && value != null && value.startsWith("CVE-")) {
          ObjectNode row = MAPPER.createObjectNode();
          row.put("cve_id", value);
          row.put("external_id", ghsaId);
          row.put("external_source", "ghsa");
          row.put("url", referenceUrl);
          gen.writeTree(row);
          count++;
        }
      }
    }
    return count;
  }

  private String extractFirstUrl(JsonNode references) {
    if (!references.isArray() || references.size() == 0) {
      return null;
    }
    return textOrNull(references.get(0), "url");
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    String t = v.asText();
    return t.isEmpty() ? null : t;
  }
}
