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
package org.apache.calcite.adapter.govdata.fedregister;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Transforms Federal Register document API responses into flat rows.
 *
 * <p>The Federal Register API is paginated (max 1000 results/page). This transformer
 * handles pagination by fetching all subsequent pages using the {@code next_page_url}
 * field returned in each response, accumulating all results before returning.
 *
 * <p>Input: first-page JSON from
 * {@code https://api.federalregister.gov/v1/documents.json?...&page=1}
 * <pre>
 * {
 *   "count": 85234,
 *   "total_pages": 86,
 *   "next_page_url": "https://...&page=2",
 *   "results": [{document}, ...]
 * }
 * </pre>
 *
 * <p>Output: flat JSON array of normalized document rows suitable for parquet materialization.
 *
 * <p>Complex fields (agencies, cfr_references, docket_ids) are stored as JSON strings
 * for flexibility. Agency names/slugs are also denormalized to plain comma-separated
 * strings for simpler SQL filtering without json_extract.
 */
public class FedRegisterDocumentsTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FedRegisterDocumentsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Max pages to fetch per dimension (safety cap: 1000 pages × 1000/page = 1M docs). */
  private static final int MAX_PAGES = 1000;

  /** Connect/read timeout for pagination HTTP calls (ms). */
  private static final int TIMEOUT_MS = 30_000;

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("FedRegister Documents: empty response for {}", context.getUrl());
      return "[]";
    }

    String docType = context.getDimensionValues().get("doc_type");
    String year = context.getDimensionValues().get("year");

    try {
      JsonNode root = MAPPER.readTree(response);

      int totalPages = root.path("total_pages").asInt(1);
      int totalCount = root.path("count").asInt(0);

      LOGGER.info("FedRegister Documents: doc_type={} year={} total={} pages={}",
          docType, year, totalCount, totalPages);

      ArrayNode allResults = MAPPER.createArrayNode();

      // Collect first page results
      addResults(root, allResults, docType);

      // Fetch remaining pages via next_page_url
      String nextPageUrl = getTextOrNull(root, "next_page_url");
      int pagesLoaded = 1;

      while (nextPageUrl != null && !nextPageUrl.isEmpty() && pagesLoaded < MAX_PAGES) {
        String pageResponse = fetchUrl(nextPageUrl);
        if (pageResponse == null) {
          LOGGER.warn("FedRegister Documents: failed to fetch page {}, stopping pagination",
              pagesLoaded + 1);
          break;
        }

        JsonNode pageRoot = MAPPER.readTree(pageResponse);
        addResults(pageRoot, allResults, docType);
        nextPageUrl = getTextOrNull(pageRoot, "next_page_url");
        pagesLoaded++;

        if (pagesLoaded % 10 == 0) {
          LOGGER.debug("FedRegister Documents: doc_type={} year={} loaded {} pages, {} rows so far",
              docType, year, pagesLoaded, allResults.size());
        }
      }

      LOGGER.info("FedRegister Documents: doc_type={} year={} — {} rows from {} pages",
          docType, year, allResults.size(), pagesLoaded);

      return allResults.toString();

    } catch (Exception e) {
      LOGGER.error("FedRegister Documents: failed to parse response for doc_type={} year={}: {}",
          docType, year, e.getMessage());
      return "[]";
    }
  }

  /**
   * Extracts and normalizes document records from a single page response,
   * appending them to the accumulator array.
   */
  private void addResults(JsonNode pageRoot, ArrayNode accumulator, String docType) {
    JsonNode results = pageRoot.path("results");
    if (!results.isArray()) {
      return;
    }

    for (JsonNode doc : results) {
      ObjectNode row = MAPPER.createObjectNode();

      row.put("document_number", getTextOrNull(doc, "document_number"));
      row.put("title", getTextOrNull(doc, "title"));
      row.put("doc_type", docType);
      row.put("abstract", getTextOrNull(doc, "abstract"));
      row.put("publication_date", getTextOrNull(doc, "publication_date"));
      row.put("effective_on", getTextOrNull(doc, "effective_on"));
      row.put("action", getTextOrNull(doc, "action"));

      // Agencies — denormalize to comma strings and preserve full JSON
      extractAgencies(doc, row);

      // CFR references — preserve as JSON string
      JsonNode cfrRefs = doc.path("cfr_references");
      row.put("cfr_references", cfrRefs.isArray() ? cfrRefs.toString() : null);

      // RIN — take first entry if multiple
      JsonNode rins = doc.path("regulation_id_numbers");
      if (rins.isArray() && rins.size() > 0) {
        row.put("rin", rins.get(0).asText());
      } else {
        row.putNull("rin");
      }

      // Significant flag (OIRA economically significant)
      JsonNode sig = doc.get("significant");
      if (sig != null && sig.isBoolean()) {
        row.put("significant", sig.booleanValue());
      } else {
        row.putNull("significant");
      }

      // Docket IDs — preserve as JSON string
      JsonNode docketIds = doc.path("docket_ids");
      row.put("docket_ids", docketIds.isArray() ? docketIds.toString() : null);

      // Presidential document fields
      row.put("president", getTextOrNull(doc, "president"));
      row.put("signing_date", getTextOrNull(doc, "signing_date"));

      // URLs
      row.put("full_text_xml_url", getTextOrNull(doc, "full_text_xml_url"));
      row.put("body_html_url", getTextOrNull(doc, "body_html_url"));

      accumulator.add(row);
    }
  }

  /**
   * Extracts agency names and slugs from the nested agencies array,
   * producing both comma-separated strings (for easy SQL filtering) and
   * a full JSON representation for detailed queries.
   */
  private void extractAgencies(JsonNode doc, ObjectNode row) {
    JsonNode agencies = doc.path("agencies");
    if (!agencies.isArray() || agencies.size() == 0) {
      row.putNull("agency_names");
      row.putNull("agency_slugs");
      return;
    }

    List<String> names = new ArrayList<String>();
    List<String> slugs = new ArrayList<String>();

    for (JsonNode agency : agencies) {
      String name = getTextOrNull(agency, "name");
      String slug = getTextOrNull(agency, "slug");
      if (name != null) {
        names.add(name);
      }
      if (slug != null) {
        slugs.add(slug);
      }
    }

    row.put("agency_names", names.isEmpty() ? null : join(names, ", "));
    row.put("agency_slugs", slugs.isEmpty() ? null : join(slugs, ","));
  }

  /**
   * Fetches a URL and returns the response body as a string.
   * Uses a simple HttpURLConnection with timeout to avoid heavy dependencies.
   * Returns null on any error.
   */
  private String fetchUrl(String urlString) {
    try {
      HttpURLConnection conn = (HttpURLConnection) URI.create(urlString).toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "govdata-calcite-adapter/1.0");
      conn.setRequestProperty("Accept", "application/json");

      int status = conn.getResponseCode();
      if (status == 429) {
        // Rate limited — wait 2 seconds and retry once
        LOGGER.warn("FedRegister Documents: rate limited (429), waiting 2s before retry");
        Thread.sleep(2000);
        conn.disconnect();
        conn = (HttpURLConnection) URI.create(urlString).toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(TIMEOUT_MS);
        conn.setReadTimeout(TIMEOUT_MS);
        conn.setRequestProperty("User-Agent", "govdata-calcite-adapter/1.0");
        conn.setRequestProperty("Accept", "application/json");
        status = conn.getResponseCode();
      }

      if (status != 200) {
        LOGGER.warn("FedRegister Documents: HTTP {} for {}", status, urlString);
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

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("FedRegister Documents: interrupted while fetching {}", urlString);
      return null;
    } catch (Exception e) {
      LOGGER.warn("FedRegister Documents: error fetching {}: {}", urlString, e.getMessage());
      return null;
    }
  }

  private static String getTextOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull() || value.isMissingNode()) {
      return null;
    }
    String text = value.asText();
    return text.isEmpty() ? null : text;
  }

  private static String join(List<String> items, String delimiter) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      sb.append(items.get(i));
    }
    return sb.toString();
  }
}
