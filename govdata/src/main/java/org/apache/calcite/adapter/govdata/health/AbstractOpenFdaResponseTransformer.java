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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Thin base class for openFDA response transformers.
 *
 * <p>Two input modes, both producing the same flattened output:
 * <ul>
 *   <li><b>Query API</b> — HttpSource handles pagination and passes one page's JSON at a
 *       time: {@code {"meta": {...}, "results": [...]}}.</li>
 *   <li><b>Bulk</b> — when the source URL ends in {@code .zip} (openFDA's unthrottled
 *       {@code download.open.fda.gov} partition files), the transformer downloads and
 *       unzips the file itself and streams its {@code results[]} array. The HTTP pipeline's
 *       own (TEXT-mode) fetch of the binary ZIP is ignored. No API key, no pagination cap.</li>
 * </ul>
 *
 * <p>Subclasses implement {@link #flattenRecord} to map one {@code results[i]} object
 * to the output row shape expected by the schema — identical for both modes.
 */
abstract class AbstractOpenFdaResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOpenFdaResponseTransformer.class);
  static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    String url = context != null ? context.getUrl() : null;
    try {
      if (url != null && url.toLowerCase().endsWith(".zip")) {
        return transformBulkZip(url);
      }
      JsonNode root = MAPPER.readTree(response);
      if (root.has("error")) {
        String msg = root.path("error").asText();
        LOGGER.warn("openFDA API error for {}: {}", getClass().getSimpleName(), msg);
        return "[]";
      }

      JsonNode results = root.path("results");
      if (!results.isArray() || results.size() == 0) {
        return "[]";
      }

      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode record : results) {
        flattenInto(record, out);
      }
      return out.toString();

    } catch (Exception e) {
      LOGGER.error("{} transform failed: {}", getClass().getSimpleName(), e.getMessage());
      return "[]";
    }
  }

  /**
   * Downloads an openFDA bulk partition ZIP, then streams the single JSON entry's
   * {@code results[]} array through {@link #flattenRecord}. Streaming the input avoids
   * materializing the (often large) raw JSON; only the flattened output is held.
   */
  private String transformBulkZip(String url) throws IOException {
    // Stream the (often large) partition ZIP to a temp file rather than buffering it in a byte[],
    // then stream the single JSON entry's results[] through the parser. Only the flattened output
    // is held in memory (bounded by the responseTransformer's String return contract).
    File tempZip = File.createTempFile("openfda-", ".zip");
    try {
      downloadToFile(url, tempZip);
      ArrayNode out = MAPPER.createArrayNode();
      try (InputStream fin = new BufferedInputStream(new FileInputStream(tempZip), 65536);
           ZipInputStream zis = new ZipInputStream(fin)) {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          if (!entry.getName().toLowerCase().endsWith(".json")) {
            zis.closeEntry();
            continue;
          }
          JsonParser parser = MAPPER.getFactory().createParser(zis);
          try {
            streamResultsArray(parser, out);
          } finally {
            parser.close();
          }
          return out.toString();
        }
      }
      LOGGER.warn("{} bulk: no .json entry in ZIP from {}", getClass().getSimpleName(), url);
      return "[]";
    } finally {
      tempZip.delete();
    }
  }

  /**
   * Advances the parser to the top-level {@code results} ARRAY and flattens each element.
   * openFDA also has a {@code meta.results} OBJECT ({@code {skip,limit,total}}) that appears
   * first in the stream, so a {@code results} field that is not an array is skipped and
   * scanning continues until the real array is found.
   */
  private void streamResultsArray(JsonParser parser, ArrayNode out) throws IOException {
    JsonToken token;
    while ((token = parser.nextToken()) != null) {
      if (token == JsonToken.FIELD_NAME && "results".equals(parser.currentName())) {
        if (parser.nextToken() == JsonToken.START_ARRAY) {
          while (parser.nextToken() != JsonToken.END_ARRAY) {
            JsonNode record = MAPPER.readTree(parser);
            flattenInto(record, out);
          }
          return;
        }
        // Not the array (e.g. meta.results object) — skip its value and keep scanning.
        parser.skipChildren();
      }
    }
  }

  private void flattenInto(JsonNode record, ArrayNode out) {
    try {
      ObjectNode row = MAPPER.createObjectNode();
      flattenRecord(record, row);
      out.add(row);
    } catch (Exception e) {
      LOGGER.debug("Skipping malformed {} record: {}", getClass().getSimpleName(), e.getMessage());
    }
  }

  private static void downloadToFile(String url, File dest) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream();
         OutputStream os = new BufferedOutputStream(new FileOutputStream(dest), 65536)) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        os.write(buf, 0, len);
      }
    }
  }

  /** Map one results[] entry into the flat output row. */
  protected abstract void flattenRecord(JsonNode record, ObjectNode row);

  /** Get first string element of a JSON array node, or null. */
  protected static String firstText(JsonNode node, String fieldName) {
    JsonNode arr = node.path(fieldName);
    if (arr.isArray() && arr.size() > 0) {
      return nullIfEmpty(arr.get(0).asText(null));
    }
    return nullIfEmpty(arr.asText(null));
  }

  /** Get nested first-array-element: node.path(parent).path(child)[0]. */
  protected static String nestedFirstText(JsonNode node, String parent, String child) {
    JsonNode parentNode = node.path(parent);
    if (parentNode.isArray() && parentNode.size() > 0) {
      parentNode = parentNode.get(0);
    }
    return firstText(parentNode, child);
  }

  /** Join all string elements of a JSON array with commas. */
  protected static String joinArray(JsonNode node, String fieldName) {
    JsonNode arr = node.path(fieldName);
    if (!arr.isArray() || arr.size() == 0) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (JsonNode item : arr) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(item.asText(""));
    }
    String result = sb.toString().trim();
    return result.isEmpty() ? null : result;
  }

  /** Put a string field, omitting null/blank values. */
  protected static void put(ObjectNode row, String key, String value) {
    if (value != null && !value.isEmpty()) {
      row.put(key, value);
    } else {
      row.putNull(key);
    }
  }

  protected static String text(JsonNode node, String fieldName) {
    return nullIfEmpty(node.path(fieldName).asText(null));
  }

  private static String nullIfEmpty(String s) {
    if (s == null || s.isEmpty() || "null".equals(s)) {
      return null;
    }
    return s;
  }
}
