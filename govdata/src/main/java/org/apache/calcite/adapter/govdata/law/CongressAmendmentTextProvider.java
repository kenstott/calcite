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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.etl.SharedHttpClient;
import org.apache.calcite.adapter.file.etl.StorageAwareDataProvider;
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.GovDataException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Text versions of Senate and House amendments, from the Congress.gov API.
 *
 * <p>The GovInfo Bill Status XML behind {@code bill_amendments} carries only a count of an
 * amendment's text versions. The Congress.gov API serves the versions themselves at
 * {@code /v3/amendment/{congress}/{type}/{number}/text}: a version type (Submitted, Offered,
 * Agreed To, ...), a date and the Congressional Record HTML and PDF URLs of the text. One row per
 * (amendment, version, format).
 *
 * <p>A crawl for one Congress lists its amendments of each type
 * ({@code /v3/amendment/{congress}/samdt} and {@code .../hamdt}, 250 per page, each with an
 * {@code updateDate}) and reads the text endpoint of each. Each text response is cached under the
 * amendment's number and {@code updateDate}, so a daily refresh of the current Congress re-reads
 * only amendments the source changed, and a completed Congress is read once.
 *
 * <p>Failures are not swallowed: an HTTP error that outlasts the retries, a listing item without a
 * number, or a response without {@code textVersions} fails the crawl. An amendment whose
 * {@code textVersions} is an empty array has no text yet and yields no rows.
 */
public class CongressAmendmentTextProvider implements StorageAwareDataProvider {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Page size of the amendment listing; 250 is the API maximum. */
  static final int PAGE_SIZE = 250;

  /** The amendment types the API lists, as URL slugs. */
  private static final String[] TYPE_SLUGS = {"samdt", "hamdt"};

  private StorageProvider storageProvider;
  private String cacheBaseDir;

  @Override public void setStorageProvider(StorageProvider sp, String cacheDir) {
    this.storageProvider = sp;
    this.cacheBaseDir = cacheDir;
  }

  private StorageProvider storageProvider() {
    if (storageProvider == null) {
      storageProvider = StorageProviderFactory.createForGovDataCache();
      cacheBaseDir = StorageProviderFactory.getGovDataCacheDir();
    }
    return storageProvider;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    HttpSourceConfig source = config.getSource();
    String congress = required(variables, "congress");
    required(variables, "refresh_date");
    String baseUrl = VariableResolver.substitute(source.getUrl(), variables);
    requireResolved("url", baseUrl);
    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("Accept", "application/json");
    if (source.getHeaders() != null) {
      for (Map.Entry<String, String> e : source.getHeaders().entrySet()) {
        String value = VariableResolver.substitute(e.getValue(), variables);
        requireResolved("header " + e.getKey(), value);
        headers.put(e.getKey(), value);
      }
    }
    // dq_record_limit is this provider's own knob, not an API parameter: in a DQ run it caps the
    // amendments read per type, so a DQ crawl does not read a whole Congress.
    int recordLimit = 0;
    String limit = source.getParameters() == null ? null
        : source.getParameters().get("dq_record_limit");
    if (limit != null && isDqSampleMode()) {
      try {
        recordLimit = Integer.parseInt(limit.trim());
      } catch (NumberFormatException e) {
        throw new GovDataException("CongressAmendmentTextProvider: dq_record_limit must be an "
            + "integer but is '" + limit + "'", e);
      }
    }
    return new RowIterator(baseUrl, congress, headers, source.getRateLimit(), storageProvider(),
        cacheBaseDir, recordLimit);
  }

  /** True when DQ sample mode is active; GOVDATA_DQ is an allowed global run flag. */
  private static boolean isDqSampleMode() {
    String v = System.getProperty("GOVDATA_DQ");
    if (v == null) {
      v = System.getenv("GOVDATA_DQ");
    }
    return "true".equalsIgnoreCase(v);
  }

  /**
   * The rows of one amendment's {@code /text} response; for tests.
   *
   * @param congress the Congress number
   * @param type the amendment type slug, {@code samdt} or {@code hamdt}
   * @param number the amendment number
   * @param response the parsed response body
   */
  static List<Map<String, Object>> rowsOf(String congress, String type, String number,
      JsonNode response) {
    JsonNode versions = response.get("textVersions");
    if (versions == null || !versions.isArray()) {
      throw new GovDataException("Amendment " + congress + "/" + type + "/" + number
          + " text response has no textVersions array");
    }
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (JsonNode version : versions) {
      JsonNode formats = version.get("formats");
      if (formats == null || !formats.isArray()) {
        throw new GovDataException("Amendment " + congress + "/" + type + "/" + number
            + " text version has no formats array");
      }
      for (JsonNode format : formats) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("congress", Integer.valueOf(congress));
        row.put("amendment_type", type.toUpperCase(Locale.ROOT));
        row.put("amendment_number", Integer.valueOf(number));
        row.put("version_type", text(version, "type"));
        row.put("version_date", text(version, "date"));
        row.put("format_type", text(format, "type"));
        row.put("url", text(format, "url"));
        rows.add(row);
      }
    }
    return rows;
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return v == null || v.isNull() ? null : v.asText();
  }

  private static String required(Map<String, String> variables, String name) {
    String v = variables.get(name);
    if (v == null || v.isEmpty()) {
      throw new GovDataException("CongressAmendmentTextProvider: dimension '" + name
          + "' is required but was not supplied");
    }
    return v;
  }

  /** An unset {@code ${VAR}} is left in place by the resolver; the API would answer 403. */
  private static void requireResolved(String what, String value) {
    if (value.contains("${")) {
      throw new GovDataException("CongressAmendmentTextProvider: " + what
          + " still contains an unresolved variable (" + value
          + "); set the environment variable it names");
    }
  }

  private static JsonNode get(String url, Map<String, String> headers,
      HttpSourceConfig.RateLimitConfig rateLimit) throws IOException {
    SharedHttpClient.Response conn = RetryableHttp.openWithRetry(url, headers, rateLimit, true);
    try (InputStream in = conn.getInputStream()) {
      return MAPPER.readTree(in);
    } finally {
      conn.disconnect();
    }
  }

  /** Walks the listings of each amendment type and reads the text of each amendment lazily. */
  private static final class RowIterator implements Iterator<Map<String, Object>> {
    private final String baseUrl;
    private final String congress;
    private final Map<String, String> headers;
    private final HttpSourceConfig.RateLimitConfig rateLimit;
    private final StorageProvider sp;
    private final String cacheBaseDir;
    private final Deque<Map<String, Object>> pending = new ArrayDeque<Map<String, Object>>();
    /** Items of the current listing page still to read. */
    private final Deque<JsonNode> items = new ArrayDeque<JsonNode>();
    private int typeIndex;
    private String typeSlug;
    private final int recordLimit;
    private int readInType;
    private String nextPageUrl;

    RowIterator(String baseUrl, String congress, Map<String, String> headers,
        HttpSourceConfig.RateLimitConfig rateLimit, StorageProvider sp, String cacheBaseDir,
        int recordLimit) {
      this.recordLimit = recordLimit;
      this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
      this.congress = congress;
      this.headers = headers;
      this.rateLimit = rateLimit;
      this.sp = sp;
      this.cacheBaseDir = cacheBaseDir;
    }

    @Override public boolean hasNext() {
      try {
        while (pending.isEmpty()) {
          if (!advance()) {
            return false;
          }
        }
        return true;
      } catch (IOException e) {
        throw new GovDataException("Congress.gov amendment text crawl failed for congress "
            + congress, e);
      }
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return pending.poll();
    }

    /** Reads the next amendment, loading listing pages and types as needed. */
    private boolean advance() throws IOException {
      while (items.isEmpty() || (recordLimit > 0 && readInType >= recordLimit)) {
        if (recordLimit > 0 && readInType >= recordLimit) {
          items.clear();
          nextPageUrl = null;
        }
        if (nextPageUrl == null) {
          if (typeIndex >= TYPE_SLUGS.length) {
            return false;
          }
          typeSlug = TYPE_SLUGS[typeIndex++];
          readInType = 0;
          nextPageUrl = baseUrl + "/" + typeSlug + "?format=json&limit=" + PAGE_SIZE;
        }
        JsonNode page = get(nextPageUrl, headers, rateLimit);
        JsonNode list = page.get("amendments");
        if (list == null || !list.isArray()) {
          throw new GovDataException("Listing " + nextPageUrl + " has no amendments array");
        }
        for (JsonNode item : list) {
          items.add(item);
        }
        JsonNode next = page.path("pagination").get("next");
        nextPageUrl = next == null || next.isNull() ? null : next.asText();
      }
      JsonNode item = items.poll();
      readInType++;
      JsonNode number = item.get("number");
      JsonNode updated = item.get("updateDate");
      if (number == null || updated == null) {
        throw new GovDataException("Listing item without number or updateDate: " + item);
      }
      JsonNode response = textResponse(number.asText(), updated.asText());
      pending.addAll(rowsOf(congress, typeSlug, number.asText(), response));
      return true;
    }

    private JsonNode textResponse(String number, String updateDate) throws IOException {
      String path = sp.resolvePath(cacheBaseDir, "congress_amendment_text/congress=" + congress
          + "/type=" + typeSlug + "/number=" + number + "/update="
          + updateDate.replaceAll("[^0-9A-Za-z]", "") + ".json");
      if (sp.exists(path)) {
        try (InputStream in = sp.openInputStream(path)) {
          return MAPPER.readTree(in);
        }
      }
      String url = baseUrl + "/" + typeSlug + "/" + number + "/text?format=json";
      SharedHttpClient.Response conn = RetryableHttp.openWithRetry(url, headers, rateLimit, true);
      byte[] body;
      try (InputStream in = conn.getInputStream()) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int n;
        while ((n = in.read(chunk)) > 0) {
          buf.write(chunk, 0, n);
        }
        body = buf.toByteArray();
      } finally {
        conn.disconnect();
      }
      JsonNode parsed = MAPPER.readTree(new String(body, StandardCharsets.UTF_8));
      // Cache only a response that has the expected shape.
      rowsOf(congress, typeSlug, number, parsed);
      sp.writeFile(path, body);
      return parsed;
    }
  }
}
