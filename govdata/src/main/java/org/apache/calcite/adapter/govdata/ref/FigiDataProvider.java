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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for figi_instruments that batches 100 tickers per OpenFIGI request.
 *
 * <p>Instead of making one HTTP call per ticker (10,354 calls ≈ 7 hours), this provider:
 * <ol>
 *   <li>Loads all tickers from the already-materialized {@code sec_company_tickers} table</li>
 *   <li>Chunks them into batches of 100 (OpenFIGI batch limit)</li>
 *   <li>Makes ~104 HTTP requests at 25 req/min ≈ 4 minutes</li>
 *   <li>Returns all results as a single Iterator</li>
 * </ol>
 *
 * <p>S3 credentials for reading {@code sec_company_tickers} are sourced from environment
 * variables ({@code AWS_ACCESS_KEY_ID}, {@code AWS_SECRET_ACCESS_KEY},
 * {@code AWS_ENDPOINT_OVERRIDE}, {@code AWS_REGION}) which are set by {@code .env.prod}
 * before the ETL run.
 *
 * <p>The tickers table path is resolved from the {@code GOVDATA_PARQUET_DIR} system property
 * (set by {@code GovDataSchemaFactory} from the model {@code directory} operand).
 *
 * <p>{@code sec_company_tickers} must appear before {@code figi_instruments} in
 * {@code ref-schema.yaml} so it is committed to Iceberg before this provider runs.
 */
public class FigiDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(FigiDataProvider.class);

  private static final String OPENFIGI_URL = "https://api.openfigi.com/v3/mapping";
  private static final int BATCH_SIZE = 100;
  /** 2.5 seconds between requests → 24 req/min, safely under the 25 req/min API key limit. */
  private static final long RATE_LIMIT_DELAY_MS = 2500;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public FigiDataProvider() {
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {

    String tickersPath = resolveTickersPath();
    if (tickersPath == null || tickersPath.isEmpty()) {
      LOGGER.warn("FigiDataProvider: GOVDATA_PARQUET_DIR not set, cannot load tickers");
      return Collections.emptyIterator();
    }

    List<String> tickers = loadTickers(tickersPath);
    if (tickers.isEmpty()) {
      LOGGER.warn("FigiDataProvider: no tickers found at {}", tickersPath);
      return Collections.emptyIterator();
    }

    String apiKey = System.getenv("OPENFIGI_API_KEY");
    LOGGER.info("FigiDataProvider: fetching FIGI for {} tickers in batches of {} (api key: {})",
        tickers.size(), BATCH_SIZE, apiKey != null && !apiKey.isEmpty() ? "set" : "not set");

    List<Map<String, Object>> allRecords = fetchAllFigi(tickers, apiKey);
    LOGGER.info("FigiDataProvider: total {} instrument records from {} tickers",
        allRecords.size(), tickers.size());
    return allRecords.iterator();
  }

  private String resolveTickersPath() {
    String parquetDir = System.getProperty("GOVDATA_PARQUET_DIR");
    if (parquetDir == null || parquetDir.isEmpty()) {
      return null;
    }
    String path = parquetDir + "/ref/sec_company_tickers";
    // Normalize: s3:/ → s3:// (StorageProvider.normalizePath equivalent)
    return path.replace("s3:/", "s3://").replace("s3:///", "s3://");
  }

  private List<String> loadTickers(String tickersPath) {
    try (Connection conn = createDuckDBWithS3();
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT DISTINCT ticker "
          + "FROM read_parquet('" + tickersPath + "/data/**/*.parquet', "
          + "hive_partitioning=true, union_by_name=true) "
          + "WHERE ticker IS NOT NULL "
          + "ORDER BY ticker";

      LOGGER.debug("FigiDataProvider: loading tickers from {}", tickersPath);
      ResultSet rs = stmt.executeQuery(sql);

      List<String> tickers = new ArrayList<String>();
      while (rs.next()) {
        String ticker = rs.getString(1);
        if (ticker != null && !ticker.isEmpty()) {
          tickers.add(ticker);
        }
      }

      LOGGER.info("FigiDataProvider: loaded {} tickers from sec_company_tickers", tickers.size());
      return tickers;

    } catch (Exception e) {
      LOGGER.warn("FigiDataProvider: failed to load tickers from {}: {}. "
          + "Ensure sec_company_tickers has been materialized first.", tickersPath, e.getMessage());
      return Collections.emptyList();
    }
  }

  private Connection createDuckDBWithS3() throws java.sql.SQLException {
    Connection conn = AbstractGovDataDownloader.getDuckDBConnection(null);

    String keyId = System.getenv("AWS_ACCESS_KEY_ID");
    String secret = System.getenv("AWS_SECRET_ACCESS_KEY");
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    String region = System.getenv("AWS_REGION");

    if (keyId == null || keyId.isEmpty() || secret == null || secret.isEmpty()) {
      LOGGER.debug("FigiDataProvider: no AWS credentials in env, DuckDB will use default chain");
      return conn;
    }

    StringBuilder sql = new StringBuilder();
    sql.append("CREATE OR REPLACE SECRET figi_s3 (TYPE s3, PROVIDER config, ");
    sql.append("KEY_ID '").append(keyId).append("', ");
    sql.append("SECRET '").append(secret).append("'");

    if (region != null && !region.isEmpty()) {
      sql.append(", REGION '").append(region).append("'");
    }

    if (endpoint != null && !endpoint.isEmpty()) {
      String host = endpoint.startsWith("https://") ? endpoint.substring("https://".length())
          : endpoint.startsWith("http://") ? endpoint.substring("http://".length()) : endpoint;
      sql.append(", ENDPOINT '").append(host).append("'");
      sql.append(", URL_STYLE 'path'");
      sql.append(", USE_SSL ").append(!endpoint.startsWith("http://"));
    }

    sql.append(")");

    try {
      conn.createStatement().execute(sql.toString());
    } catch (java.sql.SQLException e) {
      LOGGER.warn("FigiDataProvider: failed to configure S3 credentials: {}", e.getMessage());
    }

    return conn;
  }

  private List<Map<String, Object>> fetchAllFigi(List<String> tickers, String apiKey)
      throws IOException {
    List<Map<String, Object>> allRecords = new ArrayList<Map<String, Object>>();
    int totalBatches = (tickers.size() + BATCH_SIZE - 1) / BATCH_SIZE;

    for (int i = 0; i < tickers.size(); i += BATCH_SIZE) {
      List<String> batch = tickers.subList(i, Math.min(i + BATCH_SIZE, tickers.size()));
      int batchNum = i / BATCH_SIZE + 1;

      LOGGER.info("FigiDataProvider: batch {}/{} ({} tickers)", batchNum, totalBatches,
          batch.size());

      List<Map<String, Object>> batchRecords = fetchBatch(batch, apiKey);
      allRecords.addAll(batchRecords);

      if (i + BATCH_SIZE < tickers.size()) {
        try {
          Thread.sleep(RATE_LIMIT_DELAY_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn("FigiDataProvider: interrupted at batch {}", batchNum);
          break;
        }
      }
    }

    return allRecords;
  }

  private List<Map<String, Object>> fetchBatch(List<String> tickers, String apiKey)
      throws IOException {
    ArrayNode requestBody = MAPPER.createArrayNode();
    for (String ticker : tickers) {
      ObjectNode item = MAPPER.createObjectNode();
      item.put("idType", "TICKER");
      item.put("idValue", ticker);
      requestBody.add(item);
    }

    String requestJson = requestBody.toString();

    URL url = URI.create(OPENFIGI_URL).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    if (apiKey != null && !apiKey.isEmpty()) {
      conn.setRequestProperty("X-OPENFIGI-APIKEY", apiKey);
    }
    conn.setDoOutput(true);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);

    byte[] requestBytes = requestJson.getBytes(StandardCharsets.UTF_8);
    conn.setRequestProperty("Content-Length", String.valueOf(requestBytes.length));
    OutputStream out = conn.getOutputStream();
    out.write(requestBytes);
    out.flush();

    int responseCode = conn.getResponseCode();
    if (responseCode == 429) {
      LOGGER.warn("FigiDataProvider: rate limit (HTTP 429) — backing off 65s");
      try {
        Thread.sleep(65000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("OpenFIGI rate limit exceeded (HTTP 429)");
    }
    if (responseCode != 200) {
      LOGGER.warn("FigiDataProvider: HTTP {} from OpenFIGI for batch of {}",
          responseCode, tickers.size());
      return Collections.emptyList();
    }

    String responseBody;
    try (InputStream in = conn.getInputStream()) {
      byte[] buf = new byte[65536];
      StringBuilder sb = new StringBuilder();
      int n;
      while ((n = in.read(buf)) != -1) {
        sb.append(new String(buf, 0, n, StandardCharsets.UTF_8));
      }
      responseBody = sb.toString();
    }

    return parseResponse(responseBody, tickers);
  }

  /**
   * Parses the OpenFIGI batch response into records using OpenFIGI field names.
   *
   * <p>Response is an array of per-item results, one per input ticker.
   * Each result is either {@code {"data":[...]}} or {@code {"error":"..."}}.
   * Fields are kept in OpenFIGI naming (e.g., {@code exchCode}) because the
   * figi_instruments column config maps them via {@code source:} to our names.
   */
  private List<Map<String, Object>> parseResponse(String json, List<String> tickers) {
    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();

    try {
      JsonNode root = MAPPER.readTree(json);

      if (root.isObject()) {
        // Top-level error (rate limit, auth failure)
        String status = root.path("status").asText("");
        String message = root.path("message").asText(root.path("error").asText(""));
        if ("429".equals(status) || (message != null && message.toLowerCase().contains("rate"))) {
          throw new RuntimeException("OpenFIGI rate limit: " + message);
        }
        LOGGER.warn("FigiDataProvider: OpenFIGI API error: {}", message);
        return Collections.emptyList();
      }

      if (!root.isArray()) {
        LOGGER.warn("FigiDataProvider: unexpected response shape");
        return Collections.emptyList();
      }

      int errorCount = 0;
      for (int i = 0; i < root.size(); i++) {
        JsonNode resultSet = root.get(i);
        JsonNode error = resultSet.path("error");
        if (!error.isMissingNode()) {
          errorCount++;
          continue;
        }
        JsonNode data = resultSet.path("data");
        if (!data.isArray()) {
          continue;
        }
        for (JsonNode instrument : data) {
          Map<String, Object> record = new HashMap<String, Object>();
          record.put("figi", textOrNull(instrument, "figi"));
          record.put("ticker", textOrNull(instrument, "ticker"));
          record.put("name", textOrNull(instrument, "name"));
          record.put("exchCode", textOrNull(instrument, "exchCode"));
          record.put("marketSector", textOrNull(instrument, "marketSector"));
          record.put("securityType", textOrNull(instrument, "securityType"));
          record.put("securityType2", textOrNull(instrument, "securityType2"));
          record.put("securityDescription", textOrNull(instrument, "securityDescription"));
          record.put("compositeFIGI", textOrNull(instrument, "compositeFIGI"));
          record.put("shareClassFIGI", textOrNull(instrument, "shareClassFIGI"));
          records.add(record);
        }
      }

      if (errorCount > 0) {
        LOGGER.debug("FigiDataProvider: {} of {} batch items had no match",
            errorCount, root.size());
      }

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("FigiDataProvider: failed to parse OpenFIGI response: {}", e.getMessage());
    }

    return records;
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asText();
  }
}
