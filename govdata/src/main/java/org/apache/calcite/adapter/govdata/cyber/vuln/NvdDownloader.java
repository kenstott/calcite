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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.cyber.AbstractCyberDownloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Downloads NVD CVE 2.0 catalog and materializes it as parquet.
 *
 * <p>The NVD API returns paginated results (up to 2,000 per page). This downloader
 * fetches the first page and delegates all subsequent pages to
 * {@link NvdResponseTransformer} which manages pagination internally.
 *
 * <p>Optional {@code apiKey} (from {@code CYBER_NVD_API_KEY}) raises the rate limit
 * from 5 requests/30 s to 50 requests/30 s.
 *
 * <p>Optional date filters ({@code lastModStartDate} / {@code lastModEndDate})
 * restrict the download to CVEs modified in a date range, enabling delta mode.
 * Both are ISO-8601 date-times in the form {@code 2024-01-01T00:00:00.000}. The
 * NVD API enforces a 120-day window when date filters are used.
 *
 * <p>Output: {@code {parquetDirectory}/type=vulnerabilities.parquet}
 */
public class NvdDownloader extends AbstractCyberDownloader {

  private static final Logger LOGGER = LoggerFactory.getLogger(NvdDownloader.class);

  /** NVD CVE 2.0 base URL. */
  static final String NVD_BASE_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0";

  /** Parquet file path relative to parquetDirectory. */
  static final String PARQUET_REL_PATH = "type=vulnerabilities.parquet";

  /** Results per page for the first-page request (NVD maximum is 2,000). */
  private static final int PAGE_SIZE = 2000;

  private final String apiKey;
  private final String lastModStartDate;
  private final String lastModEndDate;
  private final NvdResponseTransformer transformer;

  /**
   * Full-catalog downloader — fetches all CVEs.
   *
   * @param storageProvider target storage
   * @param parquetDirectory base parquet directory for this schema
   * @param apiKey NVD API key from {@code CYBER_NVD_API_KEY}, or null for anonymous
   */
  public NvdDownloader(StorageProvider storageProvider, String parquetDirectory, String apiKey) {
    this(storageProvider, parquetDirectory, apiKey, null, null);
  }

  /**
   * Delta downloader — fetches only CVEs modified within the given date range.
   *
   * @param storageProvider    target storage
   * @param parquetDirectory   base parquet directory for this schema
   * @param apiKey             NVD API key, or null
   * @param lastModStartDate   ISO-8601 datetime (e.g. {@code 2024-01-01T00:00:00.000}), or null
   * @param lastModEndDate     ISO-8601 datetime, or null
   */
  public NvdDownloader(StorageProvider storageProvider, String parquetDirectory,
      String apiKey, String lastModStartDate, String lastModEndDate) {
    super(storageProvider, parquetDirectory);
    this.apiKey = apiKey;
    this.lastModStartDate = lastModStartDate;
    this.lastModEndDate = lastModEndDate;
    this.transformer = new NvdResponseTransformer();
  }

  @Override public void download() throws IOException, InterruptedException {
    String url = buildFirstPageUrl();
    LOGGER.info("NVD: fetching first page from {}", url);

    String[] headers = apiKey != null && !apiKey.isEmpty()
        ? new String[]{"apiKey", apiKey, "Accept", "application/json"}
        : new String[]{"Accept", "application/json"};

    String firstPage = fetchString(url, headers);

    // Build context — the transformer uses headers.apiKey and parameters for pagination
    Map<String, String> contextHeaders = new LinkedHashMap<String, String>();
    if (apiKey != null && !apiKey.isEmpty()) {
      contextHeaders.put("apiKey", apiKey);
    }

    Map<String, String> contextParams = new LinkedHashMap<String, String>();
    contextParams.put("resultsPerPage", String.valueOf(PAGE_SIZE));
    if (lastModStartDate != null) {
      contextParams.put("lastModStartDate", lastModStartDate);
    }
    if (lastModEndDate != null) {
      contextParams.put("lastModEndDate", lastModEndDate);
    }

    LOGGER.info("NVD: transforming (pagination handled by transformer)");
    String jsonArray = transformer.transform(firstPage,
        buildContext(url, contextHeaders, contextParams));

    LOGGER.info("NVD: writing parquet");
    writeJsonToParquet(jsonArray, "nvd_vulnerabilities", PARQUET_REL_PATH);
  }

  private String buildFirstPageUrl() {
    StringBuilder sb = new StringBuilder(NVD_BASE_URL);
    sb.append("?resultsPerPage=").append(PAGE_SIZE);
    sb.append("&startIndex=0");
    if (lastModStartDate != null && !lastModStartDate.isEmpty()) {
      sb.append("&lastModStartDate=").append(urlEncode(lastModStartDate));
    }
    if (lastModEndDate != null && !lastModEndDate.isEmpty()) {
      sb.append("&lastModEndDate=").append(urlEncode(lastModEndDate));
    }
    return sb.toString();
  }

  private static String urlEncode(String value) {
    try {
      return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8.name());
    } catch (java.io.UnsupportedEncodingException e) {
      return value;
    }
  }
}
