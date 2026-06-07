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

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Resolves the {@code advisory_path} dimension from a CISA CSAF tree listing.
 *
 * <p>CISA publishes its advisories as CSAF 2.0 JSON files in the {@code cisagov/CSAF}
 * GitHub repository (branch {@code develop}). Each {@code white/} tree contains an
 * {@code index.txt} (one relative path per line, e.g. {@code 2026/va-26-155-01.json})
 * and a {@code changes.csv} ({@code path,ISO-timestamp} per line). This resolver fetches
 * that listing and returns the list of relative paths, which the engine templates into
 * the source URL as {@code {advisory_path}}.
 *
 * <p>The listing URL is configured via the {@code indexUrl} dimension property. If a
 * parent {@code year} dimension value is present in the context, paths are filtered to
 * those whose first path segment matches that year.
 *
 * <h3>Schema Configuration</h3>
 * <pre>{@code
 * hooks:
 *   dimensionResolver:
 *     "org.apache.calcite.adapter.govdata.cyber.vuln.CsafAdvisoryDimensionResolver"
 *
 * dimensions:
 *   year:
 *     type: yearRange
 *     start: 2020
 *     end: current
 *   advisory_path:
 *     type: custom
 *     properties:
 *       indexUrl:
 *         "https://raw.githubusercontent.com/cisagov/CSAF/develop/csaf_files/IT/white/index.txt"
 * }</pre>
 */
public class CsafAdvisoryDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CsafAdvisoryDimensionResolver.class);
  private static final int TIMEOUT_MS = 30_000;

  /**
   * Default constructor required for reflection-based instantiation.
   */
  public CsafAdvisoryDimensionResolver() {
  }

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    if (!"advisory_path".equals(dimensionName)) {
      LOGGER.debug("CsafAdvisoryDimensionResolver: dimension '{}' not handled, returning empty",
          dimensionName);
      return new ArrayList<String>();
    }

    String indexUrl = config.getProperty("indexUrl");
    if (indexUrl == null || indexUrl.trim().isEmpty()) {
      throw new IllegalStateException(
          "CsafAdvisoryDimensionResolver: missing required property 'indexUrl' "
          + "for dimension 'advisory_path'");
    }

    String yearFilter = context.get("year");
    if (yearFilter != null && yearFilter.trim().isEmpty()) {
      yearFilter = null;
    }

    List<String> paths;
    try {
      paths = fetchIndex(indexUrl.trim(), yearFilter);
    } catch (IOException e) {
      throw new RuntimeException(
          "CsafAdvisoryDimensionResolver: failed to fetch index " + indexUrl
          + ": " + e.getMessage(), e);
    }

    LOGGER.info("CsafAdvisoryDimensionResolver: resolved {} advisory paths from {} (year={})",
        paths.size(), indexUrl, yearFilter);
    return paths;
  }

  /**
   * Fetches the index/changes file over HTTPS and parses it into relative paths.
   *
   * @param indexUrl URL of the {@code index.txt} or {@code changes.csv} listing
   * @param yearFilter Year to filter by (first path segment), or null for all
   * @return List of relative advisory paths
   * @throws IOException if the request fails or returns a non-200 status
   */
  private List<String> fetchIndex(String indexUrl, String yearFilter) throws IOException {
    HttpURLConnection conn =
        (HttpURLConnection) URI.create(indexUrl).toURL().openConnection();
    conn.setInstanceFollowRedirects(true);
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(TIMEOUT_MS);
    conn.setReadTimeout(TIMEOUT_MS);
    conn.setRequestProperty("User-Agent", "calcite-govdata/1.0");

    int status = conn.getResponseCode();
    if (status != 200) {
      conn.disconnect();
      throw new IOException("HTTP " + status + " for " + indexUrl);
    }

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
      return parseIndex(reader, yearFilter);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Parses a CSAF tree listing into relative advisory paths.
   *
   * <p>Each non-empty line's substring before the first comma is taken as the relative
   * path (handles both {@code index.txt} — bare paths — and {@code changes.csv} —
   * {@code path,timestamp}). A header line whose first token is literally {@code "path"}
   * is skipped. When {@code yearFilter} is non-null, only paths whose first segment
   * equals the filter are returned.
   *
   * @param reader Reader over the listing content
   * @param yearFilter Year to filter by (first path segment), or null for all
   * @return List of relative advisory paths
   * @throws IOException if reading fails
   */
  static List<String> parseIndex(BufferedReader reader, String yearFilter) throws IOException {
    List<String> paths = new ArrayList<String>();
    String line;
    while ((line = reader.readLine()) != null) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      int comma = trimmed.indexOf(',');
      String path = comma >= 0 ? trimmed.substring(0, comma).trim() : trimmed;
      if (path.isEmpty()) {
        continue;
      }
      if ("path".equals(path)) {
        // Header line in changes.csv
        continue;
      }
      if (yearFilter != null) {
        int slash = path.indexOf('/');
        String firstSegment = slash >= 0 ? path.substring(0, slash) : path;
        if (!yearFilter.equals(firstSegment)) {
          continue;
        }
      }
      paths.add(path);
    }
    return paths;
  }
}
