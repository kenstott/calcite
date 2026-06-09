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

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.GovDataUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves the {@code partition_file} dimension for openFDA bulk ingestion to the list of
 * partition ZIP URLs published in {@code download.json}, filtered by {@code GOVDATA_START_YEAR}.
 *
 * <p>openFDA splits each endpoint into pre-built JSON ZIP partitions on the unthrottled
 * {@code download.open.fda.gov} host (no API key, no 25k skip cap). The file count per
 * quarter is not predictable (5-of-5 in 2004, ~30-of-30 today), so the URLs cannot be
 * templated — they must be read from {@code download.json}.
 *
 * <p>Endpoint defaults to {@code drug.event} (FAERS); override with the dimension's
 * {@code endpoint} property (e.g. {@code drug.ndc}). Year is parsed from the
 * {@code /YYYYqN/} path segment; partitions for endpoints without a year segment are all
 * retained. Newest partitions are returned first so recent data processes first.
 */
public class OpenFdaPartitionResolver implements DimensionResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenFdaPartitionResolver.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String DOWNLOAD_JSON = "https://api.fda.gov/download.json";
  private static final Pattern YEAR_QUARTER = Pattern.compile("/(\\d{4})q[1-4]/");

  @Override
  public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    String endpoint = endpointFor(config);
    int startYear = startYear();
    try {
      JsonNode node = MAPPER.readTree(downloadBytes(DOWNLOAD_JSON));
      for (String segment : endpoint.split("\\.")) {
        node = node.path(segment);
      }
      JsonNode partitions = node.path("partitions");
      if (!partitions.isArray() || partitions.size() == 0) {
        LOGGER.warn("OpenFdaPartitionResolver: no partitions for endpoint '{}' in {}",
            endpoint, DOWNLOAD_JSON);
        return Collections.emptyList();
      }

      List<String> files = new ArrayList<String>();
      int skipped = 0;
      for (JsonNode part : partitions) {
        String file = part.path("file").asText(null);
        if (file == null || file.isEmpty()) {
          continue;
        }
        Matcher m = YEAR_QUARTER.matcher(file);
        if (m.find()) {
          if (Integer.parseInt(m.group(1)) < startYear) {
            skipped++;
            continue;
          }
        }
        files.add(file);
      }
      // Newest first (descending file path sorts quarters/sequence in chronological order).
      Collections.sort(files, Collections.reverseOrder());
      LOGGER.info("OpenFdaPartitionResolver: endpoint={} startYear={} -> {} partition files "
          + "({} skipped before startYear)", endpoint, startYear, files.size(), skipped);
      return files;
    } catch (Exception e) {
      LOGGER.error("OpenFdaPartitionResolver: failed to resolve partitions for '{}': {}",
          endpoint, e.getMessage());
      return Collections.emptyList();
    }
  }

  private static String endpointFor(DimensionConfig config) {
    if (config != null && config.getProperties() != null) {
      Object ep = config.getProperties().get("endpoint");
      if (ep != null && !ep.toString().isEmpty()) {
        return ep.toString();
      }
    }
    return "results.drug.event";
  }

  private static int startYear() {
    // operand -> system property -> env -> (currentYear-5) fallback, matching the rest of
    // govdata. The DQ harness exports GOVDATA_START_YEAR as an ENV var (not a -D system
    // property), so reading only the system property would silently ignore the DQ bound.
    return GovDataUtils.getStartYear(null);
  }

  private static byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    InputStream is = conn.getInputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
    } finally {
      is.close();
    }
    return baos.toByteArray();
  }
}
