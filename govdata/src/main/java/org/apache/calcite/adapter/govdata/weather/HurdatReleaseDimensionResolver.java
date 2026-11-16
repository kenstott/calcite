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
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves the {@code release_file} dimension for the NHC HURDAT2 best-track files.
 *
 * <p>NHC publishes each reanalysis as a new file whose name embeds the release date
 * ({@code hurdat2-1851-2025-092326.txt}, {@code hurdat2-nepac-1949-2025-092926.txt}) and keeps
 * the older ones, so there is no stable URL. The directory index lists a modification time for
 * every file; the resolver returns the newest file for the basin in context
 * ({@code atlantic} or {@code nepac}). Because the file name is a dimension, a new release
 * changes the raw-cache key and forces a fresh fetch.
 */
public class HurdatReleaseDimensionResolver implements DimensionResolver {

  private static final String INDEX_URL = "https://www.nhc.noaa.gov/data/hurdat/";

  /** One index row: file link followed by the modification timestamp cell. */
  private static final Pattern ROW =
      Pattern.compile("<a href=\"(hurdat2-[^\"]+\\.txt)\">[^<]*(?:<[^>]*>)?[^<]*</a></td>"
          + "<td align=\"right\">(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2})");

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    if (!"release_file".equals(dimensionName)) {
      return Collections.emptyList();
    }
    String basin = context.get("basin");
    final String prefix;
    if ("atlantic".equals(basin)) {
      prefix = "hurdat2-1851-";
    } else if ("nepac".equals(basin)) {
      prefix = "hurdat2-nepac-";
    } else {
      throw new IllegalStateException("HURDAT2: unknown basin '" + basin + "' in context");
    }
    String best = null;
    String bestTime = "";
    Matcher m = ROW.matcher(fetch(INDEX_URL));
    while (m.find()) {
      String name = m.group(1);
      if (name.startsWith(prefix) && m.group(2).compareTo(bestTime) > 0) {
        best = name;
        bestTime = m.group(2);
      }
    }
    if (best == null) {
      throw new IllegalStateException("HURDAT2: no '" + prefix + "*' file in " + INDEX_URL);
    }
    return Collections.singletonList(best);
  }

  private static String fetch(String url) {
    try {
      HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
      c.setRequestProperty("User-Agent", "calcite-govdata");
      c.setConnectTimeout(30000);
      c.setReadTimeout(60000);
      if (c.getResponseCode() != 200) {
        throw new IllegalStateException("HURDAT2: HTTP " + c.getResponseCode() + " from " + url);
      }
      try (InputStream in = c.getInputStream()) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
          out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      throw new IllegalStateException("HURDAT2: cannot read " + url + ": " + e.getMessage(), e);
    }
  }
}
