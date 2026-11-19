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
 * Resolves the {@code release_file} dimension for the NCEI nClimDiv statewide files.
 *
 * <p>NCEI republishes each file monthly under a new name that embeds the version and build date
 * ({@code climdiv-tmpcst-v1.0.0-20261105}) and removes the old one, so there is no stable URL.
 * The resolver returns the single current file for the {@code element} in context. Because the
 * name is a dimension, a new monthly build changes the combination and the raw-cache key.
 */
public class NclimdivReleaseDimensionResolver implements DimensionResolver {

  private static final String INDEX_URL = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/";

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    if (!"release_file".equals(dimensionName)) {
      return Collections.emptyList();
    }
    String element = context.get("element");
    if (element == null) {
      throw new IllegalStateException("nClimDiv: no element in context");
    }
    Pattern p = Pattern.compile("href=\"(climdiv-" + Pattern.quote(element)
        + "st-v[0-9.]+-[0-9]{8})\"");
    String best = null;
    Matcher m = p.matcher(fetch(INDEX_URL));
    while (m.find()) {
      // names share a prefix and end in the build date, so the lexicographic maximum is newest
      if (best == null || m.group(1).compareTo(best) > 0) {
        best = m.group(1);
      }
    }
    if (best == null) {
      throw new IllegalStateException("nClimDiv: no 'climdiv-" + element + "st-*' file in "
          + INDEX_URL);
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
        throw new IllegalStateException("nClimDiv: HTTP " + c.getResponseCode() + " from " + url);
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
      throw new IllegalStateException("nClimDiv: cannot read " + url + ": " + e.getMessage(), e);
    }
  }
}
