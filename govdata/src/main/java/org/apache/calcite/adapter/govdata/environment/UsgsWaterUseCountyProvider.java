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
package org.apache.calcite.adapter.govdata.environment;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;
import org.apache.calcite.adapter.govdata.ScienceBaseBrowserSession;
import org.apache.calcite.adapter.govdata.ScienceBaseClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Fetches the USGS county-level water-use compilation, a ScienceBase file, and applies the table's
 * {@code responseTransformer} to it.
 *
 * <p>www.sciencebase.gov answers a Cloudflare challenge (HTTP 403) to non-browser clients, so the
 * file is downloaded through a headed browser session ({@link ScienceBaseBrowserSession})
 * configured by the table's source parameters.
 *
 * <p>The file URL identifies its content, so it is read through the pipeline's raw cache. It is a
 * few megabytes, held in memory because {@link ResponseTransformer} takes the whole body.
 */
public class UsgsWaterUseCountyProvider implements CachingDataProvider {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Function<Map<String, String>, ScienceBaseClient> clients;

  public UsgsWaterUseCountyProvider() {
    this(ScienceBaseBrowserSession::get);
  }

  UsgsWaterUseCountyProvider(Function<Map<String, String>, ScienceBaseClient> clients) {
    this.clients = clients;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    final String url = config.getSource().getUrl();
    final Map<String, String> parameters = config.getSource().getParameters();

    String body;
    try (InputStream in =
        rawCache.openStream(url, () -> clients.apply(parameters).openStream(url))) {
      body = readUtf8(in);
    }

    String json = transformer(config).transform(body, RequestContext.builder()
        .url(url)
        .dimensionValues(variables)
        .build());
    List<Map<String, Object>> rows =
        MAPPER.readValue(json, new TypeReference<List<Map<String, Object>>>() { });
    if (rows.isEmpty()) {
      throw new IOException("USGS county water use: no county rows parsed from " + url);
    }
    return rows.iterator();
  }

  private static ResponseTransformer transformer(EtlPipelineConfig config) {
    String className = config.getHooks().getResponseTransformerClass();
    if (className == null) {
      throw new IllegalStateException(
          "USGS county water use: hooks.responseTransformer is required for " + config.getName());
    }
    try {
      return (ResponseTransformer) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("USGS county water use: cannot create " + className, e);
    }
  }

  private static String readUtf8(InputStream in) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] chunk = new byte[65536];
    int n;
    while ((n = in.read(chunk)) != -1) {
      buf.write(chunk, 0, n);
    }
    return new String(buf.toByteArray(), StandardCharsets.UTF_8);
  }
}
