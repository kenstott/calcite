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
import org.apache.calcite.adapter.file.etl.CompleteBody;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
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
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Fetches a USGS county-level water-use compilation file and applies the table's
 * {@code responseTransformer} to it.
 *
 * <p>The 2015 compilation is a ScienceBase file, and www.sciencebase.gov answers a Cloudflare
 * challenge (HTTP 403) to non-browser clients, so it is downloaded through a headed browser
 * session ({@link ScienceBaseBrowserSession}) configured by the table's source parameters. Files
 * on other hosts are downloaded directly. Those parameters configure the browser and are never
 * sent as query parameters, which is why this provider serves every URL of the table.
 *
 * <p>Each file is a closed census vintage at a URL that identifies its content, and is read through
 * the pipeline's raw cache. The files are a few megabytes, held in memory because
 * {@link ResponseTransformer} takes the whole body.
 */
public class UsgsWaterUseFileProvider implements CachingDataProvider {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Function<Map<String, String>, ScienceBaseClient> clients;

  public UsgsWaterUseFileProvider() {
    this(ScienceBaseBrowserSession::get);
  }

  UsgsWaterUseFileProvider(Function<Map<String, String>, ScienceBaseClient> clients) {
    this.clients = clients;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String template = config.getSource().getEffectiveUrl(variables);
    if (template == null || template.isEmpty()) {
      throw new IOException("USGS water use: source.url is required");
    }
    final String url = substitute(template, variables);
    final Map<String, String> parameters = config.getSource().getParameters();

    String body;
    try (InputStream in = rawCache.openStream(url, () -> ScienceBaseBrowserSession.serves(url)
        ? clients.apply(parameters).openStream(url)
        : openDirect(url))) {
      body = readUtf8(in);
    }

    String json = transformer(config).transform(body, RequestContext.builder()
        .url(url)
        .dimensionValues(variables)
        .build());
    List<Map<String, Object>> rows =
        MAPPER.readValue(json, new TypeReference<List<Map<String, Object>>>() { });
    if (rows.isEmpty()) {
      throw new IOException("USGS water use: no county rows parsed from " + url);
    }
    return rows.iterator();
  }

  private static String substitute(String template, Map<String, String> variables)
      throws IOException {
    String url = template;
    for (Map.Entry<String, String> e : variables.entrySet()) {
      url = url.replace("{" + e.getKey() + "}", e.getValue());
    }
    if (url.indexOf('{') >= 0) {
      throw new IOException("USGS water use: unresolved variable in URL " + url);
    }
    return url;
  }

  private static ResponseTransformer transformer(EtlPipelineConfig config) {
    String className = config.getHooks().getResponseTransformerClass();
    if (className == null) {
      throw new IllegalStateException("USGS water use: hooks.responseTransformer is required for "
          + config.getName());
    }
    try {
      return (ResponseTransformer) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("USGS water use: cannot create " + className, e);
    }
  }

  private static InputStream openDirect(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    NetworkOutageWait.connect(conn);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("USGS water use: HTTP " + code + " for " + url);
    }
    return CompleteBody.of(conn);
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
