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
// storage-provider-guard:ignore-file - audited: the cache is a local temp directory, never an
// object-store URI.

import org.apache.calcite.adapter.file.etl.ModelOperand;
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.govdata.BrowserHelperProcess;
import org.apache.calcite.adapter.govdata.GovDataException;
import org.apache.calcite.adapter.govdata.GovDataUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Map;

/**
 * One scripted session against the Senate's eFD system (efdsearch.senate.gov), run inside a
 * Windows-side Edge driven over the DevTools protocol (Akamai answers HTTP 403 to every client on
 * the Linux side). Every search and view is gated behind a click-through agreement;
 * {@link #acceptAgreement()} clicks through it once and the browser's cookies carry the session
 * after that. Requests are serialized and spaced by {@link #REQUEST_SPACING_MS}, and a 5xx or I/O
 * failure is retried with a growing delay (10s, 20s, ... about 2.5 minutes in all, since eFD
 * answers runs of 503s for minutes at a time) before it is thrown. A browser that cannot start
 * fails the run with the cause; there is no fallback client.
 *
 * <p>A view URL that redirects to the site root is a report the Senate no longer serves
 * (candidate reports expire a year after the candidacy ends), which {@link #getView(String)}
 * reports as {@link View#unavailable} rather than hiding behind the home page's HTML.
 */
final class SenateEfdSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(SenateEfdSession.class);

  static final String SITE = "https://efdsearch.senate.gov";

  private static final long REQUEST_SPACING_MS = 350L;
  private static final int MAX_ATTEMPTS = 6;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Result of {@link #getView(String)}. */
  static final class View {
    final String body;
    final boolean unavailable;

    View(String body, boolean unavailable) {
      this.body = body;
      this.unavailable = unavailable;
    }
  }

  /**
   * Local cache of report pages and past-year listings. A report page is immutable (an amendment
   * is a new filing with its own UUID) and an expired report stays expired, so both are cached
   * forever; a listing is cached only for a year that has ended. Every table of a year reads the
   * same pages, so without this each of the ten tables would crawl the site again.
   */
  private static File cacheDir() {
    // In the run's staging directory (REQ-1992), never the JVM temp directory
    return new File(org.apache.calcite.adapter.file.StagingDirectory.get(), "govdata-senate-efd");
  }
  private static final String UNAVAILABLE_MARKER = "UNAVAILABLE";

  private BrowserHelperProcess helper;
  private boolean agreed;
  private long lastRequestAt;

  /** Accepts the agreement in the browser session; idempotent. */
  synchronized void acceptAgreement() throws IOException {
    if (agreed) {
      return;
    }
    call(MAPPER.createObjectNode().put("op", "agree"));
    agreed = true;
  }

  /** POSTs the DataTables listing query and returns the JSON body. */
  synchronized String listing(Map<String, String> params, boolean cacheable) throws IOException {
    File cached = new File(cacheDir(), "list-" + sha256(params.toString()) + ".json");
    if (cacheable && cached.isFile()) {
      return new String(Files.readAllBytes(cached.toPath()), StandardCharsets.UTF_8);
    }
    acceptAgreement();
    ObjectNode request = MAPPER.createObjectNode().put("op", "post").put("path",
        "/search/report/data/");
    ObjectNode fields = request.putObject("fields");
    for (Map.Entry<String, String> field : params.entrySet()) {
      fields.put(field.getKey(), field.getValue());
    }
    JsonNode response = call(request);
    int status = response.path("status").asInt();
    if (status != 200) {
      throw new GovDataException(SITE + "/search/report/data/: HTTP " + status
          + " (a redirect means the session's agreement lapsed)");
    }
    String body = response.path("body").asText();
    if (cacheable) {
      store(cached, body);
    }
    return body;
  }

  /** GETs a report view path such as {@code /search/view/annual/{uuid}/}. */
  synchronized View getView(String path) throws IOException {
    File cached = new File(cacheDir(), path.replaceAll("[^A-Za-z0-9]+", "_") + ".html");
    if (cached.isFile()) {
      String body = new String(Files.readAllBytes(cached.toPath()), StandardCharsets.UTF_8);
      return body.equals(UNAVAILABLE_MARKER) ? new View(null, true) : new View(body, false);
    }
    acceptAgreement();
    JsonNode response = call(MAPPER.createObjectNode().put("op", "get").put("path", path));
    int status = response.path("status").asInt();
    String finalPath = response.path("finalPath").asText();
    if (status == 200 && !response.path("redirected").asBoolean()) {
      String body = response.path("body").asText();
      store(cached, body);
      return new View(body, false);
    }
    if (status == 200 && (finalPath.equals("/") || finalPath.equals("/search/home/"))) {
      store(cached, UNAVAILABLE_MARKER);
      return new View(null, true);
    }
    throw new GovDataException(SITE + path + ": HTTP " + status + " finalPath=" + finalPath
        + " redirected=" + response.path("redirected").asBoolean());
  }

  private static String sha256(String text) {
    try {
      byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
          .digest(text.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder();
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new GovDataException(e);
    }
  }

  private static void store(File target, String body) throws IOException {
    Files.createDirectories(cacheDir().toPath());
    File tmp = File.createTempFile("efd-", ".tmp", cacheDir());
    Files.write(tmp.toPath(), body.getBytes(StandardCharsets.UTF_8));
    Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * A law schema setting: the model operand, or, when the operand is not yet published (an ETL-time
   * reader runs before the top-level settings are merged into it), the schema YAML's own value.
   */
  private static String setting(String key) {
    String value = ModelOperand.getString("law." + key);
    if (value == null) {
      value = GovDataUtils.loadSchemaSetting(LawSchemaFactory.class, "/law/law-schema.yaml", key);
      if (value != null && value.contains("${")) {
        value = VariableResolver.resolveEnvVars(value);
      }
    }
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalStateException("fd_*: law." + key + " is not set in the law schema model");
    }
    return value.trim();
  }

  private BrowserHelperProcess helper() {
    if (helper == null || !helper.isAlive()) {
      helper = BrowserHelperProcess.start("fd_*", setting("efdNodeCommand"),
          setting("efdBrowserScript"),
          "run govdata/scripts/edge-browser/install.sh and set EFD_BROWSER_SCRIPT",
          Arrays.asList(setting("efdEdgeExecutable"), setting("efdDebugPort"),
              setting("efdProfileDir")));
    }
    return helper;
  }

  /** One request to the browser, spaced by {@link #REQUEST_SPACING_MS}; 5xx and I/O failures retried. */
  private JsonNode call(ObjectNode request) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      long wait = lastRequestAt + REQUEST_SPACING_MS - System.currentTimeMillis();
      try {
        if (wait > 0) {
          Thread.sleep(wait);
        }
        JsonNode response = helper().call(request);
        lastRequestAt = System.currentTimeMillis();
        if (response.path("status").asInt(0) < 500) {
          return response;
        }
        last = new IOException(request.path("path").asText() + ": HTTP "
            + response.path("status").asInt());
      } catch (IOException e) {
        last = e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(request.path("path").asText() + ": interrupted", e);
      }
      LOGGER.warn("{} (attempt {}/{})", last.getMessage(), attempt, MAX_ATTEMPTS);
      try {
        Thread.sleep(10000L * attempt);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(request.path("path").asText() + ": interrupted", e);
      }
    }
    throw last;
  }
}
