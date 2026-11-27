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
package org.apache.calcite.adapter.govdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;

/**
 * One headed-browser session for fetching ScienceBase resources (www.sciencebase.gov answers a
 * Cloudflare challenge, HTTP 403, to non-browser clients). The helper script
 * {@code govdata/scripts/browser-fetch/fetch-page.js} loads an entry page once and fetches every
 * later URL from inside it. The session is shared across a run and closed when the JVM exits.
 *
 * <p>Settings come from the table's source parameters ({@code browserScript},
 * {@code browserNodeCommand}, {@code browserChromeExecutable}, {@code browserRequestDelayMs},
 * {@code browserEntryPageUrl}); a missing or unusable one throws {@link IllegalStateException}
 * naming the cause. There is no fallback to a plain HTTP client.
 */
public final class ScienceBaseBrowserSession implements ScienceBaseClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String HOST = "www.sciencebase.gov";
  private static final String LABEL = "sciencebase";
  private static final String INSTALL_HINT =
      "run govdata/scripts/browser-fetch/install.sh and set SSA_BROWSER_SCRIPT";

  private static ScienceBaseBrowserSession current;

  private final String key;
  private final BrowserHelperProcess helper;

  private ScienceBaseBrowserSession(String key, BrowserHelperProcess helper) {
    this.key = key;
    this.helper = helper;
  }

  /** Whether {@code url} is served by ScienceBase, and so has to be fetched through a browser. */
  public static boolean serves(String url) {
    return HOST.equalsIgnoreCase(URI.create(url).getHost());
  }

  /** The shared session for the source parameters {@code p}, started if needed. */
  public static synchronized ScienceBaseBrowserSession get(Map<String, String> p) {
    String node = require(p, "browserNodeCommand");
    String script = p.get("browserScript");
    String entry = require(p, "browserEntryPageUrl");
    String delay = require(p, "browserRequestDelayMs");
    String chrome = p.get("browserChromeExecutable");
    String key = node + '|' + script + '|' + entry + '|' + delay + '|' + chrome;
    if (current != null && current.key.equals(key) && current.helper.isAlive()) {
      return current;
    }
    if (current != null) {
      current.helper.close();
      current = null;
    }
    BrowserHelperProcess h = BrowserHelperProcess.start(LABEL, node, script, INSTALL_HINT,
        Arrays.asList(entry, delay, chrome == null ? "" : chrome));
    current = new ScienceBaseBrowserSession(key, h);
    return current;
  }

  private static String require(Map<String, String> p, String name) {
    String v = p.get(name);
    if (v == null || v.trim().isEmpty()) {
      throw new IllegalStateException(LABEL + ": source parameter " + name
          + " is missing from the table source block");
    }
    return v.trim();
  }

  @Override public String fetchText(String url) throws IOException {
    ObjectNode req = MAPPER.createObjectNode();
    req.put("url", url);
    return call(req, url).path("text").asText();
  }

  /** The helper writes the body to a temporary file, which is removed when the stream closes. */
  @Override public InputStream openStream(String url) throws IOException {
    File file = File.createTempFile("sciencebase-", ".download");
    boolean opened = false;
    try {
      ObjectNode req = MAPPER.createObjectNode();
      req.put("url", url);
      req.put("toFile", file.getAbsolutePath());
      long reported = call(req, url).path("bytes").asLong(-1L);
      if (reported <= 0L || reported != file.length()) {
        throw new IOException(LABEL + ": incomplete download of " + url + ": helper reported "
            + reported + " bytes, file holds " + file.length());
      }
      InputStream in = Files.newInputStream(file.toPath(), StandardOpenOption.DELETE_ON_CLOSE);
      opened = true;
      return in;
    } finally {
      if (!opened) {
        Files.deleteIfExists(file.toPath());
      }
    }
  }

  private JsonNode call(ObjectNode req, String url) throws IOException {
    JsonNode reply = helper.call(req);
    if (reply.path("status").asInt() != 200) {
      throw new IOException(LABEL + ": HTTP " + reply.path("status").asInt() + " for " + url);
    }
    return reply;
  }
}
