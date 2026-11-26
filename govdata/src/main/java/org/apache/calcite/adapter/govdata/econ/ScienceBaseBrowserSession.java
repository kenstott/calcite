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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.govdata.BrowserHelperProcess;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
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
final class ScienceBaseBrowserSession implements UsgsMineralCommodityStatsProvider.Client {

  private static final ObjectMapper MAPPER = new ObjectMapper();
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

  /** The shared session for the source parameters {@code p}, started if needed. */
  static synchronized ScienceBaseBrowserSession get(Map<String, String> p) {
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
    return call(url, false).path("text").asText();
  }

  /** The whole file is held in memory: the helper replies with one JSON line per request. */
  @Override public byte[] fetchBytes(String url) throws IOException {
    return Base64.getDecoder().decode(call(url, true).path("base64").asText());
  }

  private synchronized JsonNode call(String url, boolean binary) throws IOException {
    ObjectNode req = MAPPER.createObjectNode();
    req.put("url", url);
    req.put("binary", binary);
    JsonNode reply = helper.call(req);
    if (reply.path("status").asInt() != 200) {
      throw new IOException(LABEL + ": HTTP " + reply.path("status").asInt() + " for " + url);
    }
    return reply;
  }
}
