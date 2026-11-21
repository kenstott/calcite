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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.BrowserHelperProcess;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Base64;
import java.util.Arrays;
import java.util.Map;

/**
 * One headed-browser session for fetching pages and files from federal sites that refuse
 * non-browser clients (highways.dot.gov answers HTTP 403, acf.gov answers an AWS WAF challenge). The helper script
 * {@code govdata/scripts/browser-fetch/fetch-page.js} loads an entry page once and fetches every
 * later URL from inside it. Settings come from the table's source parameters; a missing or
 * unusable one throws {@link IllegalStateException} naming the cause. There is no fallback to a
 * plain HTTP client.
 */
final class FiscalBrowserSession {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String LABEL = "browser";
  private static final String INSTALL_HINT =
      "run govdata/scripts/browser-fetch/install.sh and set SSA_BROWSER_SCRIPT";

  private static FiscalBrowserSession current;

  private final String key;
  private final BrowserHelperProcess helper;

  private FiscalBrowserSession(String key, BrowserHelperProcess helper) {
    this.key = key;
    this.helper = helper;
  }

  /** The shared session for the source parameters in {@code context}, started if needed. */
  static synchronized FiscalBrowserSession get(RequestContext context) {
    Map<String, String> p = context.getParameters();
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
    current = new FiscalBrowserSession(key, h);
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

  /** The body of {@code url}, or null when the site answers 404. */
  synchronized String fetchText(String url) throws IOException {
    ObjectNode req = MAPPER.createObjectNode();
    req.put("url", url);
    JsonNode reply = helper.call(req);
    if (reply.path("status").asInt() == 404) {
      return null;
    }
    return reply.path("text").asText();
  }

  /** The bytes of {@code url}, or null when the site answers 404. */
  synchronized byte[] fetchBytes(String url) throws IOException {
    ObjectNode req = MAPPER.createObjectNode();
    req.put("url", url);
    req.put("binary", true);
    JsonNode reply = helper.call(req);
    if (reply.path("status").asInt() == 404) {
      return null;
    }
    return Base64.getDecoder().decode(reply.path("base64").asText());
  }
}
