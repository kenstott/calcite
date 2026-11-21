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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.BrowserHelperProcess;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.Map;

/**
 * One headed-browser session for fetching ssa.gov pages (the site answers HTTP 403 to every
 * non-browser client). The helper script {@code govdata/scripts/browser-fetch/fetch-page.js}
 * loads an entry page once and fetches every later URL from inside it. The session is shared by
 * every SSA table of a run and closed when the JVM exits.
 *
 * <p>Settings come from the table's source parameters ({@code ssaBrowserScript},
 * {@code ssaNodeCommand}, {@code ssaChromeExecutable}, {@code ssaRequestDelayMs},
 * {@code ssaEntryPageUrl}); a missing or unusable one throws {@link IllegalStateException}
 * naming the cause. There is no fallback to a plain HTTP client.
 *
 * <p>Concurrent workers each own a browser session, but ssa.gov sees one client address, so every
 * request is serialised across processes by an exclusive lock on a file in the temp directory and
 * spaced {@code ssaRequestDelayMs} after the previous request of any worker.
 */
final class SsaBrowserSession {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String LABEL = "ssa";
  private static final String INSTALL_HINT =
      "run govdata/scripts/browser-fetch/install.sh and set SSA_BROWSER_SCRIPT";

  private static SsaBrowserSession current;

  private static final File LOCK_FILE =
      new File(System.getProperty("java.io.tmpdir"), "govdata-ssa-fetch.lock");

  private final String key;
  private final BrowserHelperProcess helper;
  private final long delayMs;

  private SsaBrowserSession(String key, BrowserHelperProcess helper, long delayMs) {
    this.key = key;
    this.helper = helper;
    this.delayMs = delayMs;
  }

  /** The shared session for the source parameters in {@code context}, started if needed. */
  static synchronized SsaBrowserSession get(RequestContext context) {
    Map<String, String> p = context.getParameters();
    String node = require(p, "ssaNodeCommand");
    String script = p.get("ssaBrowserScript");
    String entry = require(p, "ssaEntryPageUrl");
    String delay = require(p, "ssaRequestDelayMs");
    String chrome = p.get("ssaChromeExecutable");
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
    current = new SsaBrowserSession(key, h, Long.parseLong(delay));
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

  /** The body of {@code url}, or null when ssa.gov answers 404. */
  synchronized String fetchText(String url) throws IOException {
    ObjectNode req = MAPPER.createObjectNode();
    req.put("url", url);
    JsonNode reply;
    try (RandomAccessFile file = new RandomAccessFile(LOCK_FILE, "rw");
        FileChannel channel = file.getChannel();
        FileLock lock = channel.lock()) {
      ByteBuffer stamp = ByteBuffer.allocate(Long.BYTES);
      long last = channel.read(stamp, 0) == Long.BYTES
          ? ((ByteBuffer) stamp.flip()).getLong() : 0L;
      long wait = last + delayMs - System.currentTimeMillis();
      if (wait > 0) {
        try {
          Thread.sleep(wait);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(LABEL + ": interrupted while waiting for the shared request slot", e);
        }
      }
      try {
        reply = helper.call(req);
      } finally {
        channel.write((ByteBuffer) ByteBuffer.allocate(Long.BYTES)
            .putLong(System.currentTimeMillis()).flip(), 0);
      }
    }
    if (reply.path("status").asInt() == 404) {
      return null;
    }
    return reply.path("text").asText();
  }
}
