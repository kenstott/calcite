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
package org.apache.calcite.adapter.govdata.census;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * One headed-browser session for fetching uscis.gov workbooks (Akamai answers HTTP 403 to every
 * non-browser client). The session is a child process running the helper script: it loads the
 * data page once, then fetches each workbook from inside that page. It is started on first use,
 * shared by every fiscal year of the run and closed when the JVM exits.
 *
 * <p>Any failure to start or talk to the browser throws {@link IllegalStateException} naming the
 * cause; there is no fallback to another client.
 */
final class UscisBrowserSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(UscisBrowserSession.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** How the helper is launched; all values come from the census schema operand. */
  static final class Config {
    final String nodeCommand;
    final String script;
    final String dataPageUrl;
    final long delayMs;
    final String cacheDir;
    final String chromeExecutable;

    Config(String nodeCommand, String script, String dataPageUrl, long delayMs,
        String cacheDir, String chromeExecutable) {
      this.nodeCommand = nodeCommand;
      this.script = script;
      this.dataPageUrl = dataPageUrl;
      this.delayMs = delayMs;
      this.cacheDir = cacheDir == null ? "" : cacheDir;
      this.chromeExecutable = chromeExecutable == null ? "" : chromeExecutable;
    }

    String key() {
      return nodeCommand + '|' + script + '|' + dataPageUrl + '|' + delayMs + '|' + cacheDir
          + '|' + chromeExecutable;
    }
  }

  private static UscisBrowserSession current;

  private final Process process;
  private final BufferedWriter toHelper;
  private final BufferedReader fromHelper;
  private final String key;

  private UscisBrowserSession(Process process, String key) {
    this.process = process;
    this.key = key;
    this.toHelper = new BufferedWriter(
        new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
    this.fromHelper = new BufferedReader(
        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
  }

  /** The shared session for {@code config}, started if needed. */
  static synchronized UscisBrowserSession get(Config config) {
    if (current != null && current.key.equals(config.key()) && current.process.isAlive()) {
      return current;
    }
    if (current != null) {
      current.close();
      current = null;
    }
    if (config.nodeCommand == null || config.nodeCommand.trim().isEmpty()
        || config.dataPageUrl == null || config.dataPageUrl.trim().isEmpty()
        || config.delayMs < 0) {
      throw new IllegalStateException("uscis_i765: browser settings missing from the census "
          + "model: uscisNodeCommand, uscisDataPageUrl and uscisRequestDelayMs must be set");
    }
    if (config.script == null || config.script.trim().isEmpty()) {
      throw new IllegalStateException("uscis_i765: headed browser not configured: set "
          + "census.uscisBrowserScript (USCIS_BROWSER_SCRIPT) to the installed fetch-uscis.js");
    }
    File script = new File(config.script.trim());
    if (!script.isFile()) {
      throw new IllegalStateException("uscis_i765: browser helper script not found: "
          + script + " (run govdata/scripts/uscis-browser/install.sh)");
    }
    List<String> cmd = new ArrayList<>();
    cmd.add(config.nodeCommand);
    cmd.add(script.getPath());
    cmd.add(config.dataPageUrl);
    cmd.add(Long.toString(config.delayMs));
    cmd.add(config.cacheDir);
    cmd.add(config.chromeExecutable);
    Process p;
    try {
      p = new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.INHERIT).start();
    } catch (IOException e) {
      throw new IllegalStateException("uscis_i765: cannot start the browser helper '"
          + config.nodeCommand + "': " + e.getMessage(), e);
    }
    UscisBrowserSession s = new UscisBrowserSession(p, config.key());
    Runtime.getRuntime().addShutdownHook(new Thread(s::close, "uscis-browser-close"));
    current = s;
    LOGGER.info("uscis_i765: started headed browser helper {}", script);
    return s;
  }

  /**
   * Fetches {@code url}. Returns the workbook file, or null when the site answers 404. A cached
   * copy confirmed unchanged by the site (304) is returned as the file.
   */
  synchronized InputStream fetch(String url) throws IOException {
    ObjectNode req = MAPPER.createObjectNode();
    req.put("url", url);
    IOException writeFailure = null;
    try {
      toHelper.write(req.toString());
      toHelper.newLine();
      toHelper.flush();
    } catch (IOException e) {
      // the helper may have exited after reporting its cause on stdout; read that first
      writeFailure = e;
    }
    String line = fromHelper.readLine();
    if (line == null) {
      throw new IllegalStateException("uscis_i765: browser helper exited without a reply for "
          + url + " (exit " + exitCode() + ")"
          + (writeFailure == null ? "" : ": " + writeFailure.getMessage()), writeFailure);
    }
    JsonNode reply = MAPPER.readTree(line);
    if (reply.has("error")) {
      throw new IllegalStateException("uscis_i765: browser fetch failed: "
          + reply.get("error").asText());
    }
    int status = reply.path("status").asInt();
    if (status == 404) {
      return null;
    }
    if (status == 304) {
      LOGGER.info("uscis_i765: {} not modified (HTTP 304), using cached copy", url);
    }
    return new FileInputStream(reply.path("file").asText());
  }

  private String exitCode() {
    try {
      return Integer.toString(process.waitFor());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return "unknown";
    }
  }

  synchronized void close() {
    try {
      toHelper.close();
    } catch (IOException e) {
      LOGGER.warn("uscis_i765: closing browser helper input failed: {}", e.getMessage());
    }
    process.destroy();
  }
}
