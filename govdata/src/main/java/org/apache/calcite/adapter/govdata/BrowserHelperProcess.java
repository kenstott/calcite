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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A browser-automation helper run as a child process that speaks one JSON request and one JSON
 * reply per line on stdin/stdout (the scripts under {@code govdata/scripts/*-browser/}). It is
 * started once, closed when the JVM exits, and every failure to start or to talk to it throws
 * {@link IllegalStateException} naming the cause; there is no fallback to another client.
 */
public final class BrowserHelperProcess {

  private static final Logger LOGGER = LoggerFactory.getLogger(BrowserHelperProcess.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String label;
  private final Process process;
  private final BufferedWriter toHelper;
  private final BufferedReader fromHelper;

  private BrowserHelperProcess(String label, Process process) {
    this.label = label;
    this.process = process;
    this.toHelper = new BufferedWriter(
        new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
    this.fromHelper = new BufferedReader(
        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
  }

  /**
   * Starts {@code nodeCommand script args...}. {@code label} prefixes every error message.
   * The script must exist; the cause of a missing one names {@code installHint}.
   */
  public static BrowserHelperProcess start(String label, String nodeCommand, String script,
      String installHint, List<String> args) {
    if (nodeCommand == null || nodeCommand.trim().isEmpty()) {
      throw new IllegalStateException(label + ": node command missing from the schema model");
    }
    if (script == null || script.trim().isEmpty()) {
      throw new IllegalStateException(label + ": browser helper script not configured; "
          + installHint);
    }
    File file = new File(script.trim());
    if (!file.isFile()) {
      throw new IllegalStateException(label + ": browser helper script not found: " + file
          + " (" + installHint + ")");
    }
    List<String> cmd = new ArrayList<String>();
    cmd.add(nodeCommand.trim());
    cmd.add(file.getPath());
    cmd.addAll(args);
    Process p;
    try {
      p = new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.INHERIT).start();
    } catch (IOException e) {
      throw new IllegalStateException(label + ": cannot start the browser helper '"
          + nodeCommand + "': " + e.getMessage(), e);
    }
    final BrowserHelperProcess helper = new BrowserHelperProcess(label, p);
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override public void run() {
        helper.close();
      }
    }, "browser-helper-close"));
    LOGGER.info("{}: started browser helper {}", label, file);
    return helper;
  }

  public boolean isAlive() {
    return process.isAlive();
  }

  /** Sends {@code request}; returns the reply, throwing when the helper reports an error. */
  public synchronized JsonNode call(ObjectNode request) throws IOException {
    IOException writeFailure = null;
    try {
      toHelper.write(request.toString());
      toHelper.newLine();
      toHelper.flush();
    } catch (IOException e) {
      // the helper may have exited after reporting its cause on stdout; read that first
      writeFailure = e;
    }
    String line = fromHelper.readLine();
    if (line == null) {
      throw new IllegalStateException(label + ": browser helper exited without a reply (exit "
          + exitCode() + ")" + (writeFailure == null ? "" : ": " + writeFailure.getMessage()),
          writeFailure);
    }
    JsonNode reply = MAPPER.readTree(line);
    if (reply.has("error")) {
      throw new IllegalStateException(label + ": browser helper: " + reply.get("error").asText());
    }
    return reply;
  }

  private String exitCode() {
    try {
      return Integer.toString(process.waitFor());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return "unknown";
    }
  }

  /** Closes the helper's input so it shuts its browser down, and waits for it to exit. */
  public synchronized void close() {
    try {
      toHelper.close();
    } catch (IOException e) {
      LOGGER.warn("{}: closing browser helper input failed: {}", label, e.getMessage());
    }
    try {
      if (!process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS)) {
        LOGGER.warn("{}: browser helper did not exit within 60s; destroying it", label);
        process.destroy();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      process.destroy();
    }
  }
}
