/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.askamerica;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What a pgwire-govdata start is doing right now, read from the server's own log.
 *
 * <p>The server (pgwire-calcite) logs one {@code STAGE <name> [key=value ...]} line at each
 * start-up stage: {@code socket-claimed}, {@code backend-building}, {@code backend-built},
 * then {@code catalog-cache-hit} or {@code catalog-cache-miss} (and {@code catalog-walked}
 * after a live build), then {@code ready}. The engine tail-parses spawn.log for the last one,
 * so a call that outwaits the start can say which stage the server is in, instead of a canned
 * guess.
 */
final class ServerStartState {
  /** A log with no output for longer than this is reported as possibly wedged. */
  static final long NO_PROGRESS_MILLIS = 60_000;
  /** How much of the end of the log is read when the start's own offset is not known. */
  static final int TAIL_BYTES = 64 * 1024;

  private static final Pattern STAGE = Pattern.compile("STAGE (\\S+)([^\\r\\n]*)");
  private static final String FIRST_STAGE = "STAGE socket-claimed";

  private ServerStartState() {}

  /** The log text from {@code fromOffset} on (never more than the last {@value #TAIL_BYTES}
   *  bytes; the last {@value #TAIL_BYTES} when {@code fromOffset} is negative), cut at the
   *  last start that began in it. */
  static String readLog(File log, long fromOffset) throws IOException {
    long length = log.length();
    long from = fromOffset < 0
        ? Math.max(0, length - TAIL_BYTES)
        : Math.max(Math.min(fromOffset, length), length - TAIL_BYTES);
    byte[] bytes = new byte[(int) (length - from)];
    try (RandomAccessFile f = new RandomAccessFile(log, "r")) {
      f.seek(from);
      f.readFully(bytes);
    }
    String text = new String(bytes, StandardCharsets.UTF_8);
    int lastStart = text.lastIndexOf(FIRST_STAGE);
    return lastStart < 0 ? text : text.substring(lastStart);
  }

  /**
   * Sentences stating the start's stage and health.
   *
   * @param logText the server's log from the start of this start on
   * @param lastOutputMillis when the log file last changed
   * @param alive whether the server process is alive, or null when another process started it
   *     and so it cannot be asked
   */
  static String describe(String logText, long lastOutputMillis, long nowMillis, Boolean alive,
      String logPath) {
    String stage = null;
    String detail = "";
    String miss = null;
    Matcher m = STAGE.matcher(logText);
    while (m.find()) {
      stage = m.group(1);
      detail = m.group(2).trim();
      if ("catalog-cache-miss".equals(stage)) {
        miss = detail;
      }
    }
    StringBuilder sb = new StringBuilder();
    if (Boolean.FALSE.equals(alive)) {
      sb.append("The server process has exited. ");
    }
    if (stage == null) {
      sb.append("Its log shows no start-up stage yet, so it is still launching its runtime, "
          + "or it is a build that does not report stages.");
    } else {
      sb.append("Current stage: ").append(stageText(stage, detail, logText))
          .append('.');
    }
    if (miss != null) {
      sb.append(" DEFECT - CATALOG CACHE MISS: production must load the catalog cache that "
          + "ships with the install and never rebuild it, but this server found none it could "
          + "use and is walking the catalog live (").append(missText(miss))
          .append("). Report this with report_issue.");
    }
    long quietMillis = Math.max(0, nowMillis - lastOutputMillis);
    if (quietMillis > NO_PROGRESS_MILLIS) {
      sb.append(" Its log has had no output for ").append(quietMillis / 1000)
          .append("s, so it may be wedged.");
    } else {
      sb.append(" Its log last advanced ").append(quietMillis / 1000)
          .append("s ago, so it is progressing.");
    }
    if (alive == null) {
      sb.append(" (Another process started this server, so whether it is alive cannot be "
          + "checked from here.)");
    }
    return sb.append(" Log: ").append(logPath).toString();
  }

  private static final Pattern SCHEMA_CREATED =
      Pattern.compile("Creating government data schema '([^']+)'");

  /** Where the JVM-side model mount is, from the adapter's own "Creating government data
   *  schema" lines (one per schema; the server logs no total). */
  private static String mountProgress(String logText) {
    Matcher m = SCHEMA_CREATED.matcher(logText);
    int count = 0;
    String latest = null;
    while (m.find()) {
      count++;
      latest = m.group(1);
    }
    return logText.contains("[CALCITE] JVM started")
        ? (count == 0
            ? "JVM started, no schema created yet"
            : count + " schemas created so far, latest '" + latest + "'")
        : "JVM not started yet";
  }

  private static String stageText(String stage, String detail, String logText) {
    switch (stage) {
    case "socket-claimed":
      return "it claimed its port and is starting its query engine";
    case "backend-building":
      return "opening the Calcite connection to the model, which creates every schema's "
          + "adapter in the JVM before the server can answer (" + mountProgress(logText)
          + "; " + detail + ")";
    case "backend-built":
      return "query engine built, loading the catalog";
    case "catalog-cache-hit":
      return "catalog loaded from cache, opening for connections (" + detail + ")";
    case "catalog-cache-miss":
      return "walking the catalog live because the cache was not usable";
    case "catalog-walked":
      return "live catalog walk finished, opening for connections";
    case "ready":
      return "ready - it should answer on the next attempt";
    default:
      return stage + (detail.isEmpty() ? "" : " (" + detail + ")");
    }
  }

  private static String missText(String detail) {
    String hash = field(detail, "hash");
    return "looked for catalog-cache-" + hash + ".pkl (model hash " + hash + ") at: "
        + field(detail, "looked") + "; files that existed but could not be loaded: "
        + field(detail, "unloadable");
  }

  private static String field(String detail, String key) {
    Matcher m = Pattern.compile("\\b" + key + "=(\\S*)").matcher(detail);
    return m.find() ? m.group(1) : "unknown";
  }
}
