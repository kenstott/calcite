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

import java.net.SocketTimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns an object-store read timeout (DuckDB httpfs / JDK socket "Read timed out") into an
 * error that says what stalled: the object, how long one read waits, that its retries were
 * used up, how long the call spent, and what to do. The cause text is carried, with signed
 * query strings and credential-looking values removed.
 */
final class ReadStall {
  /** The server's per-read wait: DuckDBJdbcSchemaFactory's {@code http_timeout}
   *  (calcite.duckdb.http.timeout.ms, default 12000). */
  static final int READ_TIMEOUT_SECONDS = 12;
  /** The server's {@code http_retries} for one read; a failed statement is then retried once. */
  static final int READ_RETRIES = 6;

  private static final Pattern UNABLE_TO_CONNECT =
      Pattern.compile("Unable to connect to URL \"?([^\"\\s]+)");
  private static final Pattern URL = Pattern.compile("(?:https?|s3|r2|gs)://[^\\s\"'<>),]+");
  private static final Pattern FILE =
      Pattern.compile("(?<![\\w:/])(?:/|[A-Za-z]:\\\\)[^\\s\"'<>,)]*\\.(?:parquet|json|avro|duckdb)");
  private static final Pattern SECRET = Pattern.compile(
      "(?i)((?:secret|token|signature|credential|password|access[_-]?key[_-]?id|x-amz-[a-z-]+)"
      + "\\s*[=:]\\s*)[^\\s&\"']+");

  private ReadStall() {}

  /** A read timeout raised while running one step of a multi-step call, naming the step. */
  static final class StepFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;
    final String step;
    final long elapsedMillis;

    StepFailure(String step, long elapsedMillis, Throwable cause) {
      super(cause.getMessage(), cause);
      this.step = step;
      this.elapsedMillis = elapsedMillis;
    }
  }

  static boolean isReadTimeout(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
      if (t instanceof SocketTimeoutException
          || (t.getMessage() != null && t.getMessage().contains("Read timed out"))) {
        return true;
      }
    }
    return false;
  }

  /** The message for a read timeout in {@code e}'s chain, or null when it holds none. */
  static String describe(Throwable e, long elapsedMillis) {
    if (!isReadTimeout(e)) {
      return null;
    }
    String step = null;
    long elapsed = elapsedMillis;
    String object = null;
    String cause = null;
    for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
      if (t instanceof StepFailure && step == null) {
        step = ((StepFailure) t).step;
        elapsed = ((StepFailure) t).elapsedMillis;
      }
      String msg = t.getMessage();
      if (msg == null) {
        continue;
      }
      if (object == null) {
        object = objectIn(msg);
      }
      if (cause == null && msg.contains("Read timed out")) {
        cause = msg;
      }
    }
    StringBuilder sb = new StringBuilder("THE OBJECT STORE DID NOT ANSWER IN TIME (read timed "
        + "out). This is a network stall, not missing or empty data. ");
    if (step != null) {
      sb.append("Stalled step: ").append(step).append(". ");
    }
    sb.append("Object: ").append(object == null ? "object unknown" : object).append(". ");
    sb.append("One read waits ").append(READ_TIMEOUT_SECONDS).append("s before giving up; the "
        + "server retries each read ").append(READ_RETRIES).append(" times and then the whole "
        + "statement once more, so those retries were used up before this error reached you. ");
    if (elapsed >= 0) {
      sb.append("Time spent on this call: ").append(elapsed / 1000).append("s. ");
    }
    sb.append("What to do: retry the call; for a data query, narrow it with a year or state "
        + "filter so fewer objects are read. The data is static, but it is read over the "
        + "network, so a slow link or a stalled store shows up as this error. Cause: ")
        .append(sanitize(cause == null ? "Read timed out" : cause));
    return sb.toString();
  }

  /** The object a DuckDB/JDK message names, stripped of credentials, or null. */
  static String objectIn(String msg) {
    Matcher m = UNABLE_TO_CONNECT.matcher(msg);
    if (m.find()) {
      return stripUrl(m.group(1));
    }
    m = URL.matcher(msg);
    if (m.find()) {
      return stripUrl(m.group());
    }
    m = FILE.matcher(msg);
    return m.find() ? m.group() : null;
  }

  /** The URL without user info, query string or fragment (signed URLs carry secrets there). */
  static String stripUrl(String url) {
    String u = url;
    int cut = indexOfAny(u, '?', '#');
    if (cut >= 0) {
      u = u.substring(0, cut);
    }
    int scheme = u.indexOf("://");
    int at = u.indexOf('@', scheme + 3);
    int slash = u.indexOf('/', scheme + 3);
    if (scheme >= 0 && at >= 0 && (slash < 0 || at < slash)) {
      u = u.substring(0, scheme + 3) + u.substring(at + 1);
    }
    return u;
  }

  private static int indexOfAny(String s, char a, char b) {
    int i = s.indexOf(a);
    int j = s.indexOf(b);
    return i < 0 ? j : j < 0 ? i : Math.min(i, j);
  }

  /** {@code text} with every URL stripped and secret-looking values masked, and shortened. */
  static String sanitize(String text) {
    Matcher m = URL.matcher(text);
    StringBuffer out = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(out, Matcher.quoteReplacement(stripUrl(m.group())));
    }
    m.appendTail(out);
    String clean = SECRET.matcher(out).replaceAll("$1<redacted>");
    return clean.length() > 400 ? clean.substring(0, 400) + "..." : clean;
  }
}
