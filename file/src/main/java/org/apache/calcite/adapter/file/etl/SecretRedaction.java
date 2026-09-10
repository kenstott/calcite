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
package org.apache.calcite.adapter.file.etl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Strips credentials out of strings that are about to be logged or persisted as diagnostics.
 *
 * <p>Sources that authenticate with a query-string key put that key in every request URL, so any
 * diagnostic built from the URL carries it. Those strings do not stay in memory: they become log
 * lines and they become {@code error_message} rows in the pipeline tracker, which is durable and
 * readable by anyone with database access. Redacting at each call site was tried and is the wrong
 * shape — a URL reaches diagnostics from several places, and the one that gets missed is the one
 * that leaks — so redaction happens here and the callers pass everything through it.
 *
 * <p>Matching is on the parameter NAME, not on the value's shape. A key is just an opaque token;
 * there is nothing about the value itself that reliably distinguishes it from a legitimate
 * identifier, so any attempt to detect "secret-looking" values either misses real keys or redacts
 * real data. The name is the only trustworthy signal.
 */
public final class SecretRedaction {

  /**
   * Query parameters whose value is a credential. Matched case-insensitively against the whole
   * parameter name, so {@code key} does not match {@code monkey} or {@code sortkey}.
   */
  private static final Pattern CREDENTIAL_PARAM =
      Pattern.compile("([?&](?:api[_-]?key|apikey|key|token|access[_-]?token|auth|secret|"
          + "password|passwd|pwd|signature|sig)=)([^&#\\s\"']*)",
          Pattern.CASE_INSENSITIVE);

  /**
   * Header-style {@code Name: value} credentials that appear in exception text.
   *
   * <p>The auth scheme is deliberately kept out of the redacted span and preserved — knowing a
   * request sent {@code Bearer} rather than {@code Basic} is useful when reading the diagnostic
   * and is not itself a secret. Only the token after it is masked. Folding the scheme into the
   * value instead leaves the token exposed, since the scheme ends at a space and the token
   * begins after it.
   */
  private static final Pattern CREDENTIAL_HEADER =
      Pattern.compile("((?:Authorization|X-Api-Key|X-Auth-Token|Proxy-Authorization)\\s*[:=]\\s*"
          + "(?:(?:Bearer|Basic|Token|Digest|ApiKey)\\s+)?)"
          + "([^\\s,;]+)",
          Pattern.CASE_INSENSITIVE);

  private static final String MASK = "<redacted>";

  private SecretRedaction() {
  }

  /**
   * Returns {@code text} with any credential value replaced by a fixed mask, preserving the
   * parameter name so the diagnostic still says which credential was in play.
   *
   * <p>Null-safe and allocation-free when there is nothing to redact, which is the common case —
   * this sits on the error path of every HTTP call.
   */
  public static String redact(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }
    String result = text;
    Matcher param = CREDENTIAL_PARAM.matcher(result);
    if (param.find()) {
      result = param.reset().replaceAll("$1" + MASK);
    }
    Matcher header = CREDENTIAL_HEADER.matcher(result);
    if (header.find()) {
      result = header.reset().replaceAll("$1" + MASK);
    }
    return result;
  }
}
