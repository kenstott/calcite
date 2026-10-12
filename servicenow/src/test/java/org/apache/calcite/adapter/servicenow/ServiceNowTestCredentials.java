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
package org.apache.calcite.adapter.servicenow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Instance credentials for the live tests, read from {@code govdata/.env.prod} like the
 * Salesforce tests read theirs: {@code SN_INSTANCE_URL} (for example
 * {@code https://dev12345.service-now.com}), {@code SN_USERNAME} and {@code SN_PASSWORD}.
 *
 * <p>Unlike the Salesforce tests, a missing file or missing keys do not fail the run: no instance
 * is available to most checkouts, so {@link #load} returns null and the live tests skip
 * themselves. A file that is present but incomplete is an error.
 */
final class ServiceNowTestCredentials {
  final String instanceUrl;
  final String username;
  final String password;

  private ServiceNowTestCredentials(String instanceUrl, String username, String password) {
    this.instanceUrl = instanceUrl;
    this.username = username;
    this.password = password;
  }

  /** Returns the credentials, or null if none are configured. */
  static ServiceNowTestCredentials load() throws IOException {
    final String rootDir = System.getProperty("gradle.rootDir");
    if (rootDir == null) {
      throw new IllegalStateException("gradle.rootDir system property is not set");
    }
    final Path envFile = Paths.get(rootDir, "govdata", ".env.prod");
    if (!Files.exists(envFile)) {
      return null;
    }
    final Map<String, String> env = new HashMap<>();
    for (String raw : Files.readAllLines(envFile, StandardCharsets.UTF_8)) {
      String line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      if (line.startsWith("export ")) {
        line = line.substring("export ".length()).trim();
      }
      final int eq = line.indexOf('=');
      if (eq <= 0) {
        continue;
      }
      String value = line.substring(eq + 1).trim();
      if (value.length() >= 2 && (value.startsWith("\"") && value.endsWith("\"")
          || value.startsWith("'") && value.endsWith("'"))) {
        value = value.substring(1, value.length() - 1);
      }
      env.put(line.substring(0, eq).trim(), value);
    }
    final String[] keys = {"SN_INSTANCE_URL", "SN_USERNAME", "SN_PASSWORD"};
    int present = 0;
    for (String key : keys) {
      if (env.containsKey(key)) {
        present++;
      }
    }
    if (present == 0) {
      return null;
    }
    for (String key : keys) {
      if (!env.containsKey(key)) {
        throw new IllegalStateException(key + " is missing from " + envFile);
      }
    }
    final String url = env.get("SN_INSTANCE_URL");
    return new ServiceNowTestCredentials(
        url.startsWith("https://") ? url : "https://" + url,
        env.get("SN_USERNAME"), env.get("SN_PASSWORD"));
  }

  /** The operands of a schema for this instance. */
  Map<String, Object> operand(Path catalogCacheDirectory) {
    final Map<String, Object> operand = new HashMap<>();
    operand.put("instanceUrl", instanceUrl);
    operand.put("authType", "basic");
    operand.put("username", username);
    operand.put("password", password);
    operand.put("catalogCacheDirectory", catalogCacheDirectory.toString());
    return operand;
  }
}
