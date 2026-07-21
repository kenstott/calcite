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
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Provides R2 credentials for the govdata JDBC driver.
 *
 * <p>Resolution order:
 * <ol>
 *   <li>Disk cache at {@code ~/.askamerica/credentials.json}</li>
 *   <li>Baked-in defaults from classpath resource {@code /config/r2-defaults.json}</li>
 * </ol>
 *
 * <p>On S3 auth failure, call {@link #refresh(String)} with the caller's
 * {@code ASKAMERICA_API_KEY} to fetch fresh credentials, update the disk cache, and retry.
 */
public final class R2CredentialProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(R2CredentialProvider.class);

  private static final String CREDENTIALS_URL =
      "https://askamerica.ai/v1/catalog/credentials";
  private static final String CACHE_FILE =
      System.getProperty("user.home") + "/.askamerica/credentials.json";
  private static final String RESOURCE_PATH = "/config/r2-defaults.json";
  private static final int TIMEOUT_MS = 10_000;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, String>> MAP_TYPE =
      new TypeReference<Map<String, String>>() {};

  private R2CredentialProvider() {}

  /**
   * Returns the best available credentials without making a network call.
   * Disk cache takes priority over baked-in defaults.
   */
  public static Map<String, String> resolve() {
    Map<String, String> cached = loadDiskCache();
    if (cached != null) {
      LOGGER.debug("R2CredentialProvider: using disk-cached credentials");
      return cached;
    }
    LOGGER.debug("R2CredentialProvider: using baked-in default credentials");
    return loadDefaults();
  }

  /**
   * Fetches fresh credentials from the AskAmerica API, writes them to disk cache, and returns them.
   *
   * @param apiKey caller's ASKAMERICA_API_KEY
   * @throws IOException if the HTTP call fails or returns non-200
   */
  public static Map<String, String> refresh(String apiKey) throws IOException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IOException("Cannot refresh credentials: ASKAMERICA_API_KEY is not set");
    }
    LOGGER.info("R2CredentialProvider: refreshing credentials from {}", CREDENTIALS_URL);

    HttpURLConnection conn = (HttpURLConnection) URI.create(CREDENTIALS_URL).toURL().openConnection();
    try {
      conn.setRequestMethod("GET");
      conn.setRequestProperty("X-API-Key", apiKey);
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);

      int status = conn.getResponseCode();
      if (status != 200) {
        throw new IOException("Credential refresh failed: HTTP " + status);
      }

      byte[] body;
      try (InputStream in = conn.getInputStream()) {
        body = readFully(in);
      }

      Map<String, Object> raw = MAPPER.readValue(body, new TypeReference<Map<String, Object>>() {});
      Map<String, String> creds = remapFromApi(raw);

      if (!isComplete(creds)) {
        throw new IOException("Credential refresh returned incomplete credentials");
      }

      writeDiskCache(creds);
      LOGGER.info("R2CredentialProvider: credentials refreshed and cached");
      return creds;
    } finally {
      conn.disconnect();
    }
  }

  private static Map<String, String> loadDiskCache() {
    try {
      File f = new File(CACHE_FILE);
      if (!f.exists()) {
        return null;
      }
      Map<String, String> creds = MAPPER.readValue(f, MAP_TYPE);
      return isComplete(creds) ? creds : null;
    } catch (Exception e) {
      LOGGER.debug("R2CredentialProvider: could not read disk cache: {}", e.getMessage());
      return null;
    }
  }

  private static Map<String, String> loadDefaults() {
    try (InputStream in = R2CredentialProvider.class.getResourceAsStream(RESOURCE_PATH)) {
      if (in == null) {
        throw new IllegalStateException(
            "R2 defaults resource not found at " + RESOURCE_PATH
            + " — JAR was not built with credentials embedded");
      }
      Map<String, String> creds = MAPPER.readValue(in, MAP_TYPE);
      if (!isComplete(creds)) {
        throw new IllegalStateException(
            "R2 defaults resource is missing required fields");
      }
      return creds;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load R2 defaults: " + e.getMessage(), e);
    }
  }

  private static void writeDiskCache(Map<String, String> creds) {
    try {
      Path cacheDir = Paths.get(System.getProperty("user.home"), ".askamerica");
      Files.createDirectories(cacheDir);

      Path tmp = Files.createTempFile(cacheDir, "creds-", ".tmp");
      try {
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), creds);
        Files.move(tmp, Paths.get(CACHE_FILE), StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
        try {
          Files.setPosixFilePermissions(Paths.get(CACHE_FILE),
              PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
          // non-POSIX filesystem (Windows)
        }
      } catch (Exception e) {
        Files.deleteIfExists(tmp);
        throw e;
      }
    } catch (Exception e) {
      LOGGER.warn("R2CredentialProvider: failed to write credential cache: {}", e.getMessage());
    }
  }

  private static Map<String, String> remapFromApi(Map<String, Object> raw) {
    Map<String, String> creds = new LinkedHashMap<>();
    creds.put("accessKeyId",     stringVal(raw, "access_key_id"));
    creds.put("secretAccessKey", stringVal(raw, "secret_access_key"));
    creds.put("endpoint",        stringVal(raw, "endpoint"));
    creds.put("region",          stringVal(raw, "region", "auto"));
    // Temporary (scoped, short-lived) credentials also carry a session token.
    String sess = stringVal(raw, "session_token");
    if (sess != null && !sess.isEmpty()) {
      creds.put("sessionToken", sess);
    }
    return creds;
  }

  private static String stringVal(Map<String, Object> map, String key) {
    Object v = map.get(key);
    return v != null ? v.toString() : null;
  }

  private static String stringVal(Map<String, Object> map, String key, String defaultVal) {
    String v = stringVal(map, key);
    return (v != null && !v.isEmpty()) ? v : defaultVal;
  }

  private static boolean isComplete(Map<String, String> creds) {
    for (String field : new String[]{"accessKeyId", "secretAccessKey", "endpoint", "region"}) {
      String v = creds.get(field);
      if (v == null || v.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private static byte[] readFully(InputStream in) throws IOException {
    byte[] buf = new byte[4096];
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }
}
