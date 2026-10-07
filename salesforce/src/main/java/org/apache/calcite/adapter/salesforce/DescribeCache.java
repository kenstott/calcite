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
package org.apache.calcite.adapter.salesforce;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.regex.Pattern;

/**
 * Describe results of an org's sObjects, kept on disk so that they outlive the process.
 *
 * <p>Describing an sObject is one REST call, and a client that lists every column of every table
 * (a pgwire server building its catalog, a BI tool browsing the schema) makes one per sObject:
 * minutes for an org with a thousand of them. With this cache a restart reads them from disk.
 *
 * <p>Each describe response is stored verbatim as {@code <directory>/<scope>/<sObject>.json} and
 * is used until it is older than the time to live. The scope separates orgs, API versions and
 * credentials, since the fields a describe returns depend on all three.
 */
class DescribeCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(DescribeCache.class);

  /** sObject API names; anything else is not usable as a file name. */
  private static final Pattern SOBJECT_NAME = Pattern.compile("[A-Za-z0-9_]+");

  private final Path directory;
  private final Duration timeToLive;

  /** Loads the describe response of an sObject from Salesforce. */
  interface Loader {
    String load(String sObjectType) throws IOException;
  }

  /**
   * Creates a cache.
   *
   * @param directory  root directory, shared by every scope
   * @param scope      what the cached describes are specific to: org, API version, credentials
   * @param timeToLive how long a describe is used for; zero turns the disk cache off
   */
  DescribeCache(Path directory, String scope, Duration timeToLive) {
    if (timeToLive.isNegative()) {
      throw new IllegalArgumentException("Describe cache time to live is negative: " + timeToLive);
    }
    this.directory = directory.resolve(sha256(scope).substring(0, 16));
    this.timeToLive = timeToLive;
  }

  /** Returns the describe response of an sObject, from disk if a fresh one is there. */
  String get(String sObjectType, Loader loader) throws IOException {
    if (timeToLive.isZero()) {
      return loader.load(sObjectType);
    }
    if (!SOBJECT_NAME.matcher(sObjectType).matches()) {
      throw new IllegalArgumentException("Not an sObject name: " + sObjectType);
    }
    final Path file = directory.resolve(sObjectType + ".json");
    if (Files.exists(file)) {
      final long age = System.currentTimeMillis() - Files.getLastModifiedTime(file).toMillis();
      if (age < timeToLive.toMillis()) {
        LOGGER.debug("Describe of {} read from {}", sObjectType, file);
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
      }
    }
    final String json = loader.load(sObjectType);
    // Write beside the target and rename, so a reader never sees a partial file
    Files.createDirectories(directory);
    final Path temp = Files.createTempFile(directory, sObjectType, ".tmp");
    Files.write(temp, json.getBytes(StandardCharsets.UTF_8));
    Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    return json;
  }

  private static String sha256(String value) {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    final StringBuilder hex = new StringBuilder();
    for (byte b : digest.digest(value.getBytes(StandardCharsets.UTF_8))) {
      hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return hex.toString();
  }
}
