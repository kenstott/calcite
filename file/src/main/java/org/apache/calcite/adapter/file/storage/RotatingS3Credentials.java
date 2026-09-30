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
package org.apache.calcite.adapter.file.storage;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * A live S3 credential set, shared process-wide by every consumer that captured the same
 * access key, and replaceable in place.
 *
 * <p>Short-lived (session) credentials reach a long-running process once, as literal operand
 * strings. Each consumer that copied them — the S3 SDK client, Iceberg's S3FileIO, a DuckDB
 * secret — would keep signing with them after they expire. Consumers instead obtain the set
 * through {@link #of} and read it on every use (or register {@link #onRotation} to re-apply it
 * where the credentials are pushed, not pulled); {@link #rotate} replaces it for all of them.
 *
 * <p>Every access key the set has ever carried stays mapped to it, so a consumer created after a
 * rotation from the original, now-expired operand strings still gets the current credentials.
 *
 * <p>Also an Iceberg {@code client.credentials-provider}: {@link #create(Map)} looks the set up by
 * the {@code accessKeyId} property.
 */
public final class RotatingS3Credentials implements AwsCredentialsProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(RotatingS3Credentials.class);

  /** The property Iceberg passes to {@link #create(Map)}, prefix stripped. */
  public static final String ACCESS_KEY_ID_PROPERTY = "accessKeyId";

  private static final Map<String, RotatingS3Credentials> BY_ACCESS_KEY_ID =
      new ConcurrentHashMap<>();

  private volatile AwsCredentials current;
  private final List<Consumer<AwsCredentials>> listeners = new CopyOnWriteArrayList<>();

  private RotatingS3Credentials(AwsCredentials initial) {
    this.current = initial;
  }

  /**
   * Returns the live set that {@code accessKeyId} belongs to, creating it from these values when
   * the key has not been seen.
   */
  public static RotatingS3Credentials of(String accessKeyId, String secretAccessKey,
      @Nullable String sessionToken) {
    return BY_ACCESS_KEY_ID.computeIfAbsent(accessKeyId,
        k -> new RotatingS3Credentials(credentials(k, secretAccessKey, sessionToken)));
  }

  /** Iceberg {@code client.credentials-provider} factory. The set must already exist. */
  public static RotatingS3Credentials create(Map<String, String> properties) {
    String accessKeyId = properties.get(ACCESS_KEY_ID_PROPERTY);
    RotatingS3Credentials found = accessKeyId == null ? null : BY_ACCESS_KEY_ID.get(accessKeyId);
    if (found == null) {
      throw new IllegalStateException("No live S3 credential set for access key "
          + accessKeyId + "; it must be registered via RotatingS3Credentials.of first");
    }
    return found;
  }

  /**
   * Replaces the set {@code fromAccessKeyId} belongs to with new credentials and re-applies them
   * through every registered listener.
   *
   * @return false when no consumer ever registered {@code fromAccessKeyId}
   * @throws IllegalStateException when a listener failed to apply the new credentials (after
   *     every listener has been tried)
   */
  public static boolean rotate(String fromAccessKeyId, String accessKeyId,
      String secretAccessKey, @Nullable String sessionToken) {
    RotatingS3Credentials set;
    synchronized (BY_ACCESS_KEY_ID) {
      set = BY_ACCESS_KEY_ID.get(fromAccessKeyId);
      if (set == null) {
        return false;
      }
      RotatingS3Credentials existing = BY_ACCESS_KEY_ID.putIfAbsent(accessKeyId, set);
      if (existing != null && existing != set) {
        throw new IllegalStateException("Access key " + accessKeyId
            + " already belongs to a different credential set");
      }
      set.current = credentials(accessKeyId, secretAccessKey, sessionToken);
    }
    set.notifyListeners();
    return true;
  }

  /** Registers {@code listener} to re-apply the credentials each time they are rotated. */
  public void onRotation(Consumer<AwsCredentials> listener) {
    listeners.add(listener);
  }

  @Override public AwsCredentials resolveCredentials() {
    return current;
  }

  public String accessKeyId() {
    return current.accessKeyId();
  }

  public String secretAccessKey() {
    return current.secretAccessKey();
  }

  /** The session token, or null for a long-lived key pair. */
  public @Nullable String sessionToken() {
    AwsCredentials c = current;
    return c instanceof AwsSessionCredentials ? ((AwsSessionCredentials) c).sessionToken() : null;
  }

  private void notifyListeners() {
    AwsCredentials applied = current;
    IllegalStateException failure = null;
    for (Consumer<AwsCredentials> listener : listeners) {
      try {
        listener.accept(applied);
      } catch (RuntimeException e) {
        LOGGER.error("Could not apply rotated S3 credentials to {}", listener, e);
        if (failure == null) {
          failure = new IllegalStateException("Rotated S3 credentials were not applied to "
              + "every consumer", e);
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static AwsCredentials credentials(String accessKeyId, String secretAccessKey,
      @Nullable String sessionToken) {
    return sessionToken != null && !sessionToken.isEmpty()
        ? AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken)
        : AwsBasicCredentials.create(accessKeyId, secretAccessKey);
  }
}
