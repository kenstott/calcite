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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsCredentials;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** In-place rotation of short-lived S3 credentials shared by every consumer. */
@Tag("unit")
class RotatingS3CredentialsTest {

  /** Unique per test: the registry is process-wide. */
  private static String key(String label) {
    return label + "-" + UUID.randomUUID();
  }

  @Test void rotationReachesEveryHolderAndListener() {
    String original = key("orig");
    RotatingS3Credentials set = RotatingS3Credentials.of(original, "s1", "t1");
    List<String> applied = new ArrayList<>();
    set.onRotation(c -> applied.add(c.accessKeyId()));

    String next = key("next");
    assertTrue(RotatingS3Credentials.rotate(original, next, "s2", "t2"));

    assertEquals(next, set.accessKeyId());
    assertEquals("s2", set.secretAccessKey());
    assertEquals("t2", set.sessionToken());
    assertEquals(Collections.singletonList(next), applied);
  }

  /** A consumer built after a rotation from the original, now-expired operand strings must get
   *  the current set, not a fresh copy of the expired one. */
  @Test void originalKeyResolvesToRotatedSet() {
    String original = key("orig");
    RotatingS3Credentials set = RotatingS3Credentials.of(original, "s1", "t1");
    String second = key("second");
    RotatingS3Credentials.rotate(original, second, "s2", "t2");
    String third = key("third");
    RotatingS3Credentials.rotate(second, third, "s3", null);

    RotatingS3Credentials late = RotatingS3Credentials.of(original, "s1", "t1");
    assertSame(set, late);
    assertEquals(third, late.accessKeyId());
    assertNull(late.sessionToken());
  }

  @Test void icebergFactoryFindsSetByAccessKey() {
    String original = key("orig");
    RotatingS3Credentials set = RotatingS3Credentials.of(original, "s1", null);
    Map<String, String> props = new HashMap<>();
    props.put(RotatingS3Credentials.ACCESS_KEY_ID_PROPERTY, original);
    assertSame(set, RotatingS3Credentials.create(props));

    props.put(RotatingS3Credentials.ACCESS_KEY_ID_PROPERTY, key("unknown"));
    assertThrows(IllegalStateException.class, () -> RotatingS3Credentials.create(props));
  }

  @Test void rotatingUnknownKeyReportsNothingRotated() {
    assertFalse(RotatingS3Credentials.rotate(key("unknown"), key("next"), "s", null));
  }

  /** One consumer that cannot take the new set must not stop the rest from getting it, and the
   *  failure must surface rather than be logged away. */
  @Test void failedListenerDoesNotStopOthersAndIsReported() {
    String original = key("orig");
    RotatingS3Credentials set = RotatingS3Credentials.of(original, "s1", null);
    List<AwsCredentials> applied = new ArrayList<>();
    set.onRotation(c -> {
      throw new IllegalArgumentException("secret store unavailable");
    });
    set.onRotation(applied::add);

    String next = key("next");
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> RotatingS3Credentials.rotate(original, next, "s2", null));
    assertEquals("secret store unavailable", e.getCause().getMessage());
    assertEquals(1, applied.size());
    assertEquals(next, set.accessKeyId());
  }

  /** The S3 SDK client and every consumer that copies getS3Config() see the rotated set. */
  @Test void storageProviderServesRotatedCredentials() {
    String original = key("orig");
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", original);
    config.put("secretAccessKey", "s1");
    config.put("sessionToken", "t1");
    config.put("endpoint", "http://127.0.0.1:1");
    config.put("region", "auto");
    S3StorageProvider provider = new S3StorageProvider(config);

    String next = key("next");
    RotatingS3Credentials.rotate(original, next, "s2", "t2");

    Map<String, String> s3Config = provider.getS3Config();
    assertEquals(next, s3Config.get("accessKeyId"));
    assertEquals("s2", s3Config.get("secretAccessKey"));
    assertEquals("t2", s3Config.get("sessionToken"));
    assertEquals("http://127.0.0.1:1", s3Config.get("endpoint"));
  }
}
