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

import org.apache.calcite.adapter.file.storage.RotatingS3Credentials;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in and validation of in-place R2 credential rotation. */
@Tag("unit")
class R2CredentialRotatorTest {

  private static Map<String, Object> s3Config() {
    Map<String, Object> c = new HashMap<>();
    c.put("accessKeyId", "ak");
    c.put("secretAccessKey", "sk");
    c.put("sessionToken", "st");
    return c;
  }

  @Test void noRotationRequestedIsNoOp() {
    Map<String, Object> c = s3Config();
    R2CredentialRotator.startIfRequested(c);
    c.put(R2CredentialRotator.ROTATION_KEY, "");
    R2CredentialRotator.startIfRequested(c);
  }

  /** The model always carries the key; an ETL run with long-lived keys resolves the expiry
   *  placeholder to empty and must not start a rotator. */
  @Test void credentialsWithoutExpiryAreNotRotated() {
    Map<String, Object> c = s3Config();
    c.put(R2CredentialRotator.ROTATION_KEY, "r2");
    c.put(R2CredentialRotator.EXPIRES_AT_KEY, "");
    R2CredentialRotator.startIfRequested(c);
  }

  @Test void unknownModeIsRejected() {
    Map<String, Object> c = s3Config();
    c.put(R2CredentialRotator.ROTATION_KEY, "vault");
    c.put(R2CredentialRotator.EXPIRES_AT_KEY, "123");
    assertThrows(IllegalArgumentException.class, () -> R2CredentialRotator.startIfRequested(c));
  }

  @Test void expiringConfigWithoutKeysIsRejected() {
    Map<String, Object> c = new HashMap<>();
    c.put(R2CredentialRotator.ROTATION_KEY, "r2");
    c.put(R2CredentialRotator.EXPIRES_AT_KEY, "123");
    assertThrows(IllegalArgumentException.class, () -> R2CredentialRotator.startIfRequested(c));
  }

  /** R2 issues every temporary set under the parent access key id: only the secret and the
   *  session token change. */
  @Test void aFreshSetUnderTheSameAccessKeyIdIsApplied() {
    RotatingS3Credentials live = RotatingS3Credentials.of("same-id-ak", "sk1", "st1");
    java.util.List<String> applied = new java.util.ArrayList<>();
    live.onRotation(c -> applied.add(c.secretAccessKey()));
    Map<String, String> fresh = new HashMap<>();
    fresh.put("accessKeyId", "same-id-ak");
    fresh.put("secretAccessKey", "sk2");
    fresh.put("sessionToken", "st2");

    assertTrue(R2CredentialRotator.apply(live, fresh));
    assertEquals("sk2", live.secretAccessKey());
    assertEquals("st2", live.sessionToken());
    assertEquals(java.util.Collections.singletonList("sk2"), applied);

    assertFalse(R2CredentialRotator.apply(live, fresh));
    assertEquals(1, applied.size());
  }

  @Test void expiryFileIsReplacedAndRemoved(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("creds-expiry");
    Files.write(file, "1".getBytes(StandardCharsets.UTF_8));

    R2CredentialRotator.writeExpiryFile(file, "1790000000000");
    assertEquals("1790000000000", new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
    try (java.util.stream.Stream<Path> files = Files.list(dir)) {
      assertEquals(1, files.count());
    }

    R2CredentialRotator.writeExpiryFile(file, null);
    assertFalse(Files.exists(file));
  }
}
