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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.storage.RotatingS3Credentials;

import org.apache.iceberg.aws.s3.S3FileIO;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A loaded Iceberg table keeps its S3FileIO for the process's life, so the FileIO's client must
 *  sign with the live credential set, not the one it was built with. */
@Tag("unit")
class S3FileIOTablesRotatingCredentialsTest {

  private static Map<String, String> config(String accessKeyId) {
    Map<String, String> c = new HashMap<>();
    c.put("accessKeyId", accessKeyId);
    c.put("secretAccessKey", "s1");
    c.put("sessionToken", "t1");
    c.put("endpoint", "http://127.0.0.1:1");
    c.put("region", "auto");
    return c;
  }

  @Test void propertiesUseLiveProviderNotStaticKeys() {
    String original = "orig-" + UUID.randomUUID();
    Map<String, String> props = S3FileIOTables.toS3Properties(config(original));
    assertFalse(props.containsKey("s3.access-key-id"));
    assertFalse(props.containsKey("s3.secret-access-key"));
    assertFalse(props.containsKey("s3.session-token"));
    assertEquals(RotatingS3Credentials.class.getName(), props.get("client.credentials-provider"));
    assertEquals(original, props.get("client.credentials-provider.accessKeyId"));
    assertTrue(props.containsKey("s3.endpoint"));
  }

  @Test void fileIoClientSignsWithRotatedCredentials() {
    String original = "orig-" + UUID.randomUUID();
    try (S3FileIO io = S3FileIOTables.newIO(config(original))) {
      AwsCredentials before = resolve(io);
      assertEquals(original, before.accessKeyId());
      assertEquals("t1", ((AwsSessionCredentials) before).sessionToken());

      String next = "next-" + UUID.randomUUID();
      RotatingS3Credentials.rotate(original, next, "s2", "t2");

      AwsCredentials after = resolve(io);
      assertEquals(next, after.accessKeyId());
      assertEquals("s2", after.secretAccessKey());
      assertEquals("t2", ((AwsSessionCredentials) after).sessionToken());
    }
  }

  private static AwsCredentials resolve(S3FileIO io) {
    ClassLoader prior = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(S3FileIOTables.class.getClassLoader());
    try {
      return (AwsCredentials) io.client().serviceClientConfiguration().credentialsProvider()
          .resolveIdentity().join();
    } finally {
      Thread.currentThread().setContextClassLoader(prior);
    }
  }
}
