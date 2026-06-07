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
package org.apache.calcite.adapter.file.metadata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RemoteFileMetadata}.
 */
@Tag("unit")
class RemoteFileMetadataTest {

  @Test void testCreateForTesting() {
    RemoteFileMetadata meta =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", "etag123", "Mon, 01 Jan 2024 00:00:00 GMT",
        1024L, "abc123hash");

    assertEquals("https://example.com/data.csv", meta.getUrl());
    assertEquals("etag123", meta.getEtag());
    assertEquals("Mon, 01 Jan 2024 00:00:00 GMT", meta.getLastModified());
    assertEquals(1024L, meta.getContentLength());
    assertEquals("abc123hash", meta.getContentHash());
    assertNotNull(meta.getCheckTime());
  }

  @Test void testCreateForTestingWithNulls() {
    RemoteFileMetadata meta =
        RemoteFileMetadata.createForTesting("s3://bucket/file.parquet", null, null, -1, null);

    assertEquals("s3://bucket/file.parquet", meta.getUrl());
    assertNull(meta.getEtag());
    assertNull(meta.getLastModified());
    assertEquals(-1, meta.getContentLength());
    assertNull(meta.getContentHash());
  }

  @Test void testHasChangedWithEtag() {
    RemoteFileMetadata current =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", "etag-v2", null, 0, null);
    RemoteFileMetadata previous =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", "etag-v1", null, 0, null);

    assertTrue(current.hasChanged(previous));
  }

  @Test void testHasNotChangedWithSameEtag() {
    RemoteFileMetadata current =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", "etag-v1", null, 0, null);
    RemoteFileMetadata previous =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", "etag-v1", null, 0, null);

    assertFalse(current.hasChanged(previous));
  }

  @Test void testHasChangedWithLastModified() {
    RemoteFileMetadata current =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, "Tue, 02 Jan 2024 00:00:00 GMT", 0, null);
    RemoteFileMetadata previous =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, "Mon, 01 Jan 2024 00:00:00 GMT", 0, null);

    assertTrue(current.hasChanged(previous));
  }

  @Test void testHasNotChangedWithSameLastModified() {
    RemoteFileMetadata current =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, "Mon, 01 Jan 2024 00:00:00 GMT", 0, null);
    RemoteFileMetadata previous =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, "Mon, 01 Jan 2024 00:00:00 GMT", 0, null);

    assertFalse(current.hasChanged(previous));
  }

  @Test void testHasChangedWithContentLength() {
    RemoteFileMetadata current =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, null, 2048, null);
    RemoteFileMetadata previous =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, null, 1024, null);

    assertTrue(current.hasChanged(previous));
  }

  @Test void testHasChangedWithContentHash() {
    RemoteFileMetadata current =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, null, 0, "hash_new");
    RemoteFileMetadata previous =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, null, 0, "hash_old");

    assertTrue(current.hasChanged(previous));
  }

  @Test void testHasNotChangedWithSameContentHash() {
    RemoteFileMetadata current =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, null, 0, "same_hash");
    RemoteFileMetadata previous =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, null, 0, "same_hash");

    assertFalse(current.hasChanged(previous));
  }

  @Test void testHasChangedWithNoMetadata() {
    // When nothing is available to compare, assume changed
    RemoteFileMetadata current =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, null, -1, null);
    RemoteFileMetadata previous =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", null, null, -1, null);

    assertTrue(current.hasChanged(previous));
  }

  @Test void testWithContentHash() {
    RemoteFileMetadata original =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", "etag1", "Mon", 100, null);

    RemoteFileMetadata withHash = original.withContentHash("newhash");

    assertEquals("https://example.com/data.csv", withHash.getUrl());
    assertEquals("etag1", withHash.getEtag());
    assertEquals("Mon", withHash.getLastModified());
    assertEquals(100, withHash.getContentLength());
    assertEquals("newhash", withHash.getContentHash());
  }

  @Test void testComputeHash() throws IOException {
    String content = "Hello, World!";
    ByteArrayInputStream is =
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

    String hash = RemoteFileMetadata.computeHash(is);
    assertNotNull(hash);
    assertEquals(32, hash.length()); // MD5 hash in hex = 32 chars
  }

  @Test void testComputeHashDeterministic() throws IOException {
    String content = "Hello, World!";
    ByteArrayInputStream is1 =
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    ByteArrayInputStream is2 =
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

    String hash1 = RemoteFileMetadata.computeHash(is1);
    String hash2 = RemoteFileMetadata.computeHash(is2);
    assertEquals(hash1, hash2);
  }

  @Test void testComputeHashDifferentContent() throws IOException {
    ByteArrayInputStream is1 =
        new ByteArrayInputStream("content1".getBytes(StandardCharsets.UTF_8));
    ByteArrayInputStream is2 =
        new ByteArrayInputStream("content2".getBytes(StandardCharsets.UTF_8));

    String hash1 = RemoteFileMetadata.computeHash(is1);
    String hash2 = RemoteFileMetadata.computeHash(is2);
    assertFalse(hash1.equals(hash2));
  }

  @Test void testComputeHashEmptyStream() throws IOException {
    ByteArrayInputStream is = new ByteArrayInputStream(new byte[0]);
    String hash = RemoteFileMetadata.computeHash(is);
    assertNotNull(hash);
    assertEquals(32, hash.length());
  }

  @Test void testToString() {
    RemoteFileMetadata meta =
        RemoteFileMetadata.createForTesting("https://example.com/data.csv", "etag1", "Mon", 100, "hash");
    String str = meta.toString();
    assertNotNull(str);
    assertTrue(str.contains("https://example.com/data.csv"));
    assertTrue(str.contains("etag1"));
  }

  @Test void testEtagPrecedenceOverLastModified() {
    // When etag matches, don't check lastModified even if different
    RemoteFileMetadata current =
        RemoteFileMetadata.createForTesting("url", "same_etag", "different_date", 0, null);
    RemoteFileMetadata previous =
        RemoteFileMetadata.createForTesting("url", "same_etag", "original_date", 0, null);

    assertFalse(current.hasChanged(previous));
  }
}
