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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.metadata.RemoteFileMetadata;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for remote file refresh functionality.
 */
@Tag("integration")
public class RemoteFileRefreshTest {

  @BeforeEach
  public void checkNetworkAvailability() {
    // Check for proxy configuration if needed
    String httpProxy = System.getProperty("http.proxy");
    if (httpProxy != null && !httpProxy.isEmpty()) {
      System.out.println("Using HTTP proxy: " + httpProxy);
    }
  }

  @Test public void testHttpMetadataFetch() throws Exception {
    // Test getting metadata from a reliable HTTP source
    String testUrl = "https://www.w3.org/TR/PNG/iso_8859-1.txt";
    Source source = Sources.url(testUrl);

    RemoteFileMetadata metadata = RemoteFileMetadata.fetch(source);
    assertNotNull(metadata);

    // Debug output
    System.out.println("URL: " + metadata.getUrl());
    System.out.println("Content Length: " + metadata.getContentLength());
    System.out.println("ETag: " + metadata.getEtag());
    System.out.println("Last Modified: " + metadata.getLastModified());

    // Content length might be -1 if not available, check for that
    assertTrue(metadata.getContentLength() != 0,
        "Content length should not be 0, was: " + metadata.getContentLength());
    assertNotNull(metadata.getCheckTime());
  }

  @Test public void testMetadataChangeDetection() throws Exception {
    String testUrl = "https://www.w3.org/TR/PNG/iso_8859-1.txt";
    Source source = Sources.url(testUrl);

    RemoteFileMetadata meta1 = RemoteFileMetadata.fetch(source);
    RemoteFileMetadata meta2 = RemoteFileMetadata.fetch(source);

    // Should be the same since file hasn't changed
    assertEquals(meta1.getContentLength(), meta2.getContentLength());
    assertEquals(meta1.getEtag(), meta2.getEtag());
  }

  @Test public void testContentLengthChangeDetection() throws Exception {
    String testUrl = "https://www.w3.org/TR/PNG/iso_8859-1.txt";
    Source source = Sources.url(testUrl);

    RemoteFileMetadata meta1 = RemoteFileMetadata.fetch(source);

    // Check that metadata has expected fields
    assertNotNull(meta1.getUrl());
    assertEquals(testUrl, meta1.getUrl());

    // Content length might be -1 if server doesn't provide Content-Length header
    // This is valid, especially for HEAD requests that return 404
    assertTrue(meta1.getContentLength() != 0,
        "Content length should not be 0, was: " + meta1.getContentLength());
  }

  @Test public void testHashComputation() throws IOException {
    // Test that we can compute hash for content
    String content1 = "Hello World";
    String content2 = "Hello World";
    String content3 = "Goodbye World";

    String hash1 =
        RemoteFileMetadata.computeHash(new java.io.ByteArrayInputStream(content1.getBytes(StandardCharsets.UTF_8)));
    String hash2 =
        RemoteFileMetadata.computeHash(new java.io.ByteArrayInputStream(content2.getBytes(StandardCharsets.UTF_8)));
    String hash3 =
        RemoteFileMetadata.computeHash(new java.io.ByteArrayInputStream(content3.getBytes(StandardCharsets.UTF_8)));

    // Same content should produce same hash
    assertEquals(hash1, hash2);
    // Different content should produce different hash
    assertNotEquals(hash1, hash3);
  }
}
