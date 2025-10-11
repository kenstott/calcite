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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SecCacheManifest functionality with ETag support.
 */
@Tag("unit")
public class SecCacheManifestTest {

  @TempDir
  Path tempDir;

  @Test void testBasicCacheOperationsWithETag() throws IOException {
    String cacheDir = tempDir.toString();
    SecCacheManifest manifest = SecCacheManifest.load(cacheDir);

    String cik = "0000320193";
    String etag = "\"abc123-456def\"";

    // Initially should not be cached
    assertFalse(manifest.isCached(cik));
    assertNull(manifest.getETag(cik));

    // Create a test file
    Path testFile = tempDir.resolve("submissions.json");
    Files.write(testFile, "{\"cik\": \"0000320193\"}".getBytes());

    // Mark as cached with ETag
    long refreshAfter = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24);
    manifest.markCached(cik, testFile.toString(), etag, 100L, refreshAfter, "etag_based");

    // Should now be cached
    assertTrue(manifest.isCached(cik));
    assertEquals(etag, manifest.getETag(cik));
    assertEquals(testFile.toString(), manifest.getFilePath(cik));

    // Save and reload manifest
    manifest.save(cacheDir);
    SecCacheManifest newManifest = SecCacheManifest.load(cacheDir);

    // Should still be cached after reload
    assertTrue(newManifest.isCached(cik));
    assertEquals(etag, newManifest.getETag(cik));
    assertEquals(testFile.toString(), newManifest.getFilePath(cik));
  }

  @Test void testCacheOperationsWithoutETag() throws IOException {
    String cacheDir = tempDir.toString();
    SecCacheManifest manifest = SecCacheManifest.load(cacheDir);

    String cik = "0000789019";

    // Create a test file
    Path testFile = tempDir.resolve("submissions_msft.json");
    Files.write(testFile, "{\"cik\": \"0000789019\"}".getBytes());

    // Mark as cached without ETag (uses default 24h refresh)
    manifest.markCached(cik, testFile.toString(), 200L);

    // Should be cached
    assertTrue(manifest.isCached(cik));
    assertNull(manifest.getETag(cik));  // No ETag available
  }

  @Test void testETagCacheNeverExpiresBasedOnTime() throws IOException, InterruptedException {
    String cacheDir = tempDir.toString();
    SecCacheManifest manifest = SecCacheManifest.load(cacheDir);

    String cik = "0000004962";
    String etag = "\"xyz789\"";

    // Create a test file
    Path testFile = tempDir.resolve("submissions_bmw.json");
    Files.write(testFile, "{\"cik\": \"0000004962\"}".getBytes());

    // Mark as cached with ETag but with past refresh time
    long pastRefreshTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(48);
    manifest.markCached(cik, testFile.toString(), etag, 150L, pastRefreshTime, "etag_based");

    // Should still be cached despite past refresh time (ETag takes precedence)
    assertTrue(manifest.isCached(cik));
    assertEquals(etag, manifest.getETag(cik));

    // Cleanup should not remove entries with ETags
    int removed = manifest.cleanupExpiredEntries();
    assertEquals(0, removed);
    assertTrue(manifest.isCached(cik));
  }

  @Test void testCacheWithoutETagExpires() throws IOException, InterruptedException {
    String cacheDir = tempDir.toString();
    SecCacheManifest manifest = SecCacheManifest.load(cacheDir);

    String cik = "0000012927";

    // Create a test file
    Path testFile = tempDir.resolve("submissions_visa.json");
    Files.write(testFile, "{\"cik\": \"0000012927\"}".getBytes());

    // Mark as cached without ETag and with immediate expiration
    long pastRefreshTime = System.currentTimeMillis() - 1000;
    manifest.markCached(cik, testFile.toString(), null, 180L, pastRefreshTime, "expired_test");

    // Should not be cached (expired)
    assertFalse(manifest.isCached(cik));
  }

  @Test void testMissingFileHandling() throws IOException {
    String cacheDir = tempDir.toString();
    SecCacheManifest manifest = SecCacheManifest.load(cacheDir);

    String cik = "0000018230";
    String etag = "\"missing123\"";

    // Mark as cached but don't create the file
    String nonExistentFile = tempDir.resolve("non_existent.json").toString();
    long refreshAfter = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24);
    manifest.markCached(cik, nonExistentFile, etag, 100L, refreshAfter, "etag_based");

    // Should return false because file doesn't exist
    assertFalse(manifest.isCached(cik));
  }

  @Test void testManifestPersistence() throws IOException {
    String cacheDir = tempDir.toString();
    SecCacheManifest manifest = SecCacheManifest.load(cacheDir);

    // Add multiple cache entries
    String[] ciks = {"0001001", "0001002", "0001003", "0001004"};
    for (String cik : ciks) {
      Path testFile = tempDir.resolve("submissions_" + cik + ".json");
      Files.write(testFile, ("{\"cik\": \"" + cik + "\"}").getBytes());

      String etag = "\"etag_" + cik + "\"";
      long refreshAfter = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24);
      manifest.markCached(cik, testFile.toString(), etag, 50L, refreshAfter, "etag_based");
    }

    // Save manifest
    manifest.save(cacheDir);

    // Verify manifest file was created
    Path manifestFile = tempDir.resolve("sec_cache_manifest.json");
    assertTrue(Files.exists(manifestFile));

    // Load into new manifest
    SecCacheManifest newManifest = SecCacheManifest.load(cacheDir);

    // Verify all entries are still cached
    for (String cik : ciks) {
      assertTrue(newManifest.isCached(cik), "Entry for CIK " + cik + " should be cached");
      assertEquals("\"etag_" + cik + "\"", newManifest.getETag(cik));
    }
  }

  @Test void testCleanupExpiredEntries() throws IOException {
    String cacheDir = tempDir.toString();
    SecCacheManifest manifest = SecCacheManifest.load(cacheDir);

    // Add entry with ETag (should not be removed)
    Path testFile1 = tempDir.resolve("submissions_1.json");
    Files.write(testFile1, "{\"cik\": \"0001\"}".getBytes());
    long pastTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(48);
    manifest.markCached("0001", testFile1.toString(), "\"etag1\"", 100L, pastTime, "etag_based");

    // Add entry without ETag but not expired (should not be removed)
    Path testFile2 = tempDir.resolve("submissions_2.json");
    Files.write(testFile2, "{\"cik\": \"0002\"}".getBytes());
    long futureTime = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24);
    manifest.markCached("0002", testFile2.toString(), null, 100L, futureTime, "not_expired");

    // Add entry without ETag and expired (should be removed)
    Path testFile3 = tempDir.resolve("submissions_3.json");
    Files.write(testFile3, "{\"cik\": \"0003\"}".getBytes());
    manifest.markCached("0003", testFile3.toString(), null, 100L, pastTime, "expired");

    // Cleanup
    int removed = manifest.cleanupExpiredEntries();

    // Should remove only the expired entry without ETag
    assertEquals(1, removed);
    assertTrue(manifest.isCached("0001"));  // ETag entry kept
    assertTrue(manifest.isCached("0002"));  // Not expired kept
    assertFalse(manifest.isCached("0003")); // Expired removed
  }

  @Test void testCacheStats() throws IOException {
    String cacheDir = tempDir.toString();
    SecCacheManifest manifest = SecCacheManifest.load(cacheDir);

    // Add entries with ETags
    for (int i = 0; i < 3; i++) {
      Path testFile = tempDir.resolve("submissions_etag_" + i + ".json");
      Files.write(testFile, "{\"test\": \"data\"}".getBytes());
      long refreshAfter = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24);
      manifest.markCached("000" + i, testFile.toString(), "\"etag" + i + "\"", 100L, refreshAfter, "etag_based");
    }

    // Add entries without ETags
    for (int i = 3; i < 5; i++) {
      Path testFile = tempDir.resolve("submissions_no_etag_" + i + ".json");
      Files.write(testFile, "{\"test\": \"data\"}".getBytes());
      long refreshAfter = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24);
      manifest.markCached("000" + i, testFile.toString(), null, 100L, refreshAfter, "no_etag");
    }

    SecCacheManifest.CacheStats stats = manifest.getStats();
    assertEquals(5, stats.totalEntries);
    assertEquals(3, stats.entriesWithETag);
    assertEquals(2, stats.entriesWithoutETag);
    assertEquals(0, stats.expiredEntries);
  }

  @Test void testETagUpdate() throws IOException {
    String cacheDir = tempDir.toString();
    SecCacheManifest manifest = SecCacheManifest.load(cacheDir);

    String cik = "0000320193";
    Path testFile = tempDir.resolve("submissions.json");
    Files.write(testFile, "{\"cik\": \"0000320193\"}".getBytes());

    // Initial cache with first ETag
    String etag1 = "\"v1\"";
    long refreshAfter = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24);
    manifest.markCached(cik, testFile.toString(), etag1, 100L, refreshAfter, "etag_based");
    assertEquals(etag1, manifest.getETag(cik));

    // Update with new ETag (simulating 200 response with new data)
    String etag2 = "\"v2\"";
    manifest.markCached(cik, testFile.toString(), etag2, 150L, refreshAfter, "etag_updated");
    assertEquals(etag2, manifest.getETag(cik));

    // Verify persistence
    manifest.save(cacheDir);
    SecCacheManifest reloaded = SecCacheManifest.load(cacheDir);
    assertEquals(etag2, reloaded.getETag(cik));
  }
}
