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
package org.apache.calcite.adapter.govdata.econ;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CacheManifest functionality with local and remote storage.
 */
@Tag("unit")
public class CacheManifestTest {

  @TempDir
  Path tempDir;

  @Test void testBasicCacheOperations() throws IOException {
    String cacheDir = tempDir.toString();
    CacheManifest manifest = CacheManifest.load(cacheDir);

    // Test parameters
    String dataType = "test_data";
    int year = 2023;
    Map<String, String> params = new HashMap<>();
    params.put("type", "test");
    params.put("source", "unit_test");

    // Initially should not be cached
    assertFalse(manifest.isCached(dataType, year, params));

    // Create a test file
    Path testFile = tempDir.resolve("test_data.json");
    Files.write(testFile, "{'test': 'data'}".getBytes());

    // Mark as cached
    manifest.markCached(dataType, year, params, testFile.toString(), 100L);

    // Should now be cached
    assertTrue(manifest.isCached(dataType, year, params));

    // Save and reload manifest
    manifest.save(cacheDir);
    CacheManifest newManifest = CacheManifest.load(cacheDir);

    // Should still be cached after reload
    assertTrue(newManifest.isCached(dataType, year, params));
  }

  @Test void testCacheExpiration() throws IOException, InterruptedException {
    String cacheDir = tempDir.toString();
    CacheManifest manifest = CacheManifest.load(cacheDir);

    String dataType = "expiring_data";
    int year = 2023;
    Map<String, String> params = new HashMap<>();
    params.put("type", "expiring");

    // Create a test file
    Path testFile = tempDir.resolve("expiring_data.json");
    Files.write(testFile, "{'test': 'data'}".getBytes());

    // Mark as cached
    manifest.markCached(dataType, year, params, testFile.toString(), 100L);
    assertTrue(manifest.isCached(dataType, year, params));

    // Test with cleanup - use cleanupExpiredEntries method
    manifest.cleanupExpiredEntries();

    // After cleanup, should still be cached (not expired yet)
    assertTrue(manifest.isCached(dataType, year, params));
  }

  @Test void testMissingFileHandling() throws IOException {
    String cacheDir = tempDir.toString();
    CacheManifest manifest = CacheManifest.load(cacheDir);

    String dataType = "missing_file";
    int year = 2023;
    Map<String, String> params = new HashMap<>();
    params.put("type", "missing");

    // Mark as cached but don't create the file
    String nonExistentFile = tempDir.resolve("non_existent.json").toString();
    manifest.markCached(dataType, year, params, nonExistentFile, 100L);

    // Should return false because file doesn't exist
    assertFalse(manifest.isCached(dataType, year, params));
  }

  @Test void testManifestPersistence() throws IOException {
    String cacheDir = tempDir.toString();
    CacheManifest manifest = CacheManifest.load(cacheDir);

    // Add multiple cache entries
    for (int year = 2020; year <= 2023; year++) {
      String dataType = "test_data_" + year;
      Map<String, String> params = new HashMap<>();
      params.put("year", String.valueOf(year));

      Path testFile = tempDir.resolve("test_" + year + ".json");
      Files.write(testFile, ("{'year': " + year + "}").getBytes());

      manifest.markCached(dataType, year, params, testFile.toString(), 50L);
    }

    // Save manifest
    manifest.save(cacheDir);

    // Verify manifest file was created
    Path manifestFile = tempDir.resolve("cache_manifest.json");
    assertTrue(Files.exists(manifestFile));

    // Load into new manifest
    CacheManifest newManifest = CacheManifest.load(cacheDir);

    // Verify all entries are still cached
    for (int year = 2020; year <= 2023; year++) {
      String dataType = "test_data_" + year;
      Map<String, String> params = new HashMap<>();
      params.put("year", String.valueOf(year));

      assertTrue(newManifest.isCached(dataType, year, params),
          "Entry for year " + year + " should be cached");
    }
  }
}
