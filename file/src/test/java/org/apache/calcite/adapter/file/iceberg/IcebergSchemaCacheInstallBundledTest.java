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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the upgrade path of {@link IcebergSchemaCache#installBundled}: a JAR that ships a newer
 * bundled cache must be able to install it over one an older JAR left behind, while a warehouse
 * sync's own refresh of that file is never clobbered by a re-run of the same bundled copy.
 */
// The cache directory is selected by a system property, so every method here mutates process-wide
// state that any concurrently-running test would read. Serialize on that resource and keep the
// methods on one thread; without this the suite interleaves and methods fail on each other's dirs.
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
public class IcebergSchemaCacheInstallBundledTest {

  private static final String CACHE_DIR_PROPERTY = "iceberg.metadata.cache.directory";

  private String previousCacheDir;
  private File cacheDir;

  @BeforeEach void pinCacheDirectory(@TempDir Path tmpDir) {
    previousCacheDir = System.getProperty(CACHE_DIR_PROPERTY);
    cacheDir = tmpDir.toFile();
    System.setProperty(CACHE_DIR_PROPERTY, cacheDir.getAbsolutePath());
  }

  @AfterEach void restoreCacheDirectory() {
    if (previousCacheDir == null) {
      System.clearProperty(CACHE_DIR_PROPERTY);
    } else {
      System.setProperty(CACHE_DIR_PROPERTY, previousCacheDir);
    }
  }

  private File cacheFile() {
    return new File(cacheDir, IcebergSchemaCache.FILE_NAME);
  }

  private static byte[] cacheDocument(String marker) {
    return ("{\"tables\":{\"" + marker + "\":null}}").getBytes(StandardCharsets.UTF_8);
  }

  @Test void installsIntoAnEmptyCacheDirectory() throws Exception {
    byte[] bundled = cacheDocument("first");

    assertTrue(IcebergSchemaCache.installBundled(new ByteArrayInputStream(bundled)),
        "a first-ever install has nothing to preserve and must write the bundled cache");
    assertArrayEquals(bundled, Files.readAllBytes(cacheFile().toPath()));
  }

  @Test void installsANewBundledCacheOverTheOneAPriorJarLeft() throws Exception {
    byte[] oldJarCache = cacheDocument("old-jar");
    byte[] newJarCache = cacheDocument("new-jar");
    assertTrue(IcebergSchemaCache.installBundled(new ByteArrayInputStream(oldJarCache)));

    // The upgrade: a different JAR, so a different bundled digest than the marker records.
    assertTrue(IcebergSchemaCache.installBundled(new ByteArrayInputStream(newJarCache)),
        "an upgraded JAR ships a cache the local file has never seen and must install it");
    assertArrayEquals(newJarCache, Files.readAllBytes(cacheFile().toPath()),
        "the regenerated cache must replace the one the previous JAR installed");
  }

  @Test void leavesAWarehouseRefreshOfTheSameBundledCacheAlone() throws Exception {
    byte[] bundled = cacheDocument("shipped");
    assertTrue(IcebergSchemaCache.installBundled(new ByteArrayInputStream(bundled)));

    // Stand in for the warehouse sync overwriting the file after install; the bundled artifact
    // has not changed, so re-running install must not undo it.
    byte[] refreshed = cacheDocument("downloaded-later");
    Files.write(cacheFile().toPath(), refreshed);

    assertFalse(IcebergSchemaCache.installBundled(new ByteArrayInputStream(bundled)),
        "the same bundled artifact must not reinstall over a newer local copy");
    assertArrayEquals(refreshed, Files.readAllBytes(cacheFile().toPath()));
  }

  @Test void reinstallsAfterTheCacheIsDeletedOutFromUnderTheMarker() throws Exception {
    byte[] bundled = cacheDocument("shipped");
    assertTrue(IcebergSchemaCache.installBundled(new ByteArrayInputStream(bundled)));
    Files.delete(cacheFile().toPath());

    assertTrue(IcebergSchemaCache.installBundled(new ByteArrayInputStream(bundled)),
        "a marker with no cache beside it must not block re-seeding the missing file");
    assertArrayEquals(bundled, Files.readAllBytes(cacheFile().toPath()));
  }
}
