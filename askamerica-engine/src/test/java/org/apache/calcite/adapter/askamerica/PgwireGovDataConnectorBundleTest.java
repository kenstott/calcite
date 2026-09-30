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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Replacing an out-of-date running server, and the cache-directory handoff to a spawn. */
@Tag("unit")
@Isolated("mutates the user.home system property")
class PgwireGovDataConnectorBundleTest {
  private String originalHome;
  private Path home;

  @BeforeEach
  void redirectHome(@TempDir Path h) {
    originalHome = System.getProperty("user.home");
    System.setProperty("user.home", h.toString());
    home = h;
  }

  @AfterEach
  void restoreHome() {
    System.setProperty("user.home", originalHome);
  }

  @Test void onlyAServerOlderThanTheEngineIsReplaced() {
    assertNotNull(PgwireGovDataConnector.bundleSupersededReason("0.94.3", "0.99.2"));
    assertNull(PgwireGovDataConnector.bundleSupersededReason("0.99.2", "0.99.2"));
    // A newer server is left alone, or engines of two releases would kill each other's server.
    assertNull(PgwireGovDataConnector.bundleSupersededReason("1.0.0", "0.99.2"));
  }

  @Test void unstampedSidesNeverTriggerAReplacement() {
    assertNull(PgwireGovDataConnector.bundleSupersededReason(null, "0.99.2"));
    assertNull(PgwireGovDataConnector.bundleSupersededReason("", "0.99.2"));
    assertNull(PgwireGovDataConnector.bundleSupersededReason("0.94.3", null));
  }

  @Test void noRecordedServerReleaseMeansNothingToReplace() {
    assertNull(PgwireGovDataConnector.serverBundleSuperseded());
  }

  @Test void launcherBundleVersionReadsTheBundleRootsMarker() throws Exception {
    Path root = Files.createDirectories(home.resolve("b").resolve("bin")).getParent();
    Files.writeString(root.resolve(PgwireGovDataInstaller.MARKER), "0.99.2");
    File launcher = root.resolve("bin").resolve("pgwire-govdata").toFile();
    assertEquals("0.99.2", PgwireGovDataConnector.launcherBundleVersion(launcher));
    Files.delete(root.resolve(PgwireGovDataInstaller.MARKER));
    assertNull(PgwireGovDataConnector.launcherBundleVersion(launcher));
  }

  @Test void cacheOptionIsAppendedQuotedAndOperatorSettingWins() {
    assertEquals("\"-Dduckdb.cache_httpfs.directory=/c/x y\"",
        PgwireGovDataConnector.withHttpfsCacheOption(null, "/c/x y"));
    assertEquals("-Xss2m \"-Dduckdb.cache_httpfs.directory=/c\"",
        PgwireGovDataConnector.withHttpfsCacheOption(" -Xss2m ", "/c"));
    String operator = "-Dduckdb.cache_httpfs.directory=/mine";
    assertEquals(operator, PgwireGovDataConnector.withHttpfsCacheOption(operator, "/c"));
  }

  @Test void cacheDirDefaultsToThisHostsOwnDirectory() {
    assertEquals(new File(new File(home.toFile(), ".askamerica"), ".duckdb_httpfs_cache"),
        PgwireGovDataConnector.httpfsCacheDir());
  }

  @Test void lowDiskWarningFiresAtOrBelowTheReserve() {
    File d = new File("/x");
    long total = 460L * 1_000_000_000L;
    // The live case: 10 GB free of 460 GB, under the 23 GB reserve.
    String w = PgwireGovDataConnector.lowDiskWarning(d, 10L * 1_000_000_000L, total);
    assertNotNull(w);
    assertTrue(w.contains("ASKAMERICA_HTTPFS_CACHE_DIR"), w);
    assertNull(PgwireGovDataConnector.lowDiskWarning(d, 98L * 1_000_000_000L, total));
    assertNull(PgwireGovDataConnector.lowDiskWarning(d, 0L, 0L), "unknown volume size: no claim");
  }
}
