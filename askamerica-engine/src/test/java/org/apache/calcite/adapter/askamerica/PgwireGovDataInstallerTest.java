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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.calcite.adapter.askamerica.PgwireGovDataInstaller.Action;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bundle-currency rules of {@link PgwireGovDataInstaller}: a bundle is kept at this engine's
 * release, an unstamped one is stale, a newer one is never downgraded, the checksum is
 * mandatory, and replacement is a rename that keeps the running server's bookkeeping.
 */
@Tag("unit")
class PgwireGovDataInstallerTest {

  @Test void versionsCompareNumericallyNotAsText() {
    assertTrue(PgwireGovDataInstaller.compareVersions("0.100.0", "0.99.2") > 0);
    assertTrue(PgwireGovDataInstaller.compareVersions("0.99.2", "0.99.10") < 0);
    assertEquals(0, PgwireGovDataInstaller.compareVersions("0.99.2", "0.99.2"));
    assertEquals(0, PgwireGovDataInstaller.compareVersions("1.0", "1.0.0"));
    assertTrue(PgwireGovDataInstaller.compareVersions("1.0.1", "1.0") > 0);
  }

  @Test void noBundleMeansInstall() {
    assertEquals(Action.INSTALL, PgwireGovDataInstaller.decide(false, null, "0.99.2"));
  }

  @Test void unstampedBundlePredatesStampingAndIsUpdated() {
    // The bug this fixes: a bundle installed once, never stamped, kept forever.
    assertEquals(Action.UPDATE, PgwireGovDataInstaller.decide(true, null, "0.99.2"));
  }

  @Test void olderBundleIsUpdatedSameIsKept() {
    assertEquals(Action.UPDATE, PgwireGovDataInstaller.decide(true, "0.94.3", "0.99.2"));
    assertEquals(Action.KEEP, PgwireGovDataInstaller.decide(true, "0.99.2", "0.99.2"));
  }

  @Test void newerBundleIsNeverDowngradedByAnOlderEngine() {
    assertEquals(Action.KEEP_NEWER, PgwireGovDataInstaller.decide(true, "1.0.0", "0.99.2"));
  }

  @Test void markerRoundTripsAndAbsentOrBlankMeansUnversioned(@TempDir Path dir) throws IOException {
    assertNull(PgwireGovDataInstaller.readMarker(dir));
    Files.writeString(dir.resolve(PgwireGovDataInstaller.MARKER), "  \n");
    assertNull(PgwireGovDataInstaller.readMarker(dir));
    Files.writeString(dir.resolve(PgwireGovDataInstaller.MARKER), "0.99.2\n");
    assertEquals("0.99.2", PgwireGovDataInstaller.readMarker(dir));
  }

  @Test void checksumIsMandatory() {
    assertThrows(PgwireGovDataInstaller.InstallFailedException.class, () ->
        PgwireGovDataInstaller.requiredChecksum("u", url -> { throw new IOException("404"); }));
    assertThrows(PgwireGovDataInstaller.InstallFailedException.class, () ->
        PgwireGovDataInstaller.requiredChecksum("u", url -> "  "));
    assertThrows(PgwireGovDataInstaller.InstallFailedException.class, () ->
        PgwireGovDataInstaller.requiredChecksum("u", url -> "<html>not found</html>"));
  }

  @Test void checksumFileFormatIsParsed() {
    String hex = "4c1d2188a52691b0b2d1875b9c45132c1d5c003e27f8ac07cea515e513d83df8";
    assertEquals(hex, PgwireGovDataInstaller.requiredChecksum("u",
        url -> hex + "  pgwire-govdata-0.99.2-macos-arm64.tar.gz\n"));
  }

  private static Path bundle(Path root, String launcherContent) throws IOException {
    Files.createDirectories(root.resolve("bin"));
    Files.writeString(root.resolve("bin").resolve("pgwire-govdata"), launcherContent);
    return root;
  }

  @Test void swapReplacesTheBundleAndCarriesTheRunningServersFiles(@TempDir Path home)
      throws IOException {
    Path dir = bundle(home.resolve("pgwire-govdata"), "old");
    Files.writeString(dir.resolve("pgwire.pid"), "4242");
    Files.writeString(dir.resolve("pgwire.creds-expiry"), "123");
    Files.writeString(dir.resolve("pgwire.server-bundle-version"), "0.94.3");
    Files.writeString(dir.resolve("spawn.log"), "old log");
    Files.createDirectories(dir.resolve("model"));
    Files.writeString(dir.resolve("model").resolve("catalog-cache-old.pkl"), "stale");
    Path staging = bundle(home.resolve("pgwire-govdata.staging-1"), "new");
    Files.writeString(staging.resolve(PgwireGovDataInstaller.MARKER), "0.99.2");

    PgwireGovDataInstaller.swapIn(staging, dir);

    assertEquals("new", Files.readString(dir.resolve("bin").resolve("pgwire-govdata")));
    assertEquals("0.99.2", PgwireGovDataInstaller.readMarker(dir));
    assertEquals("4242", Files.readString(dir.resolve("pgwire.pid")));
    assertEquals("123", Files.readString(dir.resolve("pgwire.creds-expiry")));
    assertEquals("0.94.3", Files.readString(dir.resolve("pgwire.server-bundle-version")),
        "the old server's release must survive the swap so it can be recognised and replaced");
    assertEquals("old log", Files.readString(dir.resolve("spawn.log")));
    assertFalse(Files.exists(dir.resolve("model").resolve("catalog-cache-old.pkl")),
        "caches built from the old bundle must not carry over");
    assertFalse(Files.exists(staging));
    try (java.util.stream.Stream<Path> left = Files.list(home)) {
      assertEquals(1, left.count(), "no .old-* or staging directory may be left behind");
    }
  }

  @Test void swapIntoAnEmptyLocationJustRenames(@TempDir Path home) throws IOException {
    Path dir = home.resolve("pgwire-govdata");
    Path staging = bundle(home.resolve("pgwire-govdata.staging-1"), "new");
    PgwireGovDataInstaller.swapIn(staging, dir);
    assertEquals("new", Files.readString(dir.resolve("bin").resolve("pgwire-govdata")));
    assertFalse(Files.exists(staging));
  }

  @Test void failedRenameRestoresTheWorkingBundle(@TempDir Path home) throws IOException {
    Path dir = bundle(home.resolve("pgwire-govdata"), "old");
    Path missingStaging = home.resolve("pgwire-govdata.staging-missing");
    assertThrows(IOException.class, () -> PgwireGovDataInstaller.swapIn(missingStaging, dir));
    assertEquals("old", Files.readString(dir.resolve("bin").resolve("pgwire-govdata")),
        "a failed swap must leave the working bundle in place");
  }

  @Test void ownVersionIsNullOutsideAStampedJar() {
    // Under the test runner this class loads from a classes directory, not a stamped jar.
    assertNull(PgwireGovDataInstaller.ownEngineVersion());
  }
}
