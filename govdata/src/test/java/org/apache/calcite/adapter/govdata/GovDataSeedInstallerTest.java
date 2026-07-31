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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage: the seed gate used to compare only a version marker, so a catalog file
 * deleted or replaced out from under a matching marker was never re-extracted — the MCP server's
 * first run then silently fell through to a full cold rebuild while a stale marker sat right next
 * to the missing file. {@link GovDataSeedInstaller#ensureSeeded} now also requires the catalog
 * file to exist, and gates on a content fingerprint of the bundled zip rather than the project
 * version (which stays constant across many SNAPSHOT rebuilds).
 *
 * <p>Requires the real {@code duckdb/seed/govdata-seed.zip} built into this module's resources
 * (via {@code bundleGovdataSeed}); skips itself if that resource is absent from the classpath.
 */
@Tag("unit")
class GovDataSeedInstallerTest {

  @BeforeEach
  void resetGate() {
    GovDataSeedInstaller.resetForTesting();
  }

  private static boolean seedResourcePresent() {
    return GovDataSeedInstaller.class.getResourceAsStream("/duckdb/seed/govdata-seed.zip") != null;
  }

  @Test void extractsIntoFreshOperatingDir(@TempDir Path tmpDir) {
    if (!seedResourcePresent()) {
      return;
    }
    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());
    File catalog = new File(tmpDir.toFile(), ".duckdb/govdata.duckdb");
    assertTrue(catalog.isFile(), "seed extraction must produce the catalog file");
  }

  @Test void reExtractsWhenCatalogFileIsMissingDespiteAMatchingMarker(@TempDir Path tmpDir)
      throws Exception {
    if (!seedResourcePresent()) {
      return;
    }
    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());
    File catalog = new File(tmpDir.toFile(), ".duckdb/govdata.duckdb");
    File marker = new File(tmpDir.toFile(), ".duckdb/govdata.duckdb.version");
    assertTrue(catalog.isFile(), "precondition: first seed must produce the catalog file");
    assertTrue(marker.isFile(), "precondition: first seed must write the marker");

    // Simulate the reported bug's exact state: catalog deleted (or never written, e.g. by a
    // prior run that opened/rebuilt it and then the file was removed), marker untouched.
    Files.delete(catalog.toPath());

    GovDataSeedInstaller.resetForTesting();
    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());

    assertTrue(catalog.isFile(),
        "a matching marker with a missing catalog file must still trigger re-extraction");
  }

  @Test void reExtractsWhenMarkerIsStale(@TempDir Path tmpDir) throws Exception {
    if (!seedResourcePresent()) {
      return;
    }
    File duckdbDir = new File(tmpDir.toFile(), ".duckdb");
    Files.createDirectories(duckdbDir.toPath());
    File catalog = new File(duckdbDir, "govdata.duckdb");
    File marker = new File(duckdbDir, "govdata.duckdb.version");
    Files.write(catalog.toPath(), new byte[] {1, 2, 3});
    Files.write(marker.toPath(), "not-a-real-fingerprint".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());

    byte[] afterBytes = Files.readAllBytes(catalog.toPath());
    assertTrue(afterBytes.length != 3 || afterBytes[0] != 1,
        "a stale marker must force re-extraction, replacing the placeholder catalog file");
  }
}
