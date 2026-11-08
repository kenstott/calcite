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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A spawn keeps the data-server bundle in step with the engine without waiting on a download:
 * a prepared newer release is swapped in, an older bundle is used while the release is fetched in
 * the background, and nothing is fetched for a local build, a current bundle or no bundle.
 */
@Tag("unit")
class PgwireGovDataLauncherForSpawnTest {

  private static void install(Path dir, String stamp) throws Exception {
    File launcher = PgwireGovDataInstaller.launcherPath(dir);
    Files.createDirectories(launcher.toPath().getParent());
    Files.write(launcher.toPath(), "x".getBytes("UTF-8"));
    if (stamp != null) {
      Files.write(dir.resolve(PgwireGovDataInstaller.MARKER), stamp.getBytes("UTF-8"));
    }
  }

  @Test void noBundleMeansNoLauncherAndNoDownload(@TempDir Path root) {
    List<Runnable> scheduled = new ArrayList<>();
    assertNull(PgwireGovDataInstaller.launcherForSpawn(root.resolve("b"), "0.124.4",
        scheduled::add));
    assertTrue(scheduled.isEmpty());
  }

  @Test void currentBundleOrLocalBuildIsUsedAsIs(@TempDir Path root) throws Exception {
    Path dir = root.resolve("b");
    install(dir, "0.124.4");
    List<Runnable> scheduled = new ArrayList<>();
    assertEquals(PgwireGovDataInstaller.launcherPath(dir),
        PgwireGovDataInstaller.launcherForSpawn(dir, "0.124.4", scheduled::add));
    assertEquals(PgwireGovDataInstaller.launcherPath(dir),
        PgwireGovDataInstaller.launcherForSpawn(dir, null, scheduled::add));
    assertEquals(PgwireGovDataInstaller.launcherPath(dir),
        PgwireGovDataInstaller.launcherForSpawn(dir, "0.120.0", scheduled::add));
    assertTrue(scheduled.isEmpty(), "nothing fetched for a current, local or newer bundle");
  }

  @Test void olderBundleRunsWhileTheReleaseIsFetchedInTheBackground(@TempDir Path root)
      throws Exception {
    Path dir = root.resolve("b");
    install(dir, "0.118.0");
    List<Runnable> scheduled = new ArrayList<>();
    assertEquals(PgwireGovDataInstaller.launcherPath(dir),
        PgwireGovDataInstaller.launcherForSpawn(dir, "0.124.4", scheduled::add));
    assertEquals(1, scheduled.size(), "one background update, never run on the caller's thread");
  }

  @Test void aPreparedReleaseIsSwappedInWithoutADownload(@TempDir Path root) throws Exception {
    Path dir = root.resolve("b");
    install(dir, "0.118.0");
    install(PgwireGovDataInstaller.readyDir(dir, "0.124.4"), "0.124.4");
    List<Runnable> scheduled = new ArrayList<>();
    File launcher = PgwireGovDataInstaller.launcherForSpawn(dir, "0.124.4", scheduled::add);
    assertEquals(PgwireGovDataInstaller.launcherPath(dir), launcher);
    assertEquals("0.124.4", PgwireGovDataInstaller.readMarker(dir));
    assertTrue(scheduled.isEmpty());
  }
}
