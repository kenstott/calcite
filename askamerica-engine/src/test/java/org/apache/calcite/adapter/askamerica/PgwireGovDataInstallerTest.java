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

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.calcite.adapter.askamerica.PgwireGovDataInstaller.Action;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
    Files.writeString(launcher(root), launcherContent);
    return root;
  }

  /** The launcher of a bundle, under the name this platform's installer looks for. */
  private static Path launcher(Path root) {
    return PgwireGovDataInstaller.launcherPath(root).toPath();
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

    assertEquals("new", Files.readString(launcher(dir)));
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
    assertEquals("new", Files.readString(launcher(dir)));
    assertFalse(Files.exists(staging));
  }

  @Test void failedRenameRestoresTheWorkingBundle(@TempDir Path home) throws IOException {
    Path dir = bundle(home.resolve("pgwire-govdata"), "old");
    Path missingStaging = home.resolve("pgwire-govdata.staging-missing");
    assertThrows(IOException.class, () -> PgwireGovDataInstaller.swapIn(missingStaging, dir));
    assertEquals("old", Files.readString(launcher(dir)),
        "a failed swap must leave the working bundle in place");
  }

  @Test void ownVersionIsNullOutsideAStampedJar() {
    // Under the test runner this class loads from a classes directory, not a stamped jar.
    assertNull(PgwireGovDataInstaller.ownEngineVersion());
  }

  // ── an update never delays a spawn ───────────────────────────────────────

  @Test void olderBundleIsServedAtOnceAndUpdatedInTheBackground(@TempDir Path home)
      throws IOException {
    Path dir = bundle(home.resolve("pgwire-govdata"), "old");
    Files.writeString(dir.resolve(PgwireGovDataInstaller.MARKER), "0.99.0");
    List<Runnable> scheduled = new ArrayList<>();

    File launcher = PgwireGovDataInstaller.ensureLauncher(dir, "0.100.0", scheduled::add);

    assertEquals("old", Files.readString(launcher.toPath()),
        "the installed bundle must be returned without waiting for the download");
    assertEquals(1, scheduled.size(), "the update must be handed to the background executor");
  }

  @Test void currentBundleSchedulesNoUpdate(@TempDir Path home) throws IOException {
    Path dir = bundle(home.resolve("pgwire-govdata"), "current");
    Files.writeString(dir.resolve(PgwireGovDataInstaller.MARKER), "0.100.0");
    List<Runnable> scheduled = new ArrayList<>();

    File launcher = PgwireGovDataInstaller.ensureLauncher(dir, "0.100.0", scheduled::add);

    assertEquals("current", Files.readString(launcher.toPath()));
    assertTrue(scheduled.isEmpty());
  }

  @Test void preparedReleaseIsSwappedInAtTheNextSpawn(@TempDir Path home) throws IOException {
    Path dir = bundle(home.resolve("pgwire-govdata"), "old");
    Files.writeString(dir.resolve(PgwireGovDataInstaller.MARKER), "0.99.0");
    Files.writeString(dir.resolve("pgwire.pid"), "4242");
    Path ready = bundle(PgwireGovDataInstaller.readyDir(dir, "0.100.0"), "new");
    Files.writeString(ready.resolve(PgwireGovDataInstaller.MARKER), "0.100.0");
    List<Runnable> scheduled = new ArrayList<>();

    File launcher = PgwireGovDataInstaller.ensureLauncher(dir, "0.100.0", scheduled::add);

    assertEquals(PgwireGovDataInstaller.launcherPath(dir), launcher);
    assertEquals("new", Files.readString(launcher.toPath()));
    assertEquals("0.100.0", PgwireGovDataInstaller.readMarker(dir));
    assertEquals("4242", Files.readString(dir.resolve("pgwire.pid")));
    assertFalse(Files.exists(ready));
    assertTrue(scheduled.isEmpty(), "nothing is left to download once the release is adopted");
  }

  @Test void directoryThatIsNotACompletePreparedReleaseIsNeverAdopted(@TempDir Path home)
      throws IOException {
    Path dir = bundle(home.resolve("pgwire-govdata"), "old");
    Files.writeString(dir.resolve(PgwireGovDataInstaller.MARKER), "0.99.0");
    // Stamped with another release: not what this engine asked for.
    Path ready = bundle(PgwireGovDataInstaller.readyDir(dir, "0.100.0"), "new");
    Files.writeString(ready.resolve(PgwireGovDataInstaller.MARKER), "0.99.5");
    List<Runnable> scheduled = new ArrayList<>();

    File launcher = PgwireGovDataInstaller.ensureLauncher(dir, "0.100.0", scheduled::add);

    assertFalse(PgwireGovDataInstaller.isPrepared(dir, "0.100.0"));
    assertEquals("old", Files.readString(launcher.toPath()));
    assertEquals(1, scheduled.size());
  }

  @Test void orphansOfAKilledInstallAreRemovedButTheResumablePartIsKept(@TempDir Path home)
      throws IOException {
    Path dir = bundle(home.resolve("pgwire-govdata"), "old");
    Path keep = PgwireGovDataInstaller.partFile(dir, "0.100.0", "macos-arm64");
    Files.writeString(keep, "partial");
    Path stalePart = PgwireGovDataInstaller.partFile(dir, "0.99.5", "macos-arm64");
    Files.writeString(stalePart, "partial");
    Path staging = bundle(home.resolve("pgwire-govdata.staging-77"), "half");
    Path staleReady = bundle(PgwireGovDataInstaller.readyDir(dir, "0.99.5"), "stale");
    Path ready = bundle(PgwireGovDataInstaller.readyDir(dir, "0.100.0"), "new");
    Files.writeString(home.resolve("pgwire-govdata.install.lock"), "");

    PgwireGovDataInstaller.removeOrphans(dir, "0.100.0", keep);

    assertTrue(Files.exists(keep));
    assertTrue(Files.exists(ready));
    assertTrue(Files.exists(dir));
    assertTrue(Files.exists(home.resolve("pgwire-govdata.install.lock")));
    assertFalse(Files.exists(stalePart));
    assertFalse(Files.exists(staging));
    assertFalse(Files.exists(staleReady));
  }

  @Test void secondUpdateInTheSameProcessIsSkipped(@TempDir Path home) throws IOException {
    Path dir = bundle(home.resolve("pgwire-govdata"), "old");
    Files.writeString(dir.resolve(PgwireGovDataInstaller.MARKER), "0.99.0");
    Path lockFile = home.resolve("pgwire-govdata.install.lock");
    try (FileChannel ch =
             FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
         FileLock held = ch.lock()) {
      // Returns at once: no wait on this process's own lock, no lookup, nothing staged.
      PgwireGovDataInstaller.updateInBackground(dir, "0.100.0");
      assertTrue(held.isValid());
    }
    assertFalse(PgwireGovDataInstaller.isPrepared(dir, "0.100.0"));
    assertEquals("0.99.0", PgwireGovDataInstaller.readMarker(dir));
  }

  // ── resumable download ───────────────────────────────────────────────────

  /** Serves {@code body}; honours a Range header only when {@code ranges} is set. Records the
   *  Range header of each request (empty string when there was none). */
  private static HttpServer serve(byte[] body, boolean ranges, List<String> seen)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/bundle.tar.gz", ex -> {
      String range = ex.getRequestHeaders().getFirst("Range");
      seen.add(range == null ? "" : range);
      byte[] out = body;
      int status = 200;
      if (ranges && range != null) {
        int from = Integer.parseInt(range.substring("bytes=".length(), range.length() - 1));
        if (from >= body.length) {
          ex.sendResponseHeaders(416, -1);
          ex.close();
          return;
        }
        out = Arrays.copyOfRange(body, from, body.length);
        status = 206;
      }
      ex.sendResponseHeaders(status, out.length);
      try (OutputStream os = ex.getResponseBody()) {
        os.write(out);
      }
    });
    server.start();
    return server;
  }

  private static String url(HttpServer server) {
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/bundle.tar.gz";
  }

  private static byte[] payload() {
    byte[] body = new byte[200_000];
    new Random(7).nextBytes(body);
    return body;
  }

  @Test void interruptedDownloadIsResumedFromTheBytesAlreadyOnDisk(@TempDir Path home)
      throws Exception {
    byte[] body = payload();
    Path part = home.resolve("bundle.part");
    Files.write(part, Arrays.copyOfRange(body, 0, 60_000));
    List<String> seen = new ArrayList<>();
    HttpServer server = serve(body, true, seen);
    try {
      PgwireGovDataInstaller.download(url(server), part);
    } finally {
      server.stop(0);
    }
    assertEquals(Arrays.asList("bytes=60000-"), seen);
    assertArrayEquals(body, Files.readAllBytes(part));
  }

  @Test void serverThatIgnoresTheRangeReplacesThePartialFile(@TempDir Path home)
      throws Exception {
    byte[] body = payload();
    Path part = home.resolve("bundle.part");
    Files.write(part, new byte[60_000]);
    HttpServer server = serve(body, false, new ArrayList<>());
    try {
      PgwireGovDataInstaller.download(url(server), part);
    } finally {
      server.stop(0);
    }
    assertArrayEquals(body, Files.readAllBytes(part),
        "a 200 answer is the whole asset and must not be appended to the partial file");
  }

  @Test void freshDownloadSendsNoRangeAndAFullyDownloadedPartIsLeftAlone(@TempDir Path home)
      throws Exception {
    byte[] body = payload();
    Path part = home.resolve("bundle.part");
    List<String> seen = new ArrayList<>();
    HttpServer server = serve(body, true, seen);
    try {
      PgwireGovDataInstaller.download(url(server), part);
      assertArrayEquals(body, Files.readAllBytes(part));
      // Killed between download and extraction: the next attempt asks for nothing more.
      PgwireGovDataInstaller.download(url(server), part);
    } finally {
      server.stop(0);
    }
    assertEquals(Arrays.asList("", "bytes=200000-"), seen);
    assertArrayEquals(body, Files.readAllBytes(part));
  }
}
