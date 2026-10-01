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
package org.apache.calcite.adapter.askamerica;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Downloads, extracts and keeps current the airgapped {@code pgwire-govdata-<version>-<os>.tar.gz}
 * bundle under {@code ~/.askamerica/pgwire-govdata/}, the shared server every engine process
 * talks to (pgwire mode is the default since kenstott/calcite#364).
 *
 * <p>The bundle is kept at the SAME release as this engine. Both ship from one release
 * ({@code engine-v<version>} carries {@code pgwire-govdata-<version>-*}), and a skew between them
 * is not cosmetic: a bundle installed once and never refreshed kept serving a two-week-old
 * catalog (no {@code law} schema, missing canonical columns) under a current engine, and every
 * answer drawn from it looked like a data gap. So the installed release is stamped in
 * {@value #MARKER}, and a bundle that is older than this engine, or carries no stamp, is
 * replaced. A newer bundle is kept (never downgraded): processes running an older engine
 * sharing this machine must not undo an update a newer one made.
 *
 * <p>Replacement is atomic: the new release is downloaded, verified against its published
 * sha256 (mandatory) and extracted into a sibling staging directory, then swapped in by
 * rename, so no process ever runs a half-extracted bundle and a server holding the old files
 * open keeps working until it is replaced. Installs are serialized across processes by a
 * file lock. When an update cannot be completed and a working bundle exists, the working
 * bundle is kept and the failure is reported, never silently absorbed.
 *
 * <p>An update never delays a spawn. The bundle is 900 MB; fetched before the server starts it
 * held every query for the length of the download, and Claude Desktop restarting the connector
 * restarted it from zero (observed 2026-09-30, 0.102.0 superseded by 0.103.0, ~50 minutes at
 * 300 KB/s). So while a working bundle exists the server is spawned from it, and the new
 * release is downloaded (resuming a partial file), verified and extracted on a background
 * thread into a sibling {@code .ready-<version>} directory. The next spawn — the only moment no
 * server is running on the old files — swaps it in by rename. Only a machine with no bundle at
 * all waits for the download.
 *
 * <p>An operator-supplied {@code ASKAMERICA_PGWIRE_LAUNCHER} or an installer-bundled copy is
 * never touched: those are resolved before this class is reached.
 */
final class PgwireGovDataInstaller {

    private PgwireGovDataInstaller() {
    }

    private static final String LATEST_API =
        "https://api.github.com/repos/kenstott/calcite/releases/latest";
    private static final String RELEASE_BY_TAG =
        "https://api.github.com/repos/kenstott/calcite/releases/tags/";

    static Path cacheDir() {
        return Paths.get(System.getProperty("user.home"), ".askamerica", "pgwire-govdata");
    }

    /**
     * Thrown for a genuine installation failure (download error, sha256 mismatch, extraction
     * failure) — as opposed to {@code ensureLauncher()} returning null, which is reserved for
     * "nothing to install" (no asset published for this OS at all). The distinction matters:
     * callers must NOT silently swallow this into a generic downstream timeout, which is
     * exactly what buried the actual cause of a real, hours-long production failure earlier
     * (a CI sha256-generation bug) behind a meaningless "did not start accepting connections"
     * message.
     */
    static final class InstallFailedException extends RuntimeException {
        InstallFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Release stamp of an installed bundle, written into the bundle directory at install. */
    static final String MARKER = ".bundle-version";

    /**
     * Files the connector keeps inside the bundle directory that describe the RUNNING server,
     * not the bundle. They are copied into a replacement bundle so a server spawned from the
     * old files can still be found (and replaced) after the swap.
     */
    static final String[] CARRY_OVER = {
        "pgwire.pid", "pgwire.creds-expiry", "pgwire.server-bundle-version", "spawn.log"
    };

    /** What {@link #ensureLauncher()} does with the bundle directory. */
    enum Action { KEEP, KEEP_NEWER, INSTALL, UPDATE }

    /**
     * Returns the launcher of a bundle at this engine's release, installing or updating it
     * first when needed. Returns null ONLY when there is genuinely nothing to install (no
     * asset published for this OS). Every install failure with no working bundle to fall back
     * to throws {@link InstallFailedException} with the specific cause.
     */
    static File ensureLauncher() {
        return ensureLauncher(cacheDir(), ownEngineVersion(), PgwireGovDataInstaller::startDaemon);
    }

    /** {@link #ensureLauncher()} over an explicit bundle directory and engine release, with the
     *  executor that runs a background update, so a test can observe one being scheduled
     *  without it reaching the network. */
    static File ensureLauncher(Path dir, String own, Executor background) {
        File existing = launcherPath(dir);
        boolean have = existing.isFile();
        String installed = have ? readMarker(dir) : null;
        if (have && installed != null && own == null) {
            // A local build has no release of its own to match, and asking GitHub for the latest
            // one on every spawn runs into its unauthenticated rate limit (60 an hour) under a
            // test suite. A stamped bundle is a real release, so keep it; only an unstamped or
            // missing bundle makes a local build look up the latest release.
            return existing;
        }
        String target;
        try {
            target = targetVersion(own);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (have) {
                report("Could not determine which pgwire-govdata release this engine needs ("
                    + e.getMessage() + ") — keeping the installed bundle ("
                    + describe(installed) + ").");
                return existing;
            }
            throw new InstallFailedException("Could not determine which pgwire-govdata release "
                + "to install: " + e.getMessage(), e);
        }
        Action action = decide(have, installed, target);
        if (action == Action.KEEP) {
            return existing;
        }
        if (action == Action.KEEP_NEWER) {
            report("Installed pgwire-govdata " + installed + " is newer than this engine ("
                + target + ") — keeping it; an older engine never downgrades the shared bundle.");
            return existing;
        }
        if (action == Action.INSTALL) {
            return installUnderLock(dir, target);
        }
        File adopted = adoptPrepared(dir, target);
        if (adopted != null) {
            return adopted;
        }
        report("Installed pgwire-govdata (" + describe(installed) + ") is older than this "
            + "engine's " + target + " — starting the server from it while " + target
            + " downloads in the background. Until then it may lack schemas or columns the "
            + "engine expects.");
        background.execute(() -> updateInBackground(dir, target));
        return existing;
    }

    private static void startDaemon(Runnable task) {
        Thread t = new Thread(task, "askamerica-pgwire-bundle-update");
        t.setDaemon(true);
        t.start();
    }

    /** Where a downloaded, verified and extracted release waits to be swapped in. */
    static Path readyDir(Path dir, String version) {
        return dir.resolveSibling(dir.getFileName() + ".ready-" + version);
    }

    /** True once {@link #updateInBackground} has finished preparing {@code version}: the
     *  directory only ever appears by rename of a complete, stamped extraction. */
    static boolean isPrepared(Path dir, String version) {
        Path ready = readyDir(dir, version);
        return launcherPath(ready).isFile() && version.equals(readMarker(ready));
    }

    /**
     * Swaps a prepared release in and returns its launcher, or null when there is none to
     * adopt yet. Uses {@code tryLock}: a held lock means another process is installing or
     * adopting right now, and a spawn must not wait on it.
     */
    static File adoptPrepared(Path dir, String version) {
        if (!isPrepared(dir, version)) {
            return null;
        }
        Path lockFile = lockFile(dir);
        try (FileChannel ch = FileChannel.open(lockFile,
                 StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock lock = tryLock(ch)) {
            if (lock == null) {
                return null;
            }
            if (decide(launcherPath(dir).isFile(), readMarker(dir), version) != Action.UPDATE) {
                // Another process adopted it while this one waited for the lock.
                return launcherPath(dir);
            }
            if (!isPrepared(dir, version)) {
                return null;
            }
            swapIn(readyDir(dir, version), dir);
            report("pgwire-govdata updated to " + version + " at " + launcherPath(dir));
            return launcherPath(dir);
        } catch (IOException e) {
            report("Could not swap in the downloaded pgwire-govdata " + version + " ("
                + e.getMessage() + ") — keeping the installed bundle; the next spawn retries.");
            return null;
        }
    }

    /**
     * Downloads, verifies and extracts {@code version} into {@link #readyDir} while the server
     * runs on the installed bundle.
     *
     * <p>When another process holds the lock this waits for it rather than giving up. Claude
     * Desktop starts connectors and tears them down seconds later; the one that won the lock is
     * as likely as any to be the one torn down, and if the survivors had skipped, nothing would
     * download until the next spawn (2026-10-01: the lock holder lived three seconds). The
     * operating system releases a dead holder's lock, so a waiter takes over and resumes its
     * partial download; if the holder finishes instead, the waiter finds the release prepared
     * and returns. Only a second update in THIS process is skipped.
     */
    static void updateInBackground(Path dir, String version) {
        Path lockFile = lockFile(dir);
        try (FileChannel ch = FileChannel.open(lockFile,
                 StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock lock = lockOrWait(ch)) {
            if (lock == null) {
                return;
            }
            if (decide(launcherPath(dir).isFile(), readMarker(dir), version) != Action.UPDATE
                || isPrepared(dir, version)) {
                return;
            }
            Path staging = prepareRelease(dir, version);
            if (staging == null) {
                return;
            }
            Files.move(staging, readyDir(dir, version),
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            report("pgwire-govdata " + version + " downloaded and verified — it replaces the "
                + "installed bundle the next time the server is started.");
        } catch (InstallFailedException | IOException e) {
            report("Background pgwire-govdata update to " + version + " FAILED ("
                + e.getMessage() + ") — the installed bundle stays in service; the next spawn "
                + "retries, resuming any partial download.");
        }
    }

    private static Path lockFile(Path dir) {
        return dir.resolveSibling(dir.getFileName() + ".install.lock");
    }

    /** The lock, waiting for another process to release it; null when another thread of THIS
     *  process holds it, since that thread is already doing the update. */
    private static FileLock lockOrWait(FileChannel ch) throws IOException {
        try {
            FileLock lock = ch.tryLock();
            if (lock != null) {
                return lock;
            }
            report("Another AskAmerica process is already updating pgwire-govdata — waiting "
                + "to take over if it is closed before it finishes.");
            return ch.lock();
        } catch (java.nio.channels.OverlappingFileLockException e) {
            return null;
        }
    }

    /** {@code tryLock} that also answers null when another thread of THIS process holds the
     *  lock, which the JDK reports as an exception rather than as a lock not acquired. */
    private static FileLock tryLock(FileChannel ch) throws IOException {
        try {
            return ch.tryLock();
        } catch (java.nio.channels.OverlappingFileLockException e) {
            return null;
        }
    }

    /** The action for a bundle directory: install when there is none, update when the installed
     *  release is older than {@code target} or unstamped, keep otherwise. Never downgrades. */
    static Action decide(boolean haveLauncher, String installed, String target) {
        if (!haveLauncher) {
            return Action.INSTALL;
        }
        if (installed == null) {
            return Action.UPDATE;
        }
        int c = compareVersions(installed, target);
        if (c == 0) {
            return Action.KEEP;
        }
        return c < 0 ? Action.UPDATE : Action.KEEP_NEWER;
    }

    /** Numeric, dot-separated comparison ({@code 0.100.0 > 0.99.2}); a non-numeric part is
     *  compared as text, and a missing part counts as 0. */
    static int compareVersions(String a, String b) {
        String[] x = a.trim().split("\\.");
        String[] y = b.trim().split("\\.");
        for (int i = 0; i < Math.max(x.length, y.length); i++) {
            String p = i < x.length ? x[i] : "0";
            String q = i < y.length ? y[i] : "0";
            int c;
            if (p.matches("\\d+") && q.matches("\\d+")) {
                c = new java.math.BigInteger(p).compareTo(new java.math.BigInteger(q));
            } else {
                c = p.compareTo(q);
            }
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    /**
     * The release the bundle must match: this engine's own version when its jar is stamped,
     * else (a local build with no stamped bundle yet) the newest published release. Throws when
     * neither can be read.
     */
    static String targetVersion(String own) throws IOException, InterruptedException {
        if (own != null) {
            return own;
        }
        String latest = firstMatch(fetchLatestReleaseJson(),
            "\"tag_name\"\\s*:\\s*\"engine-v([^\"]+)\"");
        if (latest == null) {
            throw new IOException("this engine carries no version stamp (a local build) and the "
                + "latest release has no engine-v tag");
        }
        report("This engine carries no version stamp (a local build) — matching pgwire-govdata "
            + "to the latest release, " + latest + ".");
        return latest;
    }

    private static volatile String ownVersionCache;
    private static volatile boolean ownVersionRead;

    /**
     * {@code AskAmerica-Engine-Version} from the manifest of the jar this class was loaded from,
     * or null for an unstamped jar or a classes directory (tests, local runs). Read locally, not
     * through {@link EngineInstaller}: that class can be loaded by a different classloader than
     * this one (see SetupWindow's cachedEngineJar doc), and a package-private call across that
     * boundary fails with IllegalAccessError.
     */
    static String ownEngineVersion() {
        if (ownVersionRead) {
            return ownVersionCache;
        }
        String v = null;
        try {
            java.security.CodeSource cs =
                PgwireGovDataInstaller.class.getProtectionDomain().getCodeSource();
            if (cs != null && cs.getLocation() != null) {
                File f = new File(cs.getLocation().toURI());
                if (f.isFile()) {
                    try (java.util.jar.JarFile jf = new java.util.jar.JarFile(f)) {
                        java.util.jar.Manifest mf = jf.getManifest();
                        v = mf == null ? null
                            : mf.getMainAttributes().getValue("AskAmerica-Engine-Version");
                    }
                }
            }
        } catch (IOException | java.net.URISyntaxException | SecurityException e) {
            report("Could not read this engine's own version stamp (" + e.getMessage()
                + ") — treating it as a local build.");
        }
        ownVersionCache = v;
        ownVersionRead = true;
        return v;
    }

    /** The release stamped in {@code bundleDir}, or null when the bundle carries none. */
    static String readMarker(Path bundleDir) {
        Path m = bundleDir.resolve(MARKER);
        if (!Files.isRegularFile(m)) {
            return null;
        }
        try {
            String v = Files.readString(m).trim();
            return v.isEmpty() ? null : v;
        } catch (IOException e) {
            report("Could not read " + m + " (" + e.getMessage() + ") — treating the installed "
                + "bundle as unversioned.");
            return null;
        }
    }

    private static String describe(String installed) {
        return installed == null ? "unversioned" : "release " + installed;
    }

    /**
     * Installs {@code version} where there is no bundle at all, under a cross-process file
     * lock, re-checking inside the lock so a process that waited while another one installed
     * the same release does not repeat it. Returns null when that release publishes no asset
     * for this OS.
     */
    private static File installUnderLock(Path dir, String version) {
        Path lockFile = lockFile(dir);
        try {
            Files.createDirectories(lockFile.getParent());
            try (FileChannel ch = FileChannel.open(lockFile,
                     StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 FileLock ignored = ch.lock()) {
                File now = launcherPath(dir);
                if (now.isFile()) {
                    return now;
                }
                Path staging = prepareRelease(dir, version);
                if (staging == null) {
                    return null;
                }
                swapIn(staging, dir);
                report("pgwire-govdata " + version + " ready at " + launcherPath(dir));
                return launcherPath(dir);
            }
        } catch (IOException e) {
            throw new InstallFailedException("Could not install pgwire-govdata " + version
                + " under " + lockFile + ": " + e.getMessage(), e);
        }
    }

    /** The partial download of {@code version}; a stable name, so an interrupted download is
     *  resumed by the next attempt instead of restarted. */
    static Path partFile(Path dir, String version, String variant) {
        return dir.resolveSibling(dir.getFileName() + "-" + version + "-" + variant
            + ".tar.gz.part");
    }

    /**
     * Removes what a killed install left beside {@code dir}: staging directories, partial
     * downloads other than {@code keepPart}, and prepared releases other than {@code version}.
     * The caller must hold the install lock, which makes it the only writer of these.
     */
    static void removeOrphans(Path dir, String version, Path keepPart) throws IOException {
        String name = dir.getFileName().toString();
        java.util.List<Path> orphans = new java.util.ArrayList<>();
        try (java.util.stream.Stream<Path> siblings = Files.list(dir.toAbsolutePath().getParent())) {
            siblings.forEach(p -> {
                String n = p.getFileName().toString();
                boolean staging = n.startsWith(name + ".staging-");
                boolean part = n.startsWith(name + "-") && n.endsWith(".tar.gz.part")
                    && !p.equals(keepPart);
                boolean otherReady = n.startsWith(name + ".ready-")
                    && !n.equals(name + ".ready-" + version);
                if (staging || part || otherReady) {
                    orphans.add(p);
                }
            });
        }
        for (Path p : orphans) {
            deleteRecursively(p);
            report("Removed leftover " + p.getFileName() + " from an interrupted install.");
        }
    }

    /**
     * Downloads, verifies and extracts the bundle of release {@code version} into a staging
     * directory beside {@code dir} and returns it, stamped and ready to be renamed into place.
     * Returns null when that release publishes no asset for this OS. The caller must hold the
     * install lock.
     */
    private static Path prepareRelease(Path dir, String version) {
        String variant = osVariant();
        String json;
        try {
            json = fetchText(RELEASE_BY_TAG + "engine-v" + version, "application/vnd.github+json");
        } catch (Exception e) {
            throw new InstallFailedException("Could not look up release engine-v" + version
                + " on GitHub: " + e.getMessage(), e);
        }
        String assetUrl = firstMatch(json,
            "\"browser_download_url\"\\s*:\\s*\"([^\"]*pgwire-govdata-[^\"]*-"
            + Pattern.quote(variant) + "\\.tar\\.gz)\"");
        if (assetUrl == null) {
            report("No pgwire-govdata-*-" + variant + ".tar.gz asset in release engine-v"
                + version + " — nothing to install.");
            return null;
        }
        String expectedSha256 = requiredChecksum(assetUrl + ".sha256", url -> fetchText(url));
        report("Downloading pgwire-govdata " + version + " (" + variant + ") from " + assetUrl);
        Path staging = dir.resolveSibling(dir.getFileName() + ".staging-"
            + ProcessHandle.current().pid());
        Path part = partFile(dir, version, variant);
        try {
            removeOrphans(dir, version, part);
            // A failed download keeps the partial file so the next attempt resumes it.
            download(assetUrl, part);
            try {
                verifySha256(part, expectedSha256);
                Files.createDirectories(staging);
                extract(part, staging);
            } catch (IOException | InterruptedException e) {
                // A complete file that fails verification or extraction can never succeed on a
                // retry; discard it so the next attempt downloads afresh.
                Files.deleteIfExists(part);
                throw e;
            }
            Files.deleteIfExists(part);
            File staged = launcherPath(staging);
            if (!staged.isFile()) {
                throw new IOException("the extracted bundle has no launcher at " + staged
                    + " — the bundle's layout may have changed");
            }
            if (!staged.canExecute()) {
                staged.setExecutable(true);
            }
            Files.writeString(staging.resolve(MARKER), version);
            return staging;
        } catch (Exception e) {
            try {
                deleteRecursively(staging);
            } catch (IOException cleanup) {
                report("Could not remove the staging directory " + staging + ": "
                    + cleanup.getMessage());
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new InstallFailedException("Failed to install pgwire-govdata " + version
                + " from " + assetUrl + ": " + e.getClass().getSimpleName() + ": "
                + e.getMessage(), e);
        }
    }

    /** Fetches a URL's text; the seam {@link #requiredChecksum} is tested through. */
    interface TextFetcher {
        String fetch(String url) throws IOException, InterruptedException;
    }

    /**
     * The published sha256 for a bundle, or {@link InstallFailedException}. Mandatory: a bundle
     * this size installed without verification is how a broken CI checksum step once went
     * unnoticed, so an unreachable or empty checksum stops the install instead of skipping it.
     */
    static String requiredChecksum(String sha256Url, TextFetcher fetcher) {
        String text;
        try {
            text = fetcher.fetch(sha256Url);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new InstallFailedException("Could not fetch the published checksum "
                + sha256Url + " (" + e.getMessage() + ") — refusing to install an unverified "
                + "bundle.", e);
        }
        String hex = text == null ? "" : text.trim().split("\\s+")[0];
        if (!hex.matches("(?i)[0-9a-f]{64}")) {
            throw new InstallFailedException("The published checksum " + sha256Url
                + " is not a sha256 digest — refusing to install an unverified bundle.", null);
        }
        return hex;
    }

    /**
     * Replaces {@code dir} with {@code staging} by rename. The running server's bookkeeping files
     * ({@link #CARRY_OVER}) are copied across first; the old directory is moved aside and then
     * deleted. On a failed rename the old directory is restored. On Windows a directory whose
     * files a running server holds open cannot be moved, which surfaces as the IOException and
     * leaves the working bundle in place.
     */
    static void swapIn(Path staging, Path dir) throws IOException {
        Path old = null;
        if (Files.exists(dir)) {
            for (String f : CARRY_OVER) {
                Path src = dir.resolve(f);
                if (Files.isRegularFile(src)) {
                    Files.copy(src, staging.resolve(f),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
            }
            old = dir.resolveSibling(dir.getFileName() + ".old-" + System.currentTimeMillis());
            Files.move(dir, old, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        }
        try {
            Files.move(staging, dir, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            if (old != null) {
                Files.move(old, dir, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            }
            throw e;
        }
        if (old != null) {
            try {
                deleteRecursively(old);
            } catch (IOException e) {
                report("Replaced the pgwire-govdata bundle but could not delete the old copy at "
                    + old + " (" + e.getMessage() + "); it is unused and can be removed.");
            }
        }
    }

    static void deleteRecursively(Path p) throws IOException {
        if (!Files.exists(p, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(p)) {
            java.util.List<Path> all = new java.util.ArrayList<>();
            walk.forEach(all::add);
            java.util.Collections.reverse(all);
            for (Path q : all) {
                Files.delete(q);
            }
        }
    }

    static File launcherPath(Path base) {
        boolean windows = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
        // NOT .exe: the Windows leg of this bundle is a generated .bat (see
        // pgwire-adapters-release.yml's "Stage adapter launcher (Windows)" step) — there is
        // no compiled Windows executable at all. Confirmed live this mismatch meant a fully
        // successful download+extraction was still reported as "no launcher binary found"
        // on Windows, unconditionally, independent of anything else.
        String binName = windows ? "pgwire-govdata.bat" : "pgwire-govdata";
        return new File(new File(base.toFile(), "bin"), binName);
    }

    /**
     * Matches the {@code os.variant} labels the release matrix actually publishes
     * (see .github/workflows/pgwire-adapters-release.yml): {@code linux-x86_64},
     * {@code macos-arm64}, {@code windows-x86_64}. Intel Macs are not published yet.
     */
    private static String osVariant() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        if (os.contains("win")) {
            return "windows-x86_64";
        }
        if (os.contains("mac")) {
            if (!arch.contains("aarch64") && !arch.contains("arm")) {
                throw new IllegalStateException(
                    "pgwire-govdata is only published for Apple Silicon Macs (macos-arm64); "
                    + "this machine reports os.arch=" + arch);
            }
            return "macos-arm64";
        }
        return "linux-x86_64";
    }

    private static String fetchLatestReleaseJson() throws IOException, InterruptedException {
        return fetchText(LATEST_API, "application/vnd.github+json");
    }

    private static String fetchText(String url) throws IOException, InterruptedException {
        return fetchText(url, null);
    }

    private static String fetchText(String url, String accept) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(10))
            .build();
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
            .header("User-Agent", "askamerica-mcp-pgwire-installer")
            .timeout(Duration.ofSeconds(10))
            .GET();
        if (accept != null) {
            b.header("Accept", accept);
        }
        HttpResponse<String> resp = client.send(b.build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("HTTP " + resp.statusCode() + " from " + url);
        }
        return resp.body();
    }

    private static String firstMatch(String text, String regex) {
        Matcher m = Pattern.compile(regex).matcher(text);
        return m.find() ? m.group(1) : null;
    }

    /**
     * Downloads {@code url} into {@code part}, resuming from the bytes already there. A server
     * that ignores the range answers 200 with the whole asset, which overwrites the partial
     * file; 416 means the partial file already holds every byte, which the caller's sha256
     * check confirms or rejects.
     */
    static void download(String url, Path part) throws IOException, InterruptedException {
        long have = Files.isRegularFile(part) ? Files.size(part) : 0L;
        HttpClient client = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(30))
            .build();
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(url))
            .header("User-Agent", "askamerica-mcp-pgwire-installer")
            .GET();
        if (have > 0) {
            req.header("Range", "bytes=" + have + "-");
        }
        HttpResponse<InputStream> resp =
            client.send(req.build(), HttpResponse.BodyHandlers.ofInputStream());
        int status = resp.statusCode();
        if (have > 0 && status == 416) {
            resp.body().close();
            return;
        }
        if (status != 200 && status != 206) {
            resp.body().close();
            throw new IOException("pgwire-govdata download failed: HTTP " + status
                + " from " + url);
        }
        boolean resume = have > 0 && status == 206;
        if (resume) {
            report("Resuming the pgwire-govdata download at " + (have / (1024 * 1024)) + " MB.");
        }
        try (InputStream in = resp.body();
             OutputStream out = Files.newOutputStream(part, StandardOpenOption.CREATE,
                 resume ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING)) {
            byte[] buf = new byte[1 << 16];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
        }
    }

    static void verifySha256(Path file, String expectedHex) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream in = Files.newInputStream(file)) {
                byte[] buf = new byte[1 << 16];
                int n;
                while ((n = in.read(buf)) != -1) {
                    digest.update(buf, 0, n);
                }
            }
            StringBuilder hex = new StringBuilder();
            for (byte b : digest.digest()) {
                hex.append(String.format(Locale.ROOT, "%02x", b));
            }
            String actual = hex.toString();
            String expected = expectedHex.split("\\s+")[0].toLowerCase(Locale.ROOT);
            if (!actual.equalsIgnoreCase(expected)) {
                throw new IOException(
                    "pgwire-govdata download sha256 mismatch: expected " + expected
                    + ", got " + actual);
            }
        } catch (java.security.NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed present on every JDK; unreachable in practice.
            throw new IOException(e);
        }
    }

    /**
     * Shells out to the system {@code tar} rather than adding a tar/gzip library dependency —
     * present on macOS, Linux, and Windows 10 1803+ (bsdtar), which this project already assumes
     * for its jpackage MSI builds. Strips the tarball's single top-level
     * {@code pgwire-govdata-<version>-<variant>/} directory so the bundle lands flat under
     * {@code destDir} (i.e. {@code destDir/bin/pgwire-govdata}, not one directory deeper).
     */
    private static void extract(Path tarGz, Path destDir) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(
            "tar", "-xzf", tarGz.toAbsolutePath().toString(),
            "--strip-components=1", "-C", destDir.toAbsolutePath().toString());
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String output;
        try (InputStream in = p.getInputStream()) {
            output = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
        int exit = p.waitFor();
        if (exit != 0) {
            throw new IOException("tar extraction failed (exit " + exit + "): " + output);
        }
    }

    private static void report(String message) {
        java.io.PrintStream out = McpServer.log != null ? McpServer.log : System.err;
        out.println("[askamerica-mcp] " + message);
    }
}
