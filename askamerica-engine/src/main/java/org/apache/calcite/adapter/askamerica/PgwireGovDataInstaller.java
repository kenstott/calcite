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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Downloads and extracts the airgapped {@code pgwire-govdata-<version>-<os>.tar.gz} bundle on
 * first use of {@code ASKAMERICA_PGWIRE_MODE}, mirroring {@link EngineInstaller}'s lazy-download
 * pattern for the fat engine jar — but for the shared server binary instead, and only reached at
 * all when a user has actually opted into pgwire mode, since the bundle (a standalone CPython +
 * JRE + the govdata module's runtime jars) is far larger than the engine jar and would be wasted
 * bandwidth/disk for the default embedded-DuckDB deployment every other install uses.
 *
 * <p>Unlike the engine jar, this does not re-check for staleness on every run: once extracted
 * under {@code ~/.askamerica/pgwire-govdata/}, it is used as-is. A user who wants to force an
 * update can delete that directory (or point {@code ASKAMERICA_PGWIRE_LAUNCHER} elsewhere).
 */
final class PgwireGovDataInstaller {

    private PgwireGovDataInstaller() {
    }

    private static final String LATEST_API =
        "https://api.github.com/repos/kenstott/calcite/releases/latest";

    static Path cacheDir() {
        return Paths.get(System.getProperty("user.home"), ".askamerica", "pgwire-govdata");
    }

    /**
     * Downloads and extracts the bundle if not already present, returning the resolved launcher
     * binary. Returns null (never throws) on any failure — the caller falls back to the embedded
     * engine, which must never be blocked by this best-effort convenience path.
     */
    static File ensureLauncher() {
        File already = launcherPath(cacheDir());
        if (already.isFile()) {
            return already;
        }
        try {
            String variant = osVariant();
            String json = fetchLatestReleaseJson();
            String tag = firstMatch(json, "\"tag_name\"\\s*:\\s*\"([^\"]+)\"");
            String assetUrl = firstMatch(json,
                "\"browser_download_url\"\\s*:\\s*\"([^\"]*pgwire-govdata-[^\"]*-"
                + Pattern.quote(variant) + "\\.tar\\.gz)\"");
            if (assetUrl == null) {
                report("No pgwire-govdata-*-" + variant + ".tar.gz asset in the latest release ("
                    + tag + ") — falling back to the embedded engine.");
                return null;
            }
            String sha256Url = assetUrl + ".sha256";
            String expectedSha256 = null;
            try {
                expectedSha256 = fetchText(sha256Url);
            } catch (IOException | InterruptedException e) {
                report("Could not fetch " + sha256Url + " (" + e.getMessage()
                    + ") — proceeding without integrity verification.");
            }

            report("Downloading pgwire-govdata (" + variant + ", one-time) from " + assetUrl);
            Path tmp = Files.createTempFile("pgwire-govdata-", ".tar.gz");
            try {
                download(assetUrl, tmp);
                if (expectedSha256 != null) {
                    verifySha256(tmp, expectedSha256.trim());
                }
                Files.createDirectories(cacheDir());
                extract(tmp, cacheDir());
            } finally {
                Files.deleteIfExists(tmp);
            }
            File launcher = launcherPath(cacheDir());
            if (!launcher.isFile()) {
                report("Extracted pgwire-govdata bundle but no launcher binary found at "
                    + launcher + " — falling back to the embedded engine.");
                return null;
            }
            if (!launcher.canExecute()) {
                launcher.setExecutable(true);
            }
            report("pgwire-govdata ready at " + launcher);
            return launcher;
        } catch (Exception e) {
            report("Failed to install pgwire-govdata (" + e.getClass().getSimpleName() + ": "
                + e.getMessage() + ") — falling back to the embedded engine.");
            return null;
        }
    }

    private static File launcherPath(Path base) {
        boolean windows = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
        String binName = windows ? "pgwire-govdata.exe" : "pgwire-govdata";
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

    private static void download(String url, Path dest) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(30))
            .build();
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
            .header("User-Agent", "askamerica-mcp-pgwire-installer")
            .GET()
            .build();
        HttpResponse<InputStream> resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());
        if (resp.statusCode() != 200) {
            throw new IOException("pgwire-govdata download failed: HTTP " + resp.statusCode()
                + " from " + url);
        }
        try (InputStream in = resp.body();
             OutputStream out = Files.newOutputStream(dest, StandardOpenOption.TRUNCATE_EXISTING)) {
            byte[] buf = new byte[1 << 16];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
        }
    }

    private static void verifySha256(Path file, String expectedHex) throws IOException {
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
