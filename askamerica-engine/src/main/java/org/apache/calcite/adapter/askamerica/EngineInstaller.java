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

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GraphicsEnvironment;
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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;

/**
 * Resolves — and, on first run, downloads — the fat {@code askamerica-engine.jar}.
 *
 * <p>This class runs inside the thin jpackage launcher, <em>before</em> the engine
 * JAR is on the classpath. It must therefore depend on nothing the engine JAR
 * provides (e.g. Jackson) — JDK classes only.
 *
 * <p>The jar is cached under {@code ~/.askamerica/engine/askamerica-engine.jar} —
 * the same location the Python client uses — so a machine that has either the pip
 * package or the native app shares a single copy.
 */
final class EngineInstaller {

    private EngineInstaller() {
    }

    /** Stable, version-less asset URL — always the latest engine release. */
    private static final String DEFAULT_URL =
        "https://github.com/kenstott/calcite/releases/latest/download/askamerica-engine.jar";

    /** Newest published release; {@code tag_name} is {@code engine-vX.Y.Z}. */
    private static final String LATEST_API =
        "https://api.github.com/repos/kenstott/calcite/releases/latest";

    static Path cacheJar() {
        return Paths.get(System.getProperty("user.home"),
            ".askamerica", "engine", "askamerica-engine.jar");
    }

    /**
     * First existing jar among: {@code ASKAMERICA_ENGINE_JAR} override, a jar sitting
     * beside the launcher (an operator dropping one into the app bundle), then the
     * shared cache — written by whichever of the setup wizard or the pip client ran
     * first, and shared between them. Returns null if none is present yet.
     */
    static Path resolveExisting(File launcherDir) {
        String env = System.getenv("ASKAMERICA_ENGINE_JAR");
        if (env != null && !env.isEmpty()) {
            Path p = Paths.get(env);
            if (Files.exists(p)) {
                return p;
            }
        }
        if (launcherDir != null) {
            Path beside = launcherDir.toPath().resolve("askamerica-engine.jar");
            if (Files.exists(beside)) {
                return beside;
            }
        }
        Path cache = cacheJar();
        if (Files.exists(cache)) {
            return cache;
        }
        return null;
    }

    /**
     * Returns a usable engine jar, downloading it to the shared cache if necessary.
     *
     * @param launcherDir directory containing the launcher jar (may be null)
     * @param serverMode  true in {@code --mcp} server mode; see
     *                    {@link #progressFor(boolean, long)} for why that does not
     *                    by itself mean the download has to be invisible
     */
    static Path ensure(File launcherDir, boolean serverMode)
        throws IOException, InterruptedException {
        Path dest = cacheJar();
        Path existing = resolveExisting(launcherDir);
        if (existing != null && !existing.equals(dest)) {
            // ASKAMERICA_ENGINE_JAR, or a jar dropped beside the launcher, is a deliberate
            // pin by an operator — report the version but never replace it.
            report("Using pinned engine jar " + existing + " (" + describe(existing) + ").");
            return existing;
        }
        if (existing != null && !isStale(existing)) {
            return existing;
        }
        Files.createDirectories(dest.getParent());
        String url = System.getenv().getOrDefault("ASKAMERICA_ENGINE_URL", DEFAULT_URL);
        download(url, dest, serverMode);
        return dest;
    }

    /**
     * Chooses how to report a download that takes minutes.
     *
     * <p>Server mode is not the same thing as having nowhere to draw. Claude Desktop
     * spawns the MCP server inside the user's GUI session, and the pre-init phase this
     * download runs in has no MCP session to send notifications through — the jar is
     * being fetched precisely so the server can start. Reporting only to stderr there
     * means the user watches Claude Desktop sit still for minutes with no indication
     * why, which is the "mysterious wait" this exists to prevent. So a window is used
     * whenever one can be drawn, in server mode too.
     *
     * <p>Falls back to stderr when there is genuinely no display — CI, ssh, a Linux
     * daemon — or if the toolkit refuses to initialise, which must never be fatal to
     * a download that is otherwise fine.
     */
    private static Progress progressFor(boolean serverMode, long total) {
        if (GraphicsEnvironment.isHeadless()) {
            return new ConsoleProgress();
        }
        if (serverMode) {
            // Also on stderr: Claude Desktop records it, so a support log still explains
            // the pause even though the user saw a window.
            System.err.println("Downloading AskAmerica engine ("
                + (total > 0 ? (total / (1024 * 1024)) + " MB" : "size unknown")
                + ", one-time) — progress window opened.");
        }
        // DialogProgress falls back to stderr itself if the window cannot be drawn.
        return new DialogProgress();
    }

    /**
     * True when the cached jar is a different release from the newest published one.
     *
     * <p>An unreachable GitHub means "cannot tell", which resolves to not-stale: a check
     * that could not run must never discard a working install. It is reported rather than
     * swallowed, though — a silent "cannot tell" is indistinguishable from a successful
     * check, and that is how a jar cached once stayed in service through thirty releases.
     *
     * <p>A cached jar with no stamped version is treated as stale, not as unknown. Every
     * release from the one that introduced {@code AskAmerica-Engine-Version} onward carries
     * it, so an unstamped jar in the cache is by definition older than that release — and
     * resolving it to "keep" would leave every install that predates stamping pinned
     * forever, which is the bug this check exists to end. A jar that must not be replaced
     * belongs in {@code ASKAMERICA_ENGINE_JAR} or beside the launcher; those are returned
     * before this method is reached.
     */
    private static boolean isStale(Path jar) {
        String have;
        String latest;
        try {
            have = jarVersion(jar);
            latest = latestVersion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            report("Engine update check interrupted — keeping the cached jar.");
            return false;
        } catch (IOException e) {
            report("Engine update check failed (" + e.getMessage()
                + ") — keeping the cached jar.");
            return false;
        }
        if (latest == null) {
            report("Engine update check inconclusive (no engine-v tag in the release "
                + "response) — keeping the cached jar.");
            return false;
        }
        if (have == null) {
            report("Cached engine carries no version stamp, so it predates " + latest
                + " — updating.");
            return true;
        }
        if (have.equals(latest)) {
            return false;
        }
        report("Cached engine " + have + " is superseded by " + latest + " — updating.");
        return true;
    }

    /**
     * Engine release version from a jar manifest, or null when the jar carries none —
     * a local build, or any jar published before the attribute was stamped.
     *
     * <p>Not {@code Implementation-Version}: that carries the Apache Calcite version on
     * every jar this repo builds, which would compare unequal to every engine release tag.
     */
    static String jarVersion(Path jar) throws IOException {
        try (java.util.jar.JarFile jf = new java.util.jar.JarFile(jar.toFile())) {
            java.util.jar.Manifest mf = jf.getManifest();
            if (mf == null) {
                return null;
            }
            return mf.getMainAttributes().getValue("AskAmerica-Engine-Version");
        }
    }

    /**
     * Version of the newest published engine release, or null when the response carried
     * no recognisable {@code engine-v} tag. Parsed with a regex rather than a JSON
     * library because this class runs before the engine jar is on the classpath.
     */
    static String latestVersion() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
        HttpRequest req = HttpRequest.newBuilder(URI.create(LATEST_API))
            .header("User-Agent", "askamerica-mcp-launcher")
            .header("Accept", "application/vnd.github+json")
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("release lookup returned HTTP " + resp.statusCode());
        }
        java.util.regex.Matcher m = java.util.regex.Pattern
            .compile("\"tag_name\"\\s*:\\s*\"engine-v([^\"]+)\"")
            .matcher(resp.body());
        return m.find() ? m.group(1) : null;
    }

    /** Best-effort version label for diagnostics. */
    private static String describe(Path jar) {
        try {
            String v = jarVersion(jar);
            return v == null ? "no Implementation-Version" : "version " + v;
        } catch (IOException e) {
            return "unreadable manifest: " + e.getMessage();
        }
    }

    /**
     * Launcher diagnostics. stderr in both modes: it is what Claude Desktop captures for
     * an MCP server, and the interactive path has no console of its own to write to.
     */
    private static void report(String message) {
        System.err.println("[askamerica-launcher] " + message);
    }

    private static void download(String url, Path dest, boolean serverMode)
        throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(30))
            .build();
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
            .header("User-Agent", "askamerica-mcp-launcher")
            .GET()
            .build();
        HttpResponse<InputStream> resp =
            client.send(req, HttpResponse.BodyHandlers.ofInputStream());
        if (resp.statusCode() != 200) {
            throw new IOException(
                "Engine download failed: HTTP " + resp.statusCode() + " from " + url);
        }
        long total = resp.headers().firstValueAsLong("content-length").orElse(-1L);

        Progress progress = progressFor(serverMode, total);
        progress.start(total);

        // Download to a sibling temp file, then atomically move into place so a
        // partial download can never be mistaken for a complete engine jar.
        Path tmp = Files.createTempFile(dest.getParent(), "engine-", ".part");
        try (InputStream in = resp.body();
             OutputStream out = Files.newOutputStream(tmp, StandardOpenOption.TRUNCATE_EXISTING)) {
            byte[] buf = new byte[1 << 16];
            long read = 0;
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                read += n;
                progress.update(read, total);
            }
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            progress.done();
            throw e;
        }
        try {
            Files.move(tmp, dest,
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicUnsupported) {
            Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING);
        }
        progress.done();
    }

    // ── progress reporting ──────────────────────────────────────────────────

    private interface Progress {
        void start(long total);

        void update(long read, long total);

        void done();
    }

    /** Server (--mcp) mode: carriage-return percentage on stderr. */
    private static final class ConsoleProgress implements Progress {
        private long last = -1;

        @Override public void start(long total) {
            System.err.println("Downloading AskAmerica engine ("
                + (total > 0 ? (total / (1024 * 1024)) + " MB" : "size unknown")
                + ", one-time)...");
        }

        @Override public void update(long read, long total) {
            if (total <= 0) {
                return;
            }
            long pct = read * 100 / total;
            if (pct != last) {
                last = pct;
                System.err.print("\r  " + pct + "%");
                System.err.flush();
            }
        }

        @Override public void done() {
            System.err.println();
        }
    }

    /** Interactive launch: a small, self-contained Swing progress window. */
    private static final class DialogProgress implements Progress {
        private JFrame frame;
        private JProgressBar bar;
        /**
         * Used when the window cannot be created. Degrading to silence would reproduce
         * the failure this class exists to avoid: a multi-minute download with nothing
         * on screen and nothing in the log.
         */
        private ConsoleProgress fallback;

        @Override public void start(long total) {
            try {
                SwingUtilities.invokeAndWait(() -> {
                    frame = new JFrame("AskAmerica MCP");
                    frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
                    JPanel p = new JPanel(new BorderLayout(0, 12));
                    p.setBorder(BorderFactory.createEmptyBorder(24, 28, 24, 28));
                    JLabel label = new JLabel("Setting up AskAmerica engine (one-time, ~"
                        + (total > 0 ? total / (1024 * 1024) + " MB" : "large") + ")...");
                    bar = new JProgressBar(0, 100);
                    bar.setIndeterminate(total <= 0);
                    bar.setStringPainted(total > 0);
                    bar.setPreferredSize(new Dimension(380, 22));
                    p.add(label, BorderLayout.NORTH);
                    p.add(bar, BorderLayout.CENTER);
                    frame.setContentPane(p);
                    frame.pack();
                    frame.setResizable(false);
                    frame.setLocationRelativeTo(null);
                    frame.setVisible(true);
                });
            } catch (Exception noDisplay) {
                // The toolkit refused despite a non-headless environment (no $DISPLAY, a
                // locked-down session). Report on stderr rather than showing nothing.
                frame = null;
                fallback = new ConsoleProgress();
                fallback.start(total);
            }
        }

        @Override public void update(long read, long total) {
            if (fallback != null) {
                fallback.update(read, total);
                return;
            }
            if (frame == null || total <= 0) {
                return;
            }
            int pct = (int) (read * 100 / total);
            SwingUtilities.invokeLater(() -> {
                if (bar != null) {
                    bar.setValue(pct);
                }
            });
        }

        @Override public void done() {
            if (fallback != null) {
                fallback.done();
                return;
            }
            if (frame == null) {
                return;
            }
            SwingUtilities.invokeLater(() -> frame.dispose());
        }
    }
}
