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

    static Path cacheJar() {
        return Paths.get(System.getProperty("user.home"),
            ".askamerica", "engine", "askamerica-engine.jar");
    }

    /**
     * First existing jar among: {@code ASKAMERICA_ENGINE_JAR} override, a jar sitting
     * beside the launcher (mac postinstall pre-warm), then the shared cache.
     * Returns null if none is present yet.
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
     * @param headless    true in {@code --mcp} server mode (console progress);
     *                    false for interactive launch (Swing progress dialog)
     */
    static Path ensure(File launcherDir, boolean headless)
        throws IOException, InterruptedException {
        Path existing = resolveExisting(launcherDir);
        if (existing != null) {
            return existing;
        }
        Path dest = cacheJar();
        Files.createDirectories(dest.getParent());
        String url = System.getenv().getOrDefault("ASKAMERICA_ENGINE_URL", DEFAULT_URL);
        download(url, dest, headless);
        return dest;
    }

    private static void download(String url, Path dest, boolean headless)
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

        Progress progress = headless ? new ConsoleProgress() : new DialogProgress();
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
            } catch (Exception headlessOrInterrupted) {
                // No display available — degrade silently; the download still runs.
                frame = null;
            }
        }

        @Override public void update(long read, long total) {
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
            if (frame == null) {
                return;
            }
            SwingUtilities.invokeLater(() -> frame.dispose());
        }
    }
}
