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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Cursor;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Image;
import java.awt.Insets;
import java.awt.Toolkit;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextArea;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

/**
 * First-run setup wizard for AskAmerica MCP.
 *
 * Writes the mcpServers block into Claude Desktop's config file so the
 * user never needs to touch JSON manually.
 */
public class SetupWindow {

    private static final Color AMBER  = new Color(0xF5A623);
    private static final Color BG     = new Color(0x0A0A0A);
    private static final Color CARD   = new Color(0x141414);
    private static final Color BORDER = new Color(0x2A2A2A);
    private static final Color TEXT   = new Color(0xC8C8C8);
    private static final Color DIM    = new Color(0x666666);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JFrame frame;
    private JPasswordField apiKeyField;
    private JCheckBox telemetryCheckbox;
    private JCheckBox forceRefreshCheckbox;
    private JLabel statusLabel;
    private JLabel existingInstallLabel;
    private JButton configureBtn;
    private JButton relaunchBtn;

    @SuppressWarnings("EmptyCatch")
    public void show() {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {
        }

        SwingUtilities.invokeLater(this::buildAndShow);
    }

    private void buildAndShow() {
        frame = new JFrame("AskAmerica MCP Setup");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setResizable(false);
        frame.setBackground(BG);
        frame.setIconImages(loadAppIcons());

        JPanel root = new JPanel(new BorderLayout());
        root.setBackground(BG);
        root.setBorder(BorderFactory.createEmptyBorder(32, 40, 32, 40));

        root.add(buildHeader(), BorderLayout.NORTH);
        root.add(buildForm(),   BorderLayout.CENTER);
        root.add(buildFooter(), BorderLayout.SOUTH);

        frame.setContentPane(root);
        frame.pack();
        frame.setMinimumSize(new Dimension(480, 400));

        // Center on screen
        Dimension screen = Toolkit.getDefaultToolkit().getScreenSize();
        frame.setLocation(
            (screen.width  - frame.getWidth())  / 2,
            (screen.height - frame.getHeight()) / 2);

        frame.setVisible(true);
    }

    // ── Header ────────────────────────────────────────────────────────────────

    private JPanel buildHeader() {
        JPanel p = new JPanel(new GridBagLayout());
        p.setBackground(BG);
        p.setBorder(BorderFactory.createEmptyBorder(0, 0, 24, 0));

        JLabel title = new JLabel("AskAmerica MCP");
        title.setFont(new Font("Serif", Font.ITALIC, 28));
        title.setForeground(AMBER);

        JLabel sub = new JLabel("Query US government data from Claude");
        sub.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 13));
        sub.setForeground(DIM);

        GridBagConstraints c = new GridBagConstraints();
        c.gridy = 0; c.anchor = GridBagConstraints.CENTER;
        p.add(title, c);
        c.gridy = 1; c.insets = new Insets(6, 0, 0, 0);
        p.add(sub, c);
        return p;
    }

    // ── Form ──────────────────────────────────────────────────────────────────

    private JPanel buildForm() {
        JPanel p = new JPanel(new GridBagLayout());
        p.setBackground(BG);

        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1.0;
        c.gridx = 0;
        c.insets = new Insets(0, 0, 8, 0);
        int row = 0;

        // Existing-install banner — only shown when this run actually found something
        // (a cached engine jar and/or a previously-written API key). A reinstall used to
        // look identical to a first run: a blank key field and no acknowledgement that
        // anything was already there, so it silently kept whatever was cached with no
        // way to tell. Detected once, up front, and surfaced before either the key field
        // or the repair checkbox below so both make sense in context.
        String existingKey = detectExistingApiKey();
        String existingJarVersion = detectExistingJarVersion();
        if (existingKey != null || existingJarVersion != null) {
            StringBuilder msg = new StringBuilder("Existing install found — ");
            if (existingJarVersion != null) {
                msg.append("engine ").append(existingJarVersion).append(existingKey != null ? ", " : "");
            }
            if (existingKey != null) {
                msg.append("API key on file");
            }
            msg.append(". Edit below to replace, or click Configure to keep as-is.");
            existingInstallLabel = new JLabel(
                "<html><div style='width:400px;'>" + escapeHtml(msg.toString()) + "</div></html>");
            existingInstallLabel.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 11));
            existingInstallLabel.setForeground(AMBER);
            c.gridy = row++; c.insets = new Insets(0, 0, 12, 0);
            p.add(existingInstallLabel, c);
        }

        // API key label + link
        JPanel keyHeader = new JPanel(new BorderLayout());
        keyHeader.setBackground(BG);
        JLabel keyLabel = label("API Key", false);
        JLabel getKey = link("Get a free key →", "https://askamerica.ai/#signup");
        keyHeader.add(keyLabel, BorderLayout.WEST);
        keyHeader.add(getKey,   BorderLayout.EAST);

        c.gridy = row++; c.insets = new Insets(0, 0, 8, 0);
        p.add(keyHeader, c);

        // API key field — prefilled with whatever is already configured, so a
        // reinstall shows what's there instead of pretending nothing is.
        apiKeyField = new JPasswordField();
        if (existingKey != null) {
            apiKeyField.setText(existingKey);
        }
        apiKeyField.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        apiKeyField.setBackground(CARD);
        apiKeyField.setForeground(TEXT);
        apiKeyField.setCaretColor(AMBER);
        apiKeyField.setBorder(
            BorderFactory.createCompoundBorder(
            BorderFactory.createLineBorder(BORDER),
            BorderFactory.createEmptyBorder(8, 10, 8, 10)));
        apiKeyField.setEchoChar((char) 0); // show text
        apiKeyField.setPreferredSize(new Dimension(400, 36));
        c.gridy = row++; c.insets = new Insets(0, 0, 16, 0);
        p.add(apiKeyField, c);

        // Telemetry opt-in checkbox
        telemetryCheckbox =
            new JCheckBox("Share anonymous usage telemetry to improve AskAmerica");
        telemetryCheckbox.setSelected(loadTelemetryOptIn());
        telemetryCheckbox.setBackground(BG);
        telemetryCheckbox.setForeground(DIM);
        telemetryCheckbox.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        telemetryCheckbox.setFocusPainted(false);
        c.gridy = row++; c.insets = new Insets(0, 0, existingJarVersion != null ? 4 : 16, 0);
        p.add(telemetryCheckbox, c);

        // Repair checkbox — only offered when there is actually a cached jar to force-
        // refresh. Unchecked by default: a plain reinstall should stay fast and not
        // redownload ~450MB just because the wizard ran again, but a corrupted or
        // suspect cache needs an explicit way to force a clean redownload rather than
        // silently keeping whatever is there.
        if (existingJarVersion != null) {
            forceRefreshCheckbox =
                new JCheckBox("Force a fresh engine download (repair install)");
            forceRefreshCheckbox.setSelected(false);
            forceRefreshCheckbox.setBackground(BG);
            forceRefreshCheckbox.setForeground(DIM);
            forceRefreshCheckbox.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
            forceRefreshCheckbox.setFocusPainted(false);
            c.gridy = row++; c.insets = new Insets(0, 0, 16, 0);
            p.add(forceRefreshCheckbox, c);
        }

        // Configure button — use BasicButtonUI so setBackground(AMBER) is respected
        // on macOS Aqua L&F, which otherwise paints buttons natively and ignores fill.
        configureBtn = new JButton("Configure Claude Desktop");
        configureBtn.setUI(new javax.swing.plaf.basic.BasicButtonUI());
        configureBtn.setBackground(AMBER);
        configureBtn.setForeground(new Color(0x0A0A0A));
        configureBtn.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 13));
        configureBtn.setOpaque(true);
        configureBtn.setFocusPainted(false);
        configureBtn.setBorderPainted(false);
        configureBtn.setPreferredSize(new Dimension(400, 40));
        configureBtn.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        configureBtn.addActionListener(e -> onConfigure());
        c.gridy = row++; c.insets = new Insets(0, 0, 12, 0);
        p.add(configureBtn, c);

        // Relaunch button — hidden until a successful Configure detects Claude Desktop
        // is actually running (see onConfigure/maybeOfferRelaunch). Never fires on its
        // own: quitting an app the user may have open mid-conversation is disruptive
        // enough that it must always be an explicit, separate click, never a side
        // effect of Configure itself.
        relaunchBtn = new JButton("Quit && Relaunch Claude Desktop Now");
        relaunchBtn.setUI(new javax.swing.plaf.basic.BasicButtonUI());
        relaunchBtn.setBackground(CARD);
        relaunchBtn.setForeground(TEXT);
        relaunchBtn.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        relaunchBtn.setOpaque(true);
        relaunchBtn.setFocusPainted(false);
        relaunchBtn.setBorder(BorderFactory.createLineBorder(BORDER));
        relaunchBtn.setPreferredSize(new Dimension(400, 32));
        relaunchBtn.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        relaunchBtn.addActionListener(e -> onRelaunch());
        relaunchBtn.setVisible(false);
        c.gridy = row++; c.insets = new Insets(0, 0, 12, 0);
        p.add(relaunchBtn, c);

        // Status label
        statusLabel = new JLabel(" ");
        statusLabel.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        statusLabel.setForeground(DIM);
        statusLabel.setHorizontalAlignment(SwingConstants.CENTER);
        c.gridy = row++; c.insets = new Insets(0, 0, 0, 0);
        p.add(statusLabel, c);

        return p;
    }

    // ── Footer ────────────────────────────────────────────────────────────────

    private JPanel buildFooter() {
        JPanel p = new JPanel(new BorderLayout());
        p.setBackground(BG);
        p.setBorder(BorderFactory.createEmptyBorder(24, 0, 0, 0));

        JTextArea desc =
            new JTextArea("After configuring, restart Claude Desktop. "
            + "AskAmerica will appear as a connected tool.\n\n"
            + "Tools: list_schemas · list_tables · describe_table · query");
        desc.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        desc.setForeground(DIM);
        desc.setBackground(BG);
        desc.setEditable(false);
        desc.setWrapStyleWord(true);
        desc.setLineWrap(true);
        desc.setBorder(BorderFactory.createEmptyBorder(12, 12, 12, 12));

        JPanel card = new JPanel(new BorderLayout());
        card.setBackground(CARD);
        card.setBorder(BorderFactory.createLineBorder(BORDER));
        card.add(desc);
        p.add(card, BorderLayout.CENTER);
        return p;
    }

    // ── Action ────────────────────────────────────────────────────────────────

    private void onConfigure() {
        String apiKey = new String(apiKeyField.getPassword()).trim();
        if (apiKey.isEmpty()) {
            setStatus("Please enter your API key.", false);
            return;
        }

        configureBtn.setEnabled(false);
        setStatus("Writing configuration…", null);

        try {
            List<Path> written = claudeConfigPaths();
            for (Path configPath : written) {
                writeClaudeConfig(configPath, apiKey);
            }
            saveTelemetryOptIn(telemetryCheckbox.isSelected());

            String repairNote = "";
            if (forceRefreshCheckbox != null && forceRefreshCheckbox.isSelected()) {
                repairNote = forceEngineRefresh()
                    ? " Cached engine cleared — it will redownload on next launch."
                    : " (No cached engine found to clear.)";
            }

            // Name the count, and the paths on hover: a bare "Done!" was reported after
            // writing a config file Claude Desktop does not read, which is unfalsifiable
            // from the wizard and looks identical to success.
            setStatus("Done! Updated " + written.size()
                + (written.size() == 1 ? " config file. " : " config files. ")
                + "Restart Claude Desktop to activate." + repairNote + " Tip: try \"What "
                + "can I do with AskAmerica?\", \"What questions can AskAmerica answer?\", "
                + "or \"Pose a sample question to AskAmerica\" in a new chat any time to "
                + "see what it can do.", true);
            statusLabel.setToolTipText(written.toString());
            configureBtn.setText("Configure again");
            maybeOfferRelaunch();
        } catch (Exception ex) {
            setStatus("Error: " + ex.getMessage(), false);
        } finally {
            configureBtn.setEnabled(true);
        }
    }

    /**
     * Shows the relaunch button only when Claude Desktop is actually running right now —
     * offering it unconditionally would be a dead click most of the time (Desktop isn't
     * open, or this is a genuine first install with nothing to restart) and would imply
     * an action is needed when it isn't.
     */
    private void maybeOfferRelaunch() {
        if (isClaudeDesktopRunning()) {
            relaunchBtn.setVisible(true);
            frame.pack();
        }
    }

    private void onRelaunch() {
        relaunchBtn.setEnabled(false);
        setStatus("Quitting Claude Desktop…", null);
        try {
            quitClaudeDesktop();
            // Graceful quit is async — Desktop decides its own shutdown pace (it may have
            // its own confirmation dialogs). Poll rather than assume a fixed delay, but
            // don't wait forever: relaunching over a still-shutting-down instance is
            // relatively harmless (the OS will just focus the existing window), so a
            // capped wait is the right tradeoff over blocking the UI indefinitely.
            long deadline = System.currentTimeMillis() + 8000;
            while (isClaudeDesktopRunning() && System.currentTimeMillis() < deadline) {
                Thread.sleep(300);
            }
            setStatus("Relaunching Claude Desktop…", null);
            relaunchClaudeDesktop();
            setStatus("Done! Claude Desktop is restarting with the new configuration.", true);
            relaunchBtn.setVisible(false);
            frame.pack();
        } catch (Exception ex) {
            setStatus("Couldn't relaunch automatically (" + ex.getMessage()
                + "). Please quit and reopen Claude Desktop yourself.", false);
            relaunchBtn.setEnabled(true);
        }
    }

    // ── Telemetry helpers ─────────────────────────────────────────────────────

    private static boolean loadTelemetryOptIn() {
        try {
            java.io.File f =
                new java.io.File(System.getProperty("user.home"), ".askamerica/telemetry.json");
            if (!f.exists()) {
                return true;
            }
            return MAPPER.readTree(f).path("optIn").asBoolean(true);
        } catch (Exception e) {
            return true;
        }
    }

    private static void saveTelemetryOptIn(boolean optIn) throws IOException {
        java.io.File dir =
            new java.io.File(System.getProperty("user.home"), ".askamerica");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        java.io.File f = new java.io.File(dir, "telemetry.json");
        MAPPER.writeValue(f, Collections.singletonMap("optIn", optIn));
    }

    // ── Config writer ─────────────────────────────────────────────────────────

    private void writeClaudeConfig(Path configPath, String apiKey) throws IOException {
        ObjectNode root;
        if (Files.exists(configPath)) {
            root = (ObjectNode) MAPPER.readTree(configPath.toFile());
        } else {
            root = MAPPER.createObjectNode();
        }

        ObjectNode mcpServers = (ObjectNode) root.get("mcpServers");
        if (mcpServers == null) {
            mcpServers = MAPPER.createObjectNode();
            root.set("mcpServers", mcpServers);
        }

        ObjectNode entry = MAPPER.createObjectNode();
        entry.put("command", launcherCommand());

        // --mcp flag tells the binary to run in server mode (skip the setup UI)
        entry.putArray("args").add("--mcp");

        ObjectNode env = MAPPER.createObjectNode();
        env.put("ASKAMERICA_API_KEY", apiKey);
        entry.set("env", env);

        mcpServers.set("askamerica", entry);

        Files.createDirectories(configPath.getParent());
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(configPath.toFile(), root);
    }

    // ── Existing-install detection ───────────────────────────────────────────

    /**
     * The API key already written into Claude Desktop's config, if any — read from the
     * first config path where it's found. Best-effort: no config file, no
     * {@code mcpServers.askamerica} entry, or a read error all mean "nothing found",
     * which is exactly the right answer for a genuine first install, not an error to
     * surface.
     */
    private static String detectExistingApiKey() {
        try {
            for (Path configPath : claudeConfigPaths()) {
                if (!Files.exists(configPath)) {
                    continue;
                }
                com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(configPath.toFile());
                com.fasterxml.jackson.databind.JsonNode key = root
                    .path("mcpServers").path("askamerica").path("env").path("ASKAMERICA_API_KEY");
                if (key.isTextual() && !key.asText().isBlank()) {
                    return key.asText();
                }
            }
        } catch (Exception ignored) {
            // Best-effort — see javadoc.
        }
        return null;
    }

    /**
     * The shared engine cache path — deliberately NOT {@link EngineInstaller#cacheJar()}.
     *
     * <p>Measured live 2026-09-23: {@code McpServerLauncher} calls {@code EngineInstaller
     * .ensure()} directly, so {@code EngineInstaller} loads via the JVM's normal system
     * classloader before {@code SetupWindow} is ever touched. {@code SetupWindow} itself is
     * loaded afterward through a separate {@code URLClassLoader} pointed at the resolved fat
     * jar (see {@code McpServerLauncher}'s reflective {@code Class.forName}). Those are two
     * different classloaders, so — despite being "the same" class by name and package — a
     * call from {@code SetupWindow} into {@code EngineInstaller}'s package-private statics
     * throws {@code IllegalAccessError} across that boundary. It surfaces on the EDT, which
     * kills {@code buildForm()} before any window is ever shown: the app appears in the Dock,
     * shows nothing, and macOS eventually terminates the windowless, unresponsive process —
     * exactly what "ran the pkg, no window, then it silently exits" was.
     *
     * <p>Fix: never reference {@code EngineInstaller} from this class. The two pieces of
     * logic actually needed (the cache path, and reading a jar's stamped version) are small
     * enough to duplicate locally rather than share across a classloader boundary that can't
     * safely carry a direct call.
     */
    private static Path cachedEngineJar() {
        return Paths.get(System.getProperty("user.home"), ".askamerica", "engine",
            "askamerica-engine.jar");
    }

    /** See {@link #cachedEngineJar()} for why this doesn't call {@code EngineInstaller}. */
    private static String jarVersion(Path jar) throws IOException {
        try (java.util.jar.JarFile jf = new java.util.jar.JarFile(jar.toFile())) {
            java.util.jar.Manifest mf = jf.getManifest();
            if (mf == null) {
                return null;
            }
            return mf.getMainAttributes().getValue("AskAmerica-Engine-Version");
        }
    }

    /**
     * The version of the currently cached engine jar, or null if nothing is cached yet.
     */
    private static String detectExistingJarVersion() {
        try {
            Path cached = cachedEngineJar();
            if (!Files.exists(cached)) {
                return null;
            }
            String v = jarVersion(cached);
            return v == null ? "unknown version" : "v" + v;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Deletes the cached engine jar (and its cross-process lock file, if present) so the
     * next launch's {@code EngineInstaller.ensure} downloads a fresh copy regardless of its
     * own version-staleness check. This is the explicit "repair install" path — it must
     * never run implicitly just because the wizard was opened again, or reinstalling would
     * silently redownload the engine on every run.
     *
     * @return true if a cached jar was actually found and removed
     */
    private static boolean forceEngineRefresh() throws IOException {
        Path cached = cachedEngineJar();
        boolean existed = Files.deleteIfExists(cached);
        Files.deleteIfExists(cached.resolveSibling(cached.getFileName() + ".lock"));
        return existed;
    }

    // ── Claude Desktop process management ────────────────────────────────────

    /**
     * Best-effort, platform-specific "is the app running" check. False on any failure —
     * an unrecognized platform or a failed process probe should hide the relaunch button
     * (nothing to offer), never crash the wizard over it.
     */
    private static boolean isClaudeDesktopRunning() {
        String os = System.getProperty("os.name", "").toLowerCase();
        try {
            if (os.contains("win")) {
                Process p = new ProcessBuilder(
                    "tasklist", "/FI", "IMAGENAME eq Claude.exe").start();
                String out = new String(p.getInputStream().readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8);
                p.waitFor();
                return out.toLowerCase().contains("claude.exe");
            }
            // macOS and Linux Electron builds both register the process name "Claude".
            Process p = new ProcessBuilder("pgrep", "-x", "Claude").start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Asks Claude Desktop to quit — a graceful, OS-level request the app can act on
     * normally (save state, decline via a dialog), never a forced kill. A forced kill
     * (SIGKILL / {@code taskkill /F}) risks losing session state for the sake of a config
     * change that will apply just as well on the next ordinary restart.
     */
    private static void quitClaudeDesktop() throws IOException, InterruptedException {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("mac")) {
            new ProcessBuilder("osascript", "-e", "quit app \"Claude\"").start().waitFor();
        } else if (os.contains("win")) {
            // No /F: a plain taskkill sends WM_CLOSE, giving the app the same chance to
            // shut down cleanly that closing its window would.
            new ProcessBuilder("taskkill", "/IM", "Claude.exe").start().waitFor();
        } else {
            // Plain pkill sends SIGTERM, not SIGKILL — same "ask nicely" intent.
            new ProcessBuilder("pkill", "-x", "Claude").start().waitFor();
        }
    }

    /**
     * Relaunches Claude Desktop after a quit. Best-effort per platform; a failure here is
     * reported to the user as "please reopen it yourself" rather than treated as fatal —
     * the config change itself already succeeded regardless of whether this step works.
     */
    private static void relaunchClaudeDesktop() throws IOException {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("mac")) {
            new ProcessBuilder("open", "-a", "Claude").start();
        } else if (os.contains("win")) {
            String localAppData = System.getenv("LOCALAPPDATA");
            Path exe = localAppData == null ? null
                : Paths.get(localAppData, "Programs", "Claude", "Claude.exe");
            if (exe != null && Files.exists(exe)) {
                new ProcessBuilder(exe.toString()).start();
            } else {
                // Falls through to whatever "Claude" resolves to on PATH/shell association;
                // may not resolve on every install layout, which is why the caller reports
                // failure back to the user rather than assuming success.
                new ProcessBuilder("cmd", "/c", "start", "", "Claude").start();
            }
        } else {
            new ProcessBuilder("claude-desktop").start();
        }
    }

    /**
     * Every Claude Desktop config file that should receive the mcpServers entry.
     *
     * <p>On Windows there are two install flavours and they do not share a config
     * file:
     *
     * <ul>
     *   <li>the standalone build reads {@code %APPDATA%\Claude};</li>
     *   <li>the MSIX / Microsoft Store build runs inside an app container, where
     *       {@code %APPDATA%} is virtualised to
     *       {@code %LOCALAPPDATA%\Packages\Claude_*\LocalCache\Roaming\Claude}.
     *       This wizard runs outside that container, so its own {@code %APPDATA%}
     *       resolves to the un-redirected path: writing there creates a file the
     *       packaged app never reads, and the connector silently never appears.</li>
     * </ul>
     *
     * <p>So probe for the config directories that actually exist instead of assuming
     * a layout, and write to each one. If none exist, Claude Desktop has never been
     * launched (or isn't installed) and there is no correct file to write — say so
     * rather than guessing a path and then reporting success.
     */
    private static List<Path> claudeConfigPaths() throws IOException {
        String os = System.getProperty("os.name", "").toLowerCase();
        List<Path> candidates = new ArrayList<>();
        // Container-redirected dirs, kept apart from `candidates`: their package root
        // proves the app is installed, so they are written whether or not the leaf exists.
        List<Path> containerDirs = new ArrayList<>();
        if (os.contains("mac")) {
            candidates.add(Paths.get(System.getProperty("user.home"),
                "Library", "Application Support", "Claude"));
        } else if (os.contains("win")) {
            String localAppData = System.getenv("LOCALAPPDATA");
            if (localAppData != null) {
                Path packages = Paths.get(localAppData, "Packages");
                if (Files.isDirectory(packages)) {
                    DirectoryStream<Path> pkgs =
                        Files.newDirectoryStream(packages, "Claude_*");
                    try {
                        for (Path pkg : pkgs) {
                            containerDirs.add(pkg.resolve("LocalCache")
                                .resolve("Roaming").resolve("Claude"));
                        }
                    } finally {
                        pkgs.close();
                    }
                }
            }
            String appData = System.getenv("APPDATA");
            if (appData != null) {
                candidates.add(Paths.get(appData, "Claude"));
            }
        } else {
            candidates.add(
                Paths.get(System.getProperty("user.home"), ".config", "Claude"));
        }

        List<Path> configs = new ArrayList<>();
        for (Path dir : containerDirs) {
            // Claude Desktop creates this leaf lazily, on its first config write. Gating it
            // on Files.isDirectory dropped it silently whenever it was absent, leaving
            // %APPDATA%\Claude as the only write target — a file the containerised app never
            // reads — while the wizard still reported success. Create it instead.
            Files.createDirectories(dir);
            configs.add(dir.resolve("claude_desktop_config.json"));
        }
        for (Path dir : candidates) {
            if (Files.isDirectory(dir)) {
                configs.add(dir.resolve("claude_desktop_config.json"));
            }
        }
        if (configs.isEmpty()) {
            List<Path> looked = new ArrayList<>(containerDirs);
            looked.addAll(candidates);
            throw new IOException("Claude Desktop's config folder was not found (looked in "
                + looked + "). Install Claude Desktop and launch it once, "
                + "then click Configure again.");
        }
        return configs;
    }

    /**
     * The command Claude Desktop should spawn for the MCP server.
     *
     * <p>The jpackage launcher lives at a path containing spaces — the app is
     * "AskAmerica MCP.app" and its executable is ".../Contents/MacOS/AskAmerica MCP"
     * (Linux: "/opt/askamerica-mcp/bin/AskAmerica MCP"). Claude Desktop resolves the
     * mcpServers {@code command} against PATH and word-splits on the space, so it tries
     * to exec "/Applications/AskAmerica" and fails with "No such file or directory".
     *
     * <p>To avoid this, install a small space-free shim at
     * {@code ~/.askamerica/bin/askamerica-mcp} that {@code exec}s the real launcher,
     * and point Claude at the shim. Re-running the wizard rewrites the shim, so a
     * moved/reinstalled app is picked up automatically.
     *
     * <p>Windows is exempt: CreateProcess handles the quoted, spaced {@code .exe} path
     * directly, and a {@code .cmd} shim would itself need a shell to be spawned.
     */
    private static String launcherCommand() throws IOException {
        String launcher = executablePath();
        if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
            return launcher;
        }
        Path binDir =
            Paths.get(System.getProperty("user.home"), ".askamerica", "bin");
        Files.createDirectories(binDir);
        Path shim = binDir.resolve("askamerica-mcp");
        String script = "#!/bin/sh\n"
            + "# Auto-generated by AskAmerica MCP setup. Execs the bundled launcher via\n"
            + "# a space-free path so Claude Desktop can spawn it. Safe to regenerate.\n"
            + "exec \"" + launcher + "\" \"$@\"\n";
        Files.writeString(shim, script);
        shim.toFile().setExecutable(true, false);
        return shim.toString();
    }

    private static String executablePath() {
        // The launcher records the exact executable it was started from — use it so
        // Claude Desktop is pointed at the real binary regardless of install location.
        String actual = System.getProperty("askamerica.launcher.command");
        if (actual != null && !actual.isBlank()) {
            return actual;
        }
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("mac")) {
            return "/Applications/AskAmerica MCP.app/Contents/MacOS/AskAmerica MCP";
        } else if (os.contains("win")) {
            return "C:\\Program Files\\AskAmerica MCP\\AskAmerica MCP.exe";
        } else {
            return "/opt/askamerica-mcp/bin/AskAmerica MCP";
        }
    }

    // ── UI helpers ────────────────────────────────────────────────────────────

    /**
     * A plain {@code JFrame} shows Java's generic default icon (the coffee cup) in its title
     * bar, taskbar entry, and Alt-Tab switcher unless {@code setIconImage(s)} is called
     * explicitly — jpackage's {@code --icon} only covers the packaged EXE/shortcuts/Add-Remove-
     * Programs entry, not a running Swing window's own icon. Multiple sizes (not just one) so
     * Windows can pick the sharpest one for each context instead of scaling a single size up
     * or down.
     */
    private static List<Image> loadAppIcons() {
        List<Image> icons = new ArrayList<>();
        for (int size : new int[]{16, 32, 48, 64, 128, 256}) {
            try (InputStream in = SetupWindow.class.getResourceAsStream(
                    "/icons/askamerica-" + size + ".png")) {
                if (in != null) {
                    icons.add(javax.imageio.ImageIO.read(in));
                }
            } catch (IOException ignored) {
                // Missing/unreadable icon resource must never block the setup window itself
                // from opening — worst case, this one size is absent from the list.
            }
        }
        return icons;
    }

    private static JLabel label(String text, boolean dim) {
        JLabel l = new JLabel(text);
        l.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        l.setForeground(dim ? DIM : TEXT);
        return l;
    }

    private static JLabel link(String text, String url) {
        JLabel l = new JLabel("<html><a href=''>" + text + "</a></html>");
        l.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        l.setForeground(AMBER);
        l.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        l.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            @SuppressWarnings("EmptyCatch")
            public void mouseClicked(java.awt.event.MouseEvent e) {
                try {
                    Desktop.getDesktop().browse(new URI(url));
                } catch (Exception ignored) {
                }
            }
        });
        return l;
    }

    /**
     * A plain JLabel never wraps — it truncates instead, which is how the completion message
     * ("Done! ... Restart Claude Desktop to activate. Tip: ...") was reported clipped on
     * Windows. HTML content in a JLabel DOES wrap, but only within an explicit pixel width —
     * without one, Swing sizes the label to fit the text on a single line regardless, same as
     * plain text. 400px matches the API key field / configure button width above.
     */
    private static String escapeHtml(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private void setStatus(String msg, Boolean success) {
        statusLabel.setText("<html><div style='width:400px;text-align:center;'>"
            + escapeHtml(msg) + "</div></html>");
        if (Boolean.TRUE.equals(success)) {
            statusLabel.setForeground(new Color(0x28C840));
        } else if (Boolean.FALSE.equals(success)) {
            statusLabel.setForeground(new Color(0xFF5F57));
        } else {
            statusLabel.setForeground(DIM);
        }
        // The window is packed once at construction, sized for the initial single-line " "
        // placeholder — a wrapped, multi-line status (the completion message routinely runs
        // to 3 lines at the 400px wrap width above) would otherwise be clipped at the bottom
        // of that fixed, non-resizable frame instead of growing to fit. setResizable(false)
        // only blocks the user dragging a resize handle; it does not block a programmatic
        // pack().
        frame.pack();
    }
}
