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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collections;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Cursor;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private JLabel statusLabel;
    private JButton configureBtn;

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

        // API key label + link
        JPanel keyHeader = new JPanel(new BorderLayout());
        keyHeader.setBackground(BG);
        JLabel keyLabel = label("API Key", false);
        JLabel getKey = link("Get a free key →", "https://askamerica.ai/#signup");
        keyHeader.add(keyLabel, BorderLayout.WEST);
        keyHeader.add(getKey,   BorderLayout.EAST);

        c.gridy = 0;
        p.add(keyHeader, c);

        // API key field
        apiKeyField = new JPasswordField();
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
        c.gridy = 1; c.insets = new Insets(0, 0, 16, 0);
        p.add(apiKeyField, c);

        // Telemetry opt-in checkbox
        telemetryCheckbox = new JCheckBox(
            "Share anonymous usage telemetry to improve AskAmerica");
        telemetryCheckbox.setSelected(loadTelemetryOptIn());
        telemetryCheckbox.setBackground(BG);
        telemetryCheckbox.setForeground(DIM);
        telemetryCheckbox.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        telemetryCheckbox.setFocusPainted(false);
        c.gridy = 2; c.insets = new Insets(0, 0, 16, 0);
        p.add(telemetryCheckbox, c);

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
        c.gridy = 3; c.insets = new Insets(0, 0, 12, 0);
        p.add(configureBtn, c);

        // Status label
        statusLabel = new JLabel(" ");
        statusLabel.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        statusLabel.setForeground(DIM);
        statusLabel.setHorizontalAlignment(SwingConstants.CENTER);
        c.gridy = 4; c.insets = new Insets(0, 0, 0, 0);
        p.add(statusLabel, c);

        // Config preview (collapsible — shown after success)
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
            Path configPath = claudeConfigPath();
            writeClaudeConfig(configPath, apiKey);
            saveTelemetryOptIn(telemetryCheckbox.isSelected());
            setStatus("Done! Restart Claude Desktop to activate.", true);
            configureBtn.setText("Configure again");
        } catch (Exception ex) {
            setStatus("Error: " + ex.getMessage(), false);
        } finally {
            configureBtn.setEnabled(true);
        }
    }

    // ── Telemetry helpers ─────────────────────────────────────────────────────

    private static boolean loadTelemetryOptIn() {
        try {
            java.io.File f = new java.io.File(
                System.getProperty("user.home"), ".askamerica/telemetry.json");
            if (!f.exists()) {
                return true;
            }
            return MAPPER.readTree(f).path("optIn").asBoolean(true);
        } catch (Exception e) {
            return true;
        }
    }

    private static void saveTelemetryOptIn(boolean optIn) throws IOException {
        java.io.File dir = new java.io.File(
            System.getProperty("user.home"), ".askamerica");
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
        entry.put("command", executablePath());

        // --mcp flag tells the binary to run in server mode (skip the setup UI)
        entry.putArray("args").add("--mcp");

        ObjectNode env = MAPPER.createObjectNode();
        env.put("ASKAMERICA_API_KEY", apiKey);
        entry.set("env", env);

        mcpServers.set("askamerica", entry);

        Files.createDirectories(configPath.getParent());
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(configPath.toFile(), root);
    }

    private static Path claudeConfigPath() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("mac")) {
            return Paths.get(System.getProperty("user.home"),
                "Library", "Application Support", "Claude", "claude_desktop_config.json");
        } else if (os.contains("win")) {
            String appData = System.getenv("APPDATA");
            if (appData == null) {
                appData = System.getProperty("user.home");
            }
            return Paths.get(appData, "Claude", "claude_desktop_config.json");
        } else {
            return Paths.get(System.getProperty("user.home"),
                ".config", "Claude", "claude_desktop_config.json");
        }
    }

    private static String executablePath() {
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
            public void mouseClicked(java.awt.event.MouseEvent e) {
                try {
                    Desktop.getDesktop().browse(new URI(url));
                } catch (Exception ignored) {
                }
            }
        });
        return l;
    }

    private void setStatus(String msg, Boolean success) {
        statusLabel.setText(msg);
        if (Boolean.TRUE.equals(success)) {
            statusLabel.setForeground(new Color(0x28C840));
        } else if (Boolean.FALSE.equals(success)) {
            statusLabel.setForeground(new Color(0xFF5F57));
        } else {
            statusLabel.setForeground(DIM);
        }
    }
}
