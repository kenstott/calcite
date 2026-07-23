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

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.ServiceLoader;

/**
 * Thin launcher entry point for the jpackage app bundle.
 *
 * The installer is small — it bundles only the JRE and this launcher. The fat
 * askamerica-engine.jar is resolved (and, on first run, downloaded) at startup by
 * {@link EngineInstaller} into the shared cache ~/.askamerica/engine/, so the same
 * copy is shared with the pip client and no per-platform postinstall hook is needed.
 *
 * --mcp flag (passed by Claude Desktop via mcpServers config): run as MCP server.
 * No flag (launched from dock/Start menu/Applications): show the first-run wizard.
 *
 * Enterprise deployments can set ASKAMERICA_ENGINE_URL to a private mirror, or
 * ASKAMERICA_ENGINE_JAR to an already-present jar.
 */
public class McpServerLauncher {
    @SuppressWarnings("EmptyCatch")
    public static void main(String[] args) throws Exception {
        File launcherJar =
            new File(McpServerLauncher.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI());

        boolean mcpMode = Arrays.asList(args).contains("--mcp");

        // Record the actual launcher executable so the setup wizard can point
        // Claude Desktop at the right binary, whatever the install location.
        ProcessHandle.current().info().command()
            .ifPresent(cmd -> System.setProperty("askamerica.launcher.command", cmd));

        // Resolve — downloading if necessary — the fat engine jar. In server mode
        // this is a headless (stderr) download; interactively it shows a progress
        // window. Normally the wizard has already cached it, so this is a no-op.
        Path engineJarPath;
        try {
            engineJarPath = EngineInstaller.ensure(launcherJar.getParentFile(), mcpMode);
        } catch (Exception e) {
            System.err.println("Could not obtain the AskAmerica engine: " + e.getMessage());
            System.err.println("Check your network connection and try again, or set "
                + "ASKAMERICA_ENGINE_JAR to a local askamerica-engine.jar.");
            System.exit(1);
            return;
        }
        File engineJar = engineJarPath.toFile();
        URLClassLoader loader =
            new URLClassLoader(new URL[]{engineJar.toURI().toURL()},
            Thread.currentThread().getContextClassLoader());

        // Java 9+ DriverManager uses the system classloader for ServiceLoader, so JDBC
        // drivers bundled in the engine JAR (DuckDB, etc.) are never auto-registered.
        // Load them explicitly here so DriverManager.getConnection() can find them.
        for (Driver driver : ServiceLoader.load(Driver.class, loader)) {
            try {
                DriverManager.registerDriver(driver);
            } catch (SQLException ignored) {
            }
        }

        if (mcpMode) {
            Class<?> cls =
                Class.forName("org.apache.calcite.adapter.askamerica.McpServer", true, loader);
            Method main = cls.getMethod("main", String[].class);
            main.invoke(null, (Object) args);
        } else {
            // Launched from dock or Applications — show setup wizard.
            // SetupWindow uses EXIT_ON_CLOSE so the AWT EDT keeps the JVM alive.
            Class<?> cls =
                Class.forName("org.apache.calcite.adapter.askamerica.SetupWindow", true, loader);
            Object instance = cls.getDeclaredConstructor().newInstance();
            Method show = cls.getMethod("show");
            show.invoke(instance);
        }
    }
}
