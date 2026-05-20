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
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.ServiceLoader;

/**
 * Thin launcher entry point for the jpackage app bundle.
 *
 * The PKG installer is small — it bundles only the JRE and this launcher.
 * The postinstall script downloads askamerica-engine.jar from GitHub Releases
 * and places it alongside this launcher in Contents/app/.
 *
 * --mcp flag (passed by Claude Desktop via mcpServers config): run as MCP server.
 * No flag (launched from dock/Applications): show the first-run setup wizard.
 *
 * Enterprise deployments can set ASKAMERICA_ENGINE_URL to a private mirror.
 */
public class McpServerLauncher {
    public static void main(String[] args) throws Exception {
        File launcherJar =
            new File(McpServerLauncher.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI());
        File engineJar = new File(launcherJar.getParentFile(), "askamerica-engine.jar");
        if (!engineJar.exists()) {
            System.err.println("AskAmerica engine not found: " + engineJar.getAbsolutePath());
            System.err.println("Reinstall AskAmerica MCP to trigger engine download.");
            System.exit(1);
        }
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

        boolean mcpMode = Arrays.asList(args).contains("--mcp");

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
