/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.calcite.adapter.askamerica;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Thin launcher entry point for the jpackage app bundle.
 *
 * The PKG installer is small — it bundles only the JRE and this launcher.
 * The postinstall script downloads askamerica-engine.jar from GitHub Releases
 * and places it alongside this launcher in Contents/app/.
 *
 * Enterprise deployments can set ASKAMERICA_ENGINE_URL to a private mirror.
 */
public class McpServerLauncher {
    public static void main(String[] args) throws Exception {
        File launcherJar = new File(
            McpServerLauncher.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI());
        File engineJar = new File(launcherJar.getParentFile(), "askamerica-engine.jar");
        if (!engineJar.exists()) {
            System.err.println("AskAmerica engine not found: " + engineJar.getAbsolutePath());
            System.err.println("Reinstall AskAmerica MCP to trigger engine download.");
            System.exit(1);
        }
        URLClassLoader loader = new URLClassLoader(
            new URL[]{engineJar.toURI().toURL()},
            Thread.currentThread().getContextClassLoader());
        Class<?> cls = Class.forName(
            "org.apache.calcite.adapter.askamerica.McpServer", true, loader);
        Method main = cls.getMethod("main", String[].class);
        main.invoke(null, (Object) args);
    }
}
