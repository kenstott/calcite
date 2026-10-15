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

import java.util.Locale;

/**
 * The operating-system variant name used by the release assets (the per-OS engine jars and the
 * pgwire-govdata bundles). Its own class, with no other dependencies, because the thin launcher
 * jar (see {@code launcherJar} in build.gradle.kts) carries only {@code McpServerLauncher},
 * {@code EngineInstaller} and this class: a reference from the launcher to any other class fails
 * at startup with NoClassDefFoundError, before any window opens.
 */
final class PlatformVariant {

  private PlatformVariant() {
  }

  static String osVariant() {
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
}
