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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The installed app starts from the thin launcher jar, which holds only the classes listed in the
 * {@code launcherJar} task of build.gradle.kts. A launcher class that references any other class
 * died at startup with NoClassDefFoundError before a window opened (0.112.0 - 0.113.0:
 * EngineInstaller.platformAsset used PgwireGovDataInstaller). This loads the launcher classes
 * through a loader that sees only that include list and runs the code paths that need no network.
 */
@Tag("unit")
class LauncherJarClosureTest {

  private static final String PACKAGE = "org.apache.calcite.adapter.askamerica.";

  /** The class-name patterns of the launcherJar task, read from build.gradle.kts itself. */
  private static List<Pattern> launcherIncludes() throws IOException {
    Path build = Paths.get("build.gradle.kts");
    String text = new String(Files.readAllBytes(build), java.nio.charset.StandardCharsets.UTF_8);
    int start = text.indexOf("val launcherJar by tasks.registering");
    assertTrue(start >= 0, "launcherJar task not found in build.gradle.kts");
    int end = text.indexOf("manifest {", start);
    Matcher m = Pattern.compile("include\\(\"\\*\\*/([^\"]+)\\.class\"\\)")
        .matcher(text.substring(start, end));
    List<Pattern> patterns = new ArrayList<Pattern>();
    while (m.find()) {
      String glob = m.group(1).replace("\\$", "$");
      patterns.add(Pattern.compile(Pattern.quote(glob).replace("*", "\\E.*\\Q")));
    }
    assertFalse(patterns.isEmpty(), "no class includes found for launcherJar");
    return patterns;
  }

  /** Sees an askamerica class only when the launcher jar would carry it. */
  private static final class LauncherOnlyLoader extends ClassLoader {
    private final List<Pattern> includes;

    LauncherOnlyLoader(List<Pattern> includes) {
      super(LauncherJarClosureTest.class.getClassLoader().getParent());
      this.includes = includes;
    }

    @Override protected Class<?> loadClass(String name, boolean resolve)
        throws ClassNotFoundException {
      if (!name.startsWith(PACKAGE)) {
        return super.loadClass(name, resolve);
      }
      String simple = name.substring(PACKAGE.length());
      boolean carried = false;
      for (Pattern p : includes) {
        if (p.matcher(simple).matches()) {
          carried = true;
          break;
        }
      }
      if (!carried) {
        throw new ClassNotFoundException(name + " is not in the launcher jar");
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> c = findLoadedClass(name);
        if (c == null) {
          String resource = name.replace('.', '/') + ".class";
          try (InputStream in =
                   LauncherJarClosureTest.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
              throw new ClassNotFoundException(name);
            }
            byte[] bytes = readAll(in);
            c = defineClass(name, bytes, 0, bytes.length);
          } catch (IOException e) {
            throw new ClassNotFoundException(name, e);
          }
        }
        return c;
      }
    }

    private static byte[] readAll(InputStream in) throws IOException {
      java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) > 0) {
        out.write(buf, 0, n);
      }
      return out.toByteArray();
    }
  }

  @Test void theLauncherResolvesItsEngineJarUsingOnlyTheClassesTheLauncherJarCarries()
      throws Exception {
    ClassLoader loader = new LauncherOnlyLoader(launcherIncludes());
    Class<?> installer = Class.forName(PACKAGE + "EngineInstaller", true, loader);
    java.lang.reflect.Method asset = installer.getDeclaredMethod("platformAsset");
    asset.setAccessible(true);
    String name = (String) asset.invoke(null);
    assertTrue(name.startsWith("askamerica-engine-") && name.endsWith(".jar"), name);
    java.lang.reflect.Method url = installer.getDeclaredMethod("defaultUrl");
    url.setAccessible(true);
    assertTrue(((String) url.invoke(null)).endsWith("/" + name));
    Class.forName(PACKAGE + "McpServerLauncher", true, loader);
  }

  @Test void aClassTheLauncherJarDoesNotCarryIsReallyHiddenFromTheLoader() throws Exception {
    ClassLoader loader = new LauncherOnlyLoader(launcherIncludes());
    try {
      Class.forName(PACKAGE + "PgwireGovDataInstaller", true, loader);
      throw new AssertionError("the loader let a class outside the launcher jar through");
    } catch (ClassNotFoundException expected) {
      assertTrue(expected.getMessage().contains("not in the launcher jar"));
    }
  }

  /**
   * The engine jar's classes run in a URLClassLoader whose parent is the launcher's loader, which
   * delegates parent-first: a class both jars carry resolves from the launcher. Package-private
   * access across the two then fails (IllegalAccessError), so what the engine classes use from
   * the launcher jar must be public.
   */
  @Test void theEngineLoaderCanUseTheClassesItSharesWithTheLauncherJar() throws Exception {
    final List<Pattern> includes = launcherIncludes();
    final ClassLoader launcher = new LauncherOnlyLoader(includes);
    ClassLoader engine = new ClassLoader(launcher) {
      @Override protected Class<?> loadClass(String name, boolean resolve)
          throws ClassNotFoundException {
        if (name.startsWith(PACKAGE)) {
          try {
            return launcher.loadClass(name);
          } catch (ClassNotFoundException notInLauncher) {
            synchronized (getClassLoadingLock(name)) {
              Class<?> c = findLoadedClass(name);
              if (c == null) {
                try (InputStream in = LauncherJarClosureTest.class.getClassLoader()
                    .getResourceAsStream(name.replace('.', '/') + ".class")) {
                  if (in == null) {
                    throw new ClassNotFoundException(name);
                  }
                  byte[] bytes = LauncherOnlyLoader.readAll(in);
                  c = defineClass(name, bytes, 0, bytes.length);
                } catch (IOException e) {
                  throw new ClassNotFoundException(name, e);
                }
              }
              return c;
            }
          }
        }
        return super.loadClass(name, resolve);
      }
    };
    Class<?> installer = Class.forName(PACKAGE + "PgwireGovDataInstaller", true, engine);
    java.lang.reflect.Method variant = installer.getDeclaredMethod("osVariant");
    variant.setAccessible(true);
    String v = (String) variant.invoke(null);
    assertTrue(v.endsWith("-x86_64") || v.endsWith("-arm64"), v);
    assertTrue(Class.forName(PACKAGE + "PlatformVariant", false, engine).getClassLoader()
        == launcher, "the shared class should come from the launcher's loader");
  }
}
