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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The switch under which a govdata catalog is built by discovery belongs to the release path
 * that makes the official seed. No launcher, connector, model or workflow may set it: a server
 * never builds its catalog.
 */
@Tag("unit")
class GovDataSeedBuildSwitchTest {

  private static final Set<String> SKIPPED_DIRECTORIES =
      new HashSet<String>(
          Arrays.asList(".git", ".gradle", ".claude", ".idea", "build", "node_modules", ".venv",
              "vendor", "site"));

  private static final Set<String> SCANNED_SUFFIXES =
      new HashSet<String>(
          Arrays.asList(".java", ".py", ".sh", ".bat", ".ps1", ".yml", ".yaml", ".json", ".kts",
              ".gradle", ".toml", ".properties"));

  /** The only files that may name the property, relative to the repository root. */
  private static final Set<String> ALLOWED =
      new TreeSet<String>(
          Arrays.asList(
              "govdata/scripts/build-seed.sh",
              "govdata/src/main/java/org/apache/calcite/adapter/govdata/GovDataDriver.java"));

  private static Path repositoryRoot() {
    Path dir = Paths.get("").toAbsolutePath();
    while (dir != null && !Files.isRegularFile(dir.resolve("settings.gradle.kts"))) {
      dir = dir.getParent();
    }
    assertTrue(dir != null, "the repository root (settings.gradle.kts) is above the test's "
        + "working directory");
    return dir;
  }

  @Test void onlyTheSeedBuildScriptAndTheDriverNameTheSwitch() throws IOException {
    final Path root = repositoryRoot();
    final String property = GovDataDriver.SEED_BUILD_PROPERTY;
    final Set<String> found = new TreeSet<String>();
    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
      @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
        Path name = dir.getFileName();
        return name != null && SKIPPED_DIRECTORIES.contains(name.toString())
            ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
      }

      @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
          throws IOException {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        if (dot < 0 || !SCANNED_SUFFIXES.contains(name.substring(dot))
            || file.toString().contains("/src/test/") || attrs.size() > 4_000_000L) {
          return FileVisitResult.CONTINUE;
        }
        String text = new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
        if (text.contains(property)) {
          found.add(root.relativize(file).toString().replace('\\', '/'));
        }
        return FileVisitResult.CONTINUE;
      }
    });
    assertEquals(ALLOWED, found,
        "'" + property + "' is the release path's seed-build switch; only these files may "
            + "name it");
  }
}
