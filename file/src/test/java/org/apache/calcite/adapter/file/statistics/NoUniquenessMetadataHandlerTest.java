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
package org.apache.calcite.adapter.file.statistics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Guards the decision that this adapter contributes no key-uniqueness metadata handler.
 *
 * <p>Calcite's defaults answer "not known unique" when nothing claims otherwise, which is the
 * safe answer: a planner that believes in a key it has not verified can drop a de-dup or pick
 * a join strategy that is only valid for unique keys. Primary-key statistics exist for the
 * verify check, and wiring them into {@code RelMdColumnUniqueness} or {@code RelMdUniqueKeys}
 * would turn a measured-or-absent statistic into an assumed-unique answer on a cache miss.
 *
 * <p>If a handler is ever added deliberately, it must return null/false on a miss — and this
 * test should be replaced by one that proves it does.
 */
@Tag("unit")
public class NoUniquenessMetadataHandlerTest {

  private static final String[] FORBIDDEN = {
      "ColumnUniqueness.Handler",
      "UniqueKeys.Handler",
      "areColumnsUnique",
      "getUniqueKeys",
  };

  @Test void adapterDeclaresNoKeyUniquenessHandler() throws IOException {
    Path mainSources = locateMainSources();
    assumeTrue(mainSources != null, "could not locate src/main/java from " + System.getProperty("user.dir"));

    List<String> offenders = new ArrayList<>();
    try (Stream<Path> paths = Files.walk(mainSources)) {
      for (Path p : (Iterable<Path>) paths.filter(f -> f.toString().endsWith(".java"))::iterator) {
        String body = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
        for (String marker : FORBIDDEN) {
          if (body.contains(marker)) {
            offenders.add(p.getFileName() + " contains " + marker);
          }
        }
      }
    }

    assertTrue(offenders.isEmpty(),
        "Key-uniqueness metadata is deliberately left to Calcite's defaults so a cache miss "
            + "reads as not-known-unique. Found: " + offenders);
  }

  /** Walks up from the working directory to the file module's main sources. */
  private static Path locateMainSources() {
    File dir = new File(System.getProperty("user.dir")).getAbsoluteFile();
    for (int i = 0; i < 5 && dir != null; i++, dir = dir.getParentFile()) {
      File candidate = new File(dir, "src/main/java/org/apache/calcite/adapter/file");
      if (candidate.isDirectory()) {
        return candidate.toPath();
      }
      File fromRoot = new File(dir, "file/src/main/java/org/apache/calcite/adapter/file");
      if (fromRoot.isDirectory()) {
        return fromRoot.toPath();
      }
    }
    return null;
  }
}
