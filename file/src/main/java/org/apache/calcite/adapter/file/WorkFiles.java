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
package org.apache.calcite.adapter.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Where the adapter's own temporary files go: a {@code .tmp} directory inside a directory the
 * schema owns (its base directory, or one under it such as its operating cache), so they stay
 * under the schema's base directory like every other file the adapter writes (REQ-1992). Never
 * the JVM temp directory, which every schema in the process would share. A dot directory is
 * skipped by every scan for source and cache files.
 */
public final class WorkFiles {

  private WorkFiles() {
  }

  /** The temporary-file directory inside {@code ownedDirectory}, created if missing. */
  public static Path tempDirectory(File ownedDirectory) throws IOException {
    if (ownedDirectory == null) {
      throw new IllegalArgumentException("No schema directory for the adapter's temporary files: "
          + "every file the adapter writes goes under the schema's baseDirectory");
    }
    return Files.createDirectories(ownedDirectory.toPath().resolve(".tmp"));
  }

  /** A new empty file in the temporary-file directory inside {@code ownedDirectory}. */
  public static File newTempFile(File ownedDirectory, String prefix, String suffix)
      throws IOException {
    return Files.createTempFile(tempDirectory(ownedDirectory), prefix, suffix).toFile();
  }

  /** A new empty directory in the temporary-file directory inside {@code ownedDirectory}. */
  public static Path newTempDirectory(File ownedDirectory, String prefix) throws IOException {
    return Files.createTempDirectory(tempDirectory(ownedDirectory), prefix);
  }
}
