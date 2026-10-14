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
import java.io.UncheckedIOException;
import java.nio.file.Files;

/**
 * The process's scratch for temporary files that no schema owns: a remote read staged to disk by
 * a shared storage provider, the offline ETL's downloads, staging batches and DuckDB spill. Named
 * by the required system property {@value #PROPERTY} (REQ-1992); the JVM temp directory is never
 * used. pgwire-calcite sets it under the server's state directory, the govdata ETL scripts to
 * {@code GOVDATA_TMPDIR}, the disk-backed scratch sized for the job.
 *
 * <p>A file a schema owns goes under that schema's base directory instead ({@link WorkFiles}).
 */
public final class StagingDirectory {

  /** The system property naming the directory. */
  public static final String PROPERTY = "calcite.file.staging.directory";

  private StagingDirectory() {
  }

  /** The staging directory, created if missing; refuses, naming the property, when unset. */
  public static File get() {
    String configured = System.getProperty(PROPERTY);
    if (configured == null || configured.isEmpty()) {
      throw new IllegalStateException("The system property " + PROPERTY + " is not set: "
          + "temporary files no schema owns have no default directory. Set it to a disk-backed "
          + "scratch directory this process owns.");
    }
    try {
      return Files.createDirectories(new File(configured).toPath()).toFile();
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot create the staging directory " + configured, e);
    }
  }

  /** A new empty file in the staging directory. */
  public static File newTempFile(String prefix, String suffix) throws IOException {
    return Files.createTempFile(get().toPath(), prefix, suffix).toFile();
  }

  /** A new empty directory in the staging directory. */
  public static java.nio.file.Path newTempDirectory(String prefix) throws IOException {
    return Files.createTempDirectory(get().toPath(), prefix);
  }
}
