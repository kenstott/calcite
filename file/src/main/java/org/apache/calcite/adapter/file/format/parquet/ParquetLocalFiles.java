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
package org.apache.calcite.adapter.file.format.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Where a Parquet writer puts its bytes.
 *
 * <p>A file on the local disk is written through {@code java.nio}. Hadoop's local file system
 * sets the new file's permissions by running {@code winutils.exe}, so on Windows without a
 * Hadoop installation it cannot create a file at all.
 */
public final class ParquetLocalFiles {
  private ParquetLocalFiles() {
  }

  /**
   * The output file for a path: the local disk for a {@code file:} path, or a path without a
   * scheme while the configuration's default file system is the local one; Hadoop's file
   * system otherwise.
   */
  public static OutputFile outputFile(Path path, Configuration conf) throws IOException {
    URI uri = path.toUri();
    String scheme = uri.getScheme();
    boolean local = "file".equals(scheme)
        || (scheme == null && "file".equals(FileSystem.getDefaultUri(conf).getScheme()));
    if (!local) {
      return HadoopOutputFile.fromPath(path, conf);
    }
    // On Windows the URI's path is /C:/dir/name, which File reads as C:\dir\name
    return new LocalOutputFile(new File(uri.getPath()).toPath());
  }
}
